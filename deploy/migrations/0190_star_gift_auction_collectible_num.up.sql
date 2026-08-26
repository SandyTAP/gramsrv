-- Auction winners must keep the release number they actually won.
--
-- The auction engine stamps the won number on the awarded gift
-- (star_gift_auction_acquired.gift_num, mirrored into peer_star_gifts.gift_num),
-- and the award service message renders it. Upgrading that gift into a
-- collectible, however, minted `revision.issued + 1` — i.e. the order in which
-- people happened to press "upgrade" — so the collectible showed a number that
-- had nothing to do with the win: the gift won as #1 with a 1 000 000 Stars bid
-- rendered as #16 afterwards, and the winner of #109 rendered as #1.
--
-- unique_star_gifts.slug embeds the number ("<prefix>-<num>"), so it is
-- renumbered together with it, and every server-owned snapshot that carries a
-- collectible (service-message media, send/sender snapshots, admin log events,
-- update-event payloads, collectible emoji statuses) is rewritten through the
-- same map so history matches the collectible it describes.
--
-- Not rewritten: user-authored message text and the cached t.me/nft/<slug> link
-- previews baked into those messages. Those are user content, not a projection
-- of the collectible, and rewriting somebody's typed link is worse than leaving
-- a stale one.

LOCK TABLE unique_star_gifts IN EXCLUSIVE MODE;

CREATE TEMP TABLE star_gift_num_repair (
    unique_id bigint PRIMARY KEY,
    gift_id   bigint  NOT NULL,
    old_num   integer NOT NULL,
    new_num   integer NOT NULL,
    old_slug  text    NOT NULL,
    new_slug  text    NOT NULL
) ON COMMIT DROP;

-- source_saved_gift_id is the immutable mint link (unique index), so it still
-- resolves the award after the collectible was transferred or resold.
INSERT INTO star_gift_num_repair (unique_id, gift_id, old_num, new_num, old_slug, new_slug)
SELECT u.id, u.gift_id, u.num, a.gift_num, u.slug, r.slug_prefix || '-' || a.gift_num
FROM star_gift_auction_acquired a
JOIN unique_star_gifts u ON u.source_saved_gift_id = a.saved_gift_id
JOIN star_gift_collectible_revisions r ON r.id = u.collectible_revision_id
WHERE a.gift_num > 0
  AND (u.num <> a.gift_num OR u.slug <> r.slug_prefix || '-' || a.gift_num);

-- A collectible minted from a regular purchase may be sitting on a number an
-- auction winner has to get back. Move it to the smallest free number of the
-- same gift instead of failing the repair: it never had a claim to that number,
-- while the winner does.
DO $repair$
DECLARE
    conflict   record;
    prefix     text;
    supply     integer;
    free_num   integer;
BEGIN
    FOR conflict IN
        SELECT u.id, u.gift_id, u.num, u.slug, u.collectible_revision_id
        FROM unique_star_gifts u
        JOIN star_gift_num_repair m ON m.gift_id = u.gift_id AND m.new_num = u.num
        WHERE NOT EXISTS (SELECT 1 FROM star_gift_num_repair x WHERE x.unique_id = u.id)
        ORDER BY u.id
    LOOP
        SELECT r.slug_prefix, r.supply_total INTO prefix, supply
        FROM star_gift_collectible_revisions r
        WHERE r.id = conflict.collectible_revision_id;

        -- Occupancy is judged *after* the repair: a row already in the map vacates
        -- its old number, which is usually exactly the slot this conflict should
        -- take (winner and squatter simply swap). Rows not in the map keep their
        -- number, so they still block. Relocations are appended to the map inside
        -- the loop, so later iterations see them as taken.
        SELECT n INTO free_num
        FROM generate_series(1, GREATEST(supply, 1)) AS n
        WHERE NOT EXISTS (SELECT 1 FROM unique_star_gifts x
                          WHERE x.gift_id = conflict.gift_id AND x.num = n
                            AND NOT EXISTS (SELECT 1 FROM star_gift_num_repair m WHERE m.unique_id = x.id))
          AND NOT EXISTS (SELECT 1 FROM star_gift_num_repair x WHERE x.gift_id = conflict.gift_id AND x.new_num = n)
          AND NOT EXISTS (SELECT 1 FROM peer_star_gifts p
                          WHERE p.gift_id = conflict.gift_id AND p.gift_num = n AND p.unique_gift_id IS NULL)
        ORDER BY n
        LIMIT 1;

        IF free_num IS NULL THEN
            RAISE EXCEPTION 'no free collectible number for gift % (unique %)', conflict.gift_id, conflict.id;
        END IF;

        INSERT INTO star_gift_num_repair (unique_id, gift_id, old_num, new_num, old_slug, new_slug)
        VALUES (conflict.id, conflict.gift_id, conflict.num, free_num, conflict.slug, prefix || '-' || free_num);
    END LOOP;
END
$repair$;

-- Two phases: the target numbers are a permutation of the current ones, and
-- unique_star_gift_number_uniq / unique_star_gift_slug_uniq are checked per row,
-- so the renumbered rows are first parked outside the live numbering space
-- (num > any supply_total, num > 0 keeps unique_star_gift_num_check happy).
UPDATE unique_star_gifts u
SET num = m.old_num + 1000000,
    slug = m.old_slug || '~num-repair-' || u.id,
    updated_at = now()
FROM star_gift_num_repair m
WHERE u.id = m.unique_id;

UPDATE unique_star_gifts u
SET num = m.new_num,
    slug = m.new_slug,
    updated_at = now()
FROM star_gift_num_repair m
WHERE u.id = m.unique_id;

-- Stored snapshots keep their own copy of the collectible, so history would
-- keep showing the wrong number after the row above was fixed. Walk every JSON
-- payload and patch the collectible objects addressed by the map:
--   * domain.UniqueStarGift has no json tags, so its keys are the Go field
--     names ("ID"/"Num"/"Slug") — service-message media, send snapshots, …
--   * collectible emoji statuses reference the gift by "collectible_id" and
--     carry "slug" (users.emoji_status_collectible, update payloads).
-- Anything else is returned untouched.
CREATE FUNCTION star_gift_num_repair_rewrite(payload jsonb) RETURNS jsonb
LANGUAGE plpgsql AS $rewrite$
DECLARE
    result jsonb;
    entry  record;
    mapped record;
BEGIN
    IF payload IS NULL THEN
        RETURN NULL;
    END IF;
    IF jsonb_typeof(payload) = 'array' THEN
        SELECT COALESCE(jsonb_agg(star_gift_num_repair_rewrite(element) ORDER BY ordinality), '[]'::jsonb)
        INTO result
        FROM jsonb_array_elements(payload) WITH ORDINALITY AS t(element, ordinality);
        RETURN result;
    END IF;
    IF jsonb_typeof(payload) <> 'object' THEN
        RETURN payload;
    END IF;

    result := payload;

    IF jsonb_typeof(result -> 'ID') = 'number' AND result ? 'Num' AND result ? 'Slug' THEN
        SELECT * INTO mapped FROM star_gift_num_repair WHERE unique_id = (result ->> 'ID')::bigint;
        IF FOUND THEN
            result := jsonb_set(result, '{Num}', to_jsonb(mapped.new_num));
            result := jsonb_set(result, '{Slug}', to_jsonb(mapped.new_slug));
        END IF;
    END IF;

    IF jsonb_typeof(result -> 'collectible_id') = 'number' AND result ? 'slug' THEN
        SELECT * INTO mapped FROM star_gift_num_repair WHERE unique_id = (result ->> 'collectible_id')::bigint;
        IF FOUND THEN
            result := jsonb_set(result, '{slug}', to_jsonb(mapped.new_slug));
            IF result ? 'num' THEN
                result := jsonb_set(result, '{num}', to_jsonb(mapped.new_num));
            END IF;
        END IF;
    END IF;

    FOR entry IN SELECT key, value FROM jsonb_each(result) LOOP
        IF jsonb_typeof(entry.value) IN ('object', 'array') THEN
            result := jsonb_set(result, ARRAY[entry.key], star_gift_num_repair_rewrite(entry.value));
        END IF;
    END LOOP;
    RETURN result;
END
$rewrite$;

-- Every jsonb column of every base table is swept rather than a hand-listed set,
-- so a snapshot carrier added later cannot be silently missed. The candidate
-- filter keeps the rewrite off rows that hold no collectible at all.
DO $sweep$
DECLARE
    target  record;
    changed bigint;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM star_gift_num_repair) THEN
        RETURN;
    END IF;
    FOR target IN
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE c.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'
          AND c.data_type = 'jsonb'
          AND c.is_generated = 'NEVER'
          AND c.is_updatable = 'YES'
        ORDER BY c.table_name, c.column_name
    LOOP
        EXECUTE format(
            'UPDATE %I SET %I = star_gift_num_repair_rewrite(%I) '
            'WHERE (%I::text LIKE ''%%"Slug"%%'' OR %I::text LIKE ''%%"collectible_id"%%'') '
            '  AND star_gift_num_repair_rewrite(%I) IS DISTINCT FROM %I',
            target.table_name, target.column_name, target.column_name,
            target.column_name, target.column_name, target.column_name, target.column_name);
        GET DIAGNOSTICS changed = ROW_COUNT;
        IF changed > 0 THEN
            RAISE NOTICE 'star gift number repair: rewrote % row(s) in %.%',
                changed, target.table_name, target.column_name;
        END IF;
    END LOOP;
END
$sweep$;

DROP FUNCTION star_gift_num_repair_rewrite(jsonb);

-- Fail loudly instead of half-repairing: after this migration no upgraded
-- auction award may carry a number other than the one it won.
DO $verify$
DECLARE
    wrong_num  bigint;
    wrong_slug bigint;
BEGIN
    SELECT count(*) INTO wrong_num
    FROM star_gift_auction_acquired a
    JOIN unique_star_gifts u ON u.source_saved_gift_id = a.saved_gift_id
    WHERE a.gift_num > 0 AND u.num <> a.gift_num;

    SELECT count(*) INTO wrong_slug
    FROM star_gift_auction_acquired a
    JOIN unique_star_gifts u ON u.source_saved_gift_id = a.saved_gift_id
    JOIN star_gift_collectible_revisions r ON r.id = u.collectible_revision_id
    WHERE a.gift_num > 0 AND u.slug <> r.slug_prefix || '-' || u.num;

    IF wrong_num > 0 OR wrong_slug > 0 THEN
        RAISE EXCEPTION 'star gift number repair incomplete: % wrong number(s), % wrong slug(s)',
            wrong_num, wrong_slug;
    END IF;
END
$verify$;
