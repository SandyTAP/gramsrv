-- Auction awards must show the winning bid, not the catalog starting price.
--
-- star_gift_catalog_revisions.stars is the auction's starting/min bid
-- (ensureStarGiftAuction copies it into star_gift_auctions.min_bid_amount), and
-- both the award service message and the saved-gift card used to project that
-- value, so every win rendered as "You won the auction with a bid of <min> Stars".
-- paid_stars records the real settled price for a saved gift; 0 keeps the legacy
-- behaviour of projecting the immutable catalog revision price.
ALTER TABLE peer_star_gifts
    ADD COLUMN IF NOT EXISTS paid_stars bigint NOT NULL DEFAULT 0 CHECK (paid_stars >= 0);

-- Retroactive fix #1: saved gifts already awarded by the auction engine.
UPDATE peer_star_gifts p
SET paid_stars = a.bid_amount
FROM star_gift_auction_acquired a
WHERE a.saved_gift_id = p.id
  AND a.bid_amount > 0
  AND p.paid_stars <> a.bid_amount;

-- Retroactive fix #2: the award service message carries its own immutable gift
-- snapshot, so the stars field inside it has to be rewritten as well. The award
-- is addressed through peer_star_gifts.msg_id (the recipient box id) rather than
-- by matching JSON contents, and the JSON shape is still asserted so no other
-- media kind can ever be touched.
WITH awards AS (
    SELECT a.bid_amount, p.owner_peer_id AS owner_user_id, p.msg_id
    FROM star_gift_auction_acquired a
    JOIN peer_star_gifts p ON p.id = a.saved_gift_id
    WHERE a.bid_amount > 0
      AND p.owner_peer_type = 'user'
      AND p.msg_id > 0
), targets AS (
    SELECT DISTINCT b.private_message_id, w.bid_amount
    FROM message_boxes b
    JOIN awards w ON w.owner_user_id = b.owner_user_id AND w.msg_id = b.box_id
    WHERE b.private_message_id IS NOT NULL
      AND (b.media -> 'service_action' -> 'star_gift' ->> 'auction_acquired') = 'true'
)
UPDATE message_boxes b
SET media = jsonb_set(b.media, '{service_action,star_gift,stars}', to_jsonb(t.bid_amount))
FROM targets t
WHERE b.private_message_id = t.private_message_id
  AND (b.media -> 'service_action' -> 'star_gift' ->> 'auction_acquired') = 'true'
  AND COALESCE((b.media -> 'service_action' -> 'star_gift' ->> 'stars')::bigint, 0) <> t.bid_amount;

WITH awards AS (
    SELECT a.bid_amount, p.owner_peer_id AS owner_user_id, p.msg_id
    FROM star_gift_auction_acquired a
    JOIN peer_star_gifts p ON p.id = a.saved_gift_id
    WHERE a.bid_amount > 0
      AND p.owner_peer_type = 'user'
      AND p.msg_id > 0
), targets AS (
    SELECT DISTINCT b.private_message_id, w.bid_amount
    FROM message_boxes b
    JOIN awards w ON w.owner_user_id = b.owner_user_id AND w.msg_id = b.box_id
    WHERE b.private_message_id IS NOT NULL
)
UPDATE private_messages m
SET media = jsonb_set(m.media, '{service_action,star_gift,stars}', to_jsonb(t.bid_amount))
FROM targets t
WHERE m.id = t.private_message_id
  AND (m.media -> 'service_action' -> 'star_gift' ->> 'auction_acquired') = 'true'
  AND COALESCE((m.media -> 'service_action' -> 'star_gift' ->> 'stars')::bigint, 0) <> t.bid_amount;
