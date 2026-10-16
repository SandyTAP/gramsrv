-- Participants and draw outcome for Stars channel giveaways.
--
-- The purchase, launch post and boost halves already exist. This adds the two
-- things a giveaway cannot run without: the roster of who entered, and a
-- single durable record of who won.
--
-- Entries are rows rather than a counter because the draw has to choose distinct
-- accounts and because "who entered" has to remain answerable after a winner
-- disputes the result.

CREATE TABLE IF NOT EXISTS stars_giveaway_entries (
    giveaway_id  bigint  NOT NULL,
    user_id      bigint  NOT NULL,
    entered_at   integer NOT NULL,
    reward_state text    NOT NULL DEFAULT 'none',
    reward_at    integer NOT NULL DEFAULT 0,
    reward_error text    NOT NULL DEFAULT '',
    PRIMARY KEY (giveaway_id, user_id),
    CONSTRAINT stars_giveaway_entries_giveaway_fk
        FOREIGN KEY (giveaway_id) REFERENCES stars_giveaways (id) ON DELETE CASCADE,
    CONSTRAINT stars_giveaway_entries_user_fk
        FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT stars_giveaway_entries_state_check
        CHECK (reward_state = ANY (ARRAY['none'::text, 'won'::text, 'paid'::text, 'failed'::text])),
    CONSTRAINT stars_giveaway_entries_shape_check
        CHECK (entered_at > 0)
);

-- The draw reads one giveaway's roster in entry order.
CREATE INDEX IF NOT EXISTS stars_giveaway_entries_draw_idx
    ON stars_giveaway_entries (giveaway_id, entered_at, user_id);

-- "How many people entered" is shown on the giveaway post and in
-- payments.getGiveawayInfo, which is polled by every viewer.
CREATE INDEX IF NOT EXISTS stars_giveaway_entries_user_idx
    ON stars_giveaway_entries (user_id);

-- Draw outcome. Held on the giveaway row rather than in a sibling table so that
-- the winner list and the completed state are written in one statement and can
-- never disagree.
ALTER TABLE stars_giveaways
    ADD COLUMN IF NOT EXISTS winners_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS drawn_at integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS activated_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS winner_count integer NOT NULL DEFAULT 0;

-- A drawn giveaway must be completed, and a completed giveaway that has a
-- winner list must have been drawn. Without this the announcement message and
-- the row can drift, which is the failure mode where a winner is announced and
-- then never paid.
ALTER TABLE stars_giveaways
    DROP CONSTRAINT IF EXISTS stars_giveaways_draw_check;

ALTER TABLE stars_giveaways
    ADD CONSTRAINT stars_giveaways_draw_check CHECK (
        (drawn_at = 0 AND winners_json = '[]'::jsonb)
        OR
        (drawn_at > 0
            AND state = 'completed'
            AND jsonb_typeof(winners_json) = 'array'
            AND activated_count >= winner_count
            AND winner_count > 0
            AND winner_count <= users)
    );