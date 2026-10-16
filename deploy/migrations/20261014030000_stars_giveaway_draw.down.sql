-- Reverts the draw state only. The entries table is dropped rather than
-- preserved: a draw roster without the draw outcome that paid it is the exact
-- state that lets a giveaway be re-drawn, so it is not a state worth keeping.

DROP TABLE IF EXISTS stars_giveaway_entries;

ALTER TABLE stars_giveaways
    DROP CONSTRAINT IF EXISTS stars_giveaways_draw_check;

ALTER TABLE stars_giveaways
    DROP COLUMN IF EXISTS winners_json,
    DROP COLUMN IF EXISTS drawn_at,
    DROP COLUMN IF EXISTS activated_count,
    DROP COLUMN IF EXISTS winner_count;