-- The rewritten service-message snapshots are intentionally not reverted: the
-- pre-migration value was the catalog starting price, which was wrong.
ALTER TABLE peer_star_gifts
    DROP COLUMN IF EXISTS paid_stars;
