-- Retune the local composite rating from a 100-level ladder at 25n^2 to a
-- 500-level ladder at 6n^2.
--
-- Same shape as 20261013010000_rating_level_scale: the score -> level mapping is
-- a server-side policy curve, but account_rating stores the derived level, so
-- every existing row still carries a level produced by the old curve. Until the
-- background worker happened to reach a row (bounded by
-- TELESRV_RATING_STALE_AFTER, so hours), the profile projection and the admin
-- leaderboard would keep showing the old levels -- and the leaderboard ordering
-- is wrong in between, because it is keyed on level.
--
-- account_rating is a derived read model: level, current_level_stars and
-- next_level_stars are pure functions of stars, so they can be re-derived here
-- without touching the score, its components or the adjustment ledger. This
-- mirrors domain.AccountRatingLevelForStars exactly:
--
--   level   = min(500, floor(sqrt(max(stars, 0) / 6)))
--   current = 6 * level^2
--   next    = 6 * (level + 1)^2 while level < 500, NULL above it
--
-- The thresholds are exact small integers (6 * 500^2 = 1500000), so the double
-- representation is exact and sqrt() on it is correctly rounded: a score sitting
-- precisely on a threshold keeps the level it just reached instead of dropping
-- back one rung. The identical argument held for the 25n^2 ladder it replaces.
--
-- Why the unit dropped from 25 to 6: keeping 25 would put the top rung at 6.25M,
-- which widens the ladder without moving the population -- the mean level goes
-- 11.9 -> 12.7 and the same accounts stay pinned to the ceiling. At 6 the top
-- rung is 1.5M, just above the highest score observed (1399398), so the mean
-- lands at 26.1 and nobody is capped. The unit sets how much of the score range
-- the ladder spans; the level count alone does not.
WITH remapped AS (
    SELECT
        user_id,
        LEAST(500, GREATEST(0, FLOOR(SQRT(GREATEST(0, stars)::double precision / 6))))::integer AS level
    FROM public.account_rating
)
UPDATE public.account_rating AS r
SET level = m.level,
    current_level_stars = 6 * m.level * m.level,
    next_level_stars = CASE WHEN m.level >= 500 THEN NULL ELSE 6 * (m.level + 1) * (m.level + 1) END
FROM remapped AS m
WHERE r.user_id = m.user_id;
