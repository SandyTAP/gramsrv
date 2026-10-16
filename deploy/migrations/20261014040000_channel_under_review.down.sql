-- Clears every channel under review. Reviews are operational state, not data:
-- restoring the columns to their defaults returns each channel to normal
-- visibility, which is the documented behaviour of a down migration for this
-- table. The audit trail in admin_commands is intentionally left alone.
ALTER TABLE channels
    DROP CONSTRAINT IF EXISTS channels_under_review_check;

DROP INDEX IF EXISTS channels_under_review_active_idx;

ALTER TABLE channels
    DROP COLUMN IF EXISTS under_review_started_at,
    DROP COLUMN IF EXISTS under_review_until,
    DROP COLUMN IF EXISTS under_review_reason;