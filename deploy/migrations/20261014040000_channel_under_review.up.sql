-- Temporary moderation review for channels.
--
-- While a channel is under review its profile, member list and boost counters
-- stay visible, but ordinary viewers cannot read its posts: the history reads
-- are refused and the channel shows a service message explaining why.
--
-- Columns rather than a side table, because "is this channel under review right
-- now" is read on the hot path of every history read and every channel
-- projection, and a join against a separate table would put a second lookup in
-- front of both. The audit trail lives in admin_commands, like every other
-- operator write.
--
-- under_review_until = 0 means "no deadline": the channel stays blocked until an
-- operator clears it by hand. A non-zero value is an absolute unix timestamp so
-- expiry needs no sweep job -- the same comparison decides on every read.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS under_review_started_at integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS under_review_until     integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS under_review_reason   text    NOT NULL DEFAULT '';

ALTER TABLE channels
    DROP CONSTRAINT IF EXISTS channels_under_review_check;

ALTER TABLE channels
    ADD CONSTRAINT channels_under_review_check CHECK (
        -- Not under review: every field agrees and the reason is empty.
        (under_review_started_at = 0 AND under_review_until = 0 AND under_review_reason = '')
        OR
        -- Under review: started, with a reason the viewer can be shown, and
        -- either no deadline or one strictly after the start.
        (under_review_started_at > 0
            AND under_review_reason <> ''
            AND octet_length(under_review_reason) <= 200
            AND (under_review_until = 0 OR under_review_until > under_review_started_at))
    );

-- The blocking read is "give me the channels a viewer is allowed to read", and
-- the common case is "this channel is fine". A partial index over the blocked
-- subset keeps that check from scanning the whole table.
CREATE INDEX IF NOT EXISTS channels_under_review_active_idx
    ON channels (under_review_started_at, under_review_until)
    WHERE under_review_started_at > 0;