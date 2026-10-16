-- Nothing to restore: chat streaks are a derived read model. A row only exists
-- because the two accounts exchanged a private message, and it is fully
-- re-derivable from that history by replaying it day by day. Dropping the table
-- loses the skip budget an operator granted by hand, and that is the one piece
-- that cannot be recomputed -- which is exactly why down is a no-op and the
-- grant is treated as authoritative rather than derived.
SELECT 1;
