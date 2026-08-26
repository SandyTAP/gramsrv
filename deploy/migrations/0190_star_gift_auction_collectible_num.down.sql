-- The repaired numbers are the ones the winners actually won, so they are not
-- reverted: restoring "whoever upgraded first gets #1" would reintroduce the
-- defect. Dropping the fix only means new collectibles are numbered by the
-- upgrade counter again (the pre-0190 code path).
SELECT 1;
