-- Chat streaks ("огоньки"): one row per unordered pair of human accounts.
--
-- The row is the whole state of the mechanic. There is no event log: a streak
-- is a single number plus the day it was last counted, so an append-only ledger
-- would be pure write amplification on the hottest path in the server (every
-- private message sends an upsert).
--
-- user_a < user_b is a CHECK rather than a convention because it is what makes
-- (user_a, user_b) a usable primary key: the pair is the identity, so the same
-- two accounts must land on the same row no matter who wrote first.
--
-- last_day is a date, not a timestamp. The mechanic compares calendar days, so
-- storing the instant would invite a caller to subtract two midnights and get a
-- number of hours. NULL means "this pair has never been counted", which is
-- different from day zero of a broken streak and keeps NewChatStreak's
-- pre-message grant (a freeze bought before the pair ever talks) loadable.
CREATE TABLE public.chat_streaks (
    user_a     bigint      NOT NULL,
    user_b     bigint      NOT NULL,
    days       integer     NOT NULL DEFAULT 0,
    last_day   date,
    frozen_days integer    NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    version    bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (user_a, user_b),
    CONSTRAINT chat_streaks_pair_ordered CHECK (user_a < user_b),
    CONSTRAINT chat_streaks_users_positive CHECK (user_a > 0 AND user_b > 0),
    CONSTRAINT chat_streaks_days_range CHECK (days >= 0 AND days <= 100000),
    CONSTRAINT chat_streaks_freeze_range CHECK (frozen_days >= 0 AND frozen_days <= 365)
);

-- Every lookup a surface does is "all streaks of this account": the cabinet
-- profile, the admin grant, and the "who is this user talking to" view. The
-- primary key already serves user_a; this serves user_b. Both directions matter
-- because the pair is symmetric and the row does not record who started it.
CREATE INDEX chat_streaks_user_b_idx ON public.chat_streaks (user_b, days DESC);

-- The dashboard's "longest streaks right now" query. Partial: rows with no
-- counted day carry days=0 and can never win a leaderboard.
CREATE INDEX chat_streaks_live_rank_idx ON public.chat_streaks (days DESC, user_a)
    WHERE last_day IS NOT NULL AND days > 0;
