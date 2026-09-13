-- Restrict a catalog gift to official-support accounts only. The flag lives on
-- the immutable catalog revision (like require_premium) and is enforced at the
-- RPC purchase boundary: buyers without the support account flag receive a
-- 403 NOT_TESTER error instead of a payment form.

ALTER TABLE public.star_gift_catalog_revisions
    ADD COLUMN support_only boolean DEFAULT false NOT NULL;