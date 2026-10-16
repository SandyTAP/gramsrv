-- Preserve profile-signature history and message privacy; this migration is forward-only.
DO $$ BEGIN RAISE EXCEPTION '20261016010000 preserves sender visibility snapshots; roll forward instead'; END $$;
