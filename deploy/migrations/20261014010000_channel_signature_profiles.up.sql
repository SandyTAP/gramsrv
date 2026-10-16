ALTER TABLE public.channels ADD COLUMN IF NOT EXISTS signature_profiles boolean DEFAULT false NOT NULL;
