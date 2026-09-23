ALTER TABLE public.gif_catalog
    ADD COLUMN file_name text NOT NULL DEFAULT '',
    ADD COLUMN category_override text,
    ADD CONSTRAINT gif_catalog_file_name_valid CHECK (char_length(file_name) <= 255),
    ADD CONSTRAINT gif_catalog_category_override_valid CHECK (
        category_override IS NULL OR category_override IN
        ('reaction', 'humor', 'animals', 'sports', 'celebration', 'other')
    );

UPDATE public.gif_catalog SET file_name = source_filename WHERE source_filename <> '';
