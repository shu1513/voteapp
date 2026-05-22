BEGIN;

ALTER TABLE public.candidates
ADD COLUMN IF NOT EXISTS official_website_url text;

COMMIT;
