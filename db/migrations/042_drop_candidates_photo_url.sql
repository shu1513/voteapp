BEGIN;

ALTER TABLE public.candidates
DROP COLUMN IF EXISTS photo_url;

COMMIT;
