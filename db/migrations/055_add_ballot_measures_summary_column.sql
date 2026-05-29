BEGIN;

ALTER TABLE public.ballot_measures
ADD COLUMN IF NOT EXISTS summary text;

COMMIT;
