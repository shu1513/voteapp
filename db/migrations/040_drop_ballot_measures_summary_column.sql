BEGIN;

ALTER TABLE public.ballot_measures
DROP COLUMN IF EXISTS summary;

COMMIT;
