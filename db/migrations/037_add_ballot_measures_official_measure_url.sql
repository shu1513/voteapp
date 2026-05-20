BEGIN;

ALTER TABLE public.ballot_measures
  ADD COLUMN IF NOT EXISTS official_measure_url text;

COMMIT;
