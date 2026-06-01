BEGIN;

ALTER TABLE public.candidates
  ADD COLUMN IF NOT EXISTS last_records_searched_at timestamptz;

COMMIT;
