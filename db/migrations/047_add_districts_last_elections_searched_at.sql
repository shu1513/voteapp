BEGIN;

ALTER TABLE public.districts
ADD COLUMN IF NOT EXISTS last_elections_searched_at timestamptz;

COMMIT;
