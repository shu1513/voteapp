BEGIN;

ALTER TABLE public.ballot_measures
  ADD COLUMN IF NOT EXISTS research_area_tags_researched_at timestamptz;

COMMIT;
