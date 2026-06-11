BEGIN;

ALTER TABLE public.districts
  RENAME COLUMN vote_power_score TO representation_power_score;

ALTER TABLE public.districts
  DROP CONSTRAINT IF EXISTS districts_vote_power_score_check;

ALTER TABLE public.districts
  ADD CONSTRAINT districts_representation_power_score_check
  CHECK (
    representation_power_score IS NULL
    OR (representation_power_score >= 0 AND representation_power_score <= 100)
  );

COMMIT;
