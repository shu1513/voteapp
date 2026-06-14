BEGIN;

ALTER TABLE public.presidential_cycle_candidates
  ADD COLUMN IF NOT EXISTS presidential_profile_researched boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS running_mate_profile_researched boolean NOT NULL DEFAULT false;

ALTER TABLE public.presidential_cycle_candidates
  ALTER COLUMN presidential_profile_researched SET DEFAULT false,
  ALTER COLUMN running_mate_profile_researched SET DEFAULT false;

UPDATE public.presidential_cycle_candidates
SET presidential_profile_researched = false
WHERE presidential_profile_researched IS NULL;

UPDATE public.presidential_cycle_candidates
SET running_mate_profile_researched = false
WHERE running_mate_profile_researched IS NULL;

ALTER TABLE public.presidential_cycle_candidates
  ALTER COLUMN presidential_profile_researched SET NOT NULL,
  ALTER COLUMN running_mate_profile_researched SET NOT NULL;

COMMIT;
