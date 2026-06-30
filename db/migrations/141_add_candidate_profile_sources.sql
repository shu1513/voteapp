ALTER TABLE public.candidates
  ADD COLUMN IF NOT EXISTS profile_sources jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE public.candidates
  DROP CONSTRAINT IF EXISTS chk_candidates_profile_sources_json;

ALTER TABLE public.candidates
  ADD CONSTRAINT chk_candidates_profile_sources_json
  CHECK (jsonb_typeof(profile_sources) = 'array');
