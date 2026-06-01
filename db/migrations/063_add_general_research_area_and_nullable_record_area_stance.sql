BEGIN;

ALTER TABLE public.candidate_record_area_tags
  ALTER COLUMN stance DROP NOT NULL;

INSERT INTO public.research_areas (slug, name, description)
VALUES (
  'general',
  'General',
  'General candidate record not mapped to a specific office research area.'
)
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  updated_at = now();

COMMIT;
