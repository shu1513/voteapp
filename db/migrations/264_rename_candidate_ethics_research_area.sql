BEGIN;

UPDATE public.research_areas
SET
  name = 'Candidate Ethics',
  description = 'The candidate has documented convictions, ethics findings, sanctions, or other verified accountability records.',
  updated_at = now()
WHERE slug = 'integrity_and_ethics';

COMMIT;
