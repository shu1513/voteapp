BEGIN;

INSERT INTO public.research_areas (slug, name, description)
VALUES (
  'legal_and_ethics_record',
  'Legal and Ethics Record',
  'Documented criminal convictions, official ethics findings, sanctions, disciplinary actions, court judgments, enforcement actions, or other verified public accountability records.'
)
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  updated_at = now();

COMMIT;
