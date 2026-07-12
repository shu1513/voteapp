BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'place',
  'Municipal Trustee',
  'Serves on a village or town governing board, setting local policy, budgets, and oversight for municipal operations.'
)
ON CONFLICT (scope, canonical_name)
DO UPDATE SET
  summary = EXCLUDED.summary,
  updated_at = now();

WITH aliases(canonical_name, alias_text, normalized_alias) AS (
  VALUES
    ('Mayor', 'Village President', 'village president'),
    ('Mayor', 'Town President', 'town president'),
    ('Alderman', 'Alderman', 'alderman'),
    ('Alderman', 'Alderperson', 'alderperson'),
    ('City Council Member', 'Councilman', 'councilman'),
    ('City Council Member', 'Councilperson', 'councilperson'),
    ('City Council Member', 'City Council', 'city council'),
    ('Municipal Trustee', 'Trustee', 'trustee'),
    ('Municipal Trustee', 'Village Trustee', 'village trustee'),
    ('Municipal Trustee', 'Town Trustee', 'town trustee')
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT office.id, 'place', aliases.alias_text, aliases.normalized_alias
FROM aliases
JOIN public.offices AS office
  ON office.scope = 'place'
 AND office.canonical_name = aliases.canonical_name
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT office.id, research_area.id
FROM public.offices AS office
JOIN public.research_areas AS research_area
  ON research_area.slug = ANY (
    ARRAY[
      'civil_rights',
      'environment_and_public_health',
      'government_efficiency',
      'government_spending_reduction',
      'housing_affordability',
      'public_infrastructure',
      'public_safety_and_crime_control',
      'social_programs_and_welfare'
    ]::text[]
  )
WHERE office.scope = 'place'
  AND office.canonical_name = 'Municipal Trustee'
ON CONFLICT (office_id, research_area_id) DO NOTHING;

COMMIT;
