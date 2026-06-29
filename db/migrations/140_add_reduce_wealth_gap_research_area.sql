BEGIN;

INSERT INTO public.research_areas (slug, name, description)
VALUES (
  'reduce_wealth_gap',
  'Reduce Wealth Gap',
  'Narrow wealth disparities through policy that expands asset building, economic mobility, and equitable opportunity.'
)
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  updated_at = now();

WITH target_area AS (
  SELECT id
  FROM public.research_areas
  WHERE slug = 'reduce_wealth_gap'
),
source_areas AS (
  SELECT id
  FROM public.research_areas
  WHERE slug IN (
    'cost_of_living_reduction',
    'housing_affordability',
    'personal_income_tax_reduction',
    'social_programs_and_welfare'
  )
),
target_offices AS (
  SELECT DISTINCT link.office_id
  FROM public.office_research_areas link
  JOIN source_areas source_area
    ON source_area.id = link.research_area_id
  JOIN public.offices office
    ON office.id = link.office_id
  WHERE office.canonical_name NOT ILIKE '%judge%'
    AND office.canonical_name NOT ILIKE '%justice%'
)
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT target_office.office_id, target_area.id
FROM target_offices target_office
CROSS JOIN target_area
ON CONFLICT (office_id, research_area_id) DO NOTHING;

COMMIT;
