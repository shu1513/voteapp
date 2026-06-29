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

DO $$
DECLARE
  source_slugs text[] := ARRAY[
    'cost_of_living_reduction',
    'housing_affordability',
    'personal_income_tax_reduction',
    'social_programs_and_welfare'
  ]::text[];
  target_area_count integer;
  source_area_count integer;
  target_office_count integer;
  missing_mapping_count integer;
BEGIN
  SELECT COUNT(*)
  INTO target_area_count
  FROM public.research_areas
  WHERE slug = 'reduce_wealth_gap';

  IF target_area_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 research area for slug=reduce_wealth_gap, found %',
      target_area_count;
  END IF;

  SELECT COUNT(*)
  INTO source_area_count
  FROM public.research_areas
  WHERE slug = ANY (source_slugs);

  IF source_area_count <> COALESCE(array_length(source_slugs, 1), 0) THEN
    RETURN;
  END IF;

  WITH source_areas AS (
    SELECT id
    FROM public.research_areas
    WHERE slug = ANY (source_slugs)
  )
  SELECT COUNT(DISTINCT link.office_id)
  INTO target_office_count
  FROM public.office_research_areas link
  JOIN source_areas source_area
    ON source_area.id = link.research_area_id
  JOIN public.offices office
    ON office.id = link.office_id
  WHERE office.canonical_name NOT ILIKE '%judge%'
    AND office.canonical_name NOT ILIKE '%justice%';

  IF target_office_count = 0 THEN
    RETURN;
  END IF;

  WITH target_area AS (
    SELECT id
    FROM public.research_areas
    WHERE slug = 'reduce_wealth_gap'
  ),
  source_areas AS (
    SELECT id
    FROM public.research_areas
    WHERE slug = ANY (source_slugs)
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

  WITH target_area AS (
    SELECT id
    FROM public.research_areas
    WHERE slug = 'reduce_wealth_gap'
  ),
  source_areas AS (
    SELECT id
    FROM public.research_areas
    WHERE slug = ANY (source_slugs)
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
  SELECT COUNT(*)
  INTO missing_mapping_count
  FROM target_offices target_office
  CROSS JOIN target_area
  LEFT JOIN public.office_research_areas link
    ON link.office_id = target_office.office_id
   AND link.research_area_id = target_area.id
  WHERE link.office_id IS NULL;

  IF missing_mapping_count <> 0 THEN
    RAISE EXCEPTION
      'Expected reduce_wealth_gap mappings for all derived offices, missing %',
      missing_mapping_count;
  END IF;
END
$$;

COMMIT;
