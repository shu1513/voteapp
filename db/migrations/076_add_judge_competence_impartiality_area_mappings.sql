BEGIN;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'legal_competence',
    'impartiality'
  ]::text[];
  expected_area_count integer;
  expected_office_count integer := 3;
  office_count integer;
  area_count integer;
BEGIN
  expected_area_count := COALESCE(array_length(desired_slugs, 1), 0);

  WITH target_offices(scope, canonical_name) AS (
    VALUES
      ('statewide', 'State Level Judge'),
      ('county', 'County Level Judge'),
      ('place', 'Place Level Judge')
  )
  SELECT COUNT(*)
  INTO office_count
  FROM public.offices office
  JOIN target_offices target
    ON target.scope = office.scope
   AND target.canonical_name = office.canonical_name;

  IF office_count <> expected_office_count THEN
    RAISE EXCEPTION
      'Expected % consolidated judge office rows, found %',
      expected_office_count,
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % judge evaluation research areas, found %',
      expected_area_count,
      area_count;
  END IF;

  WITH target_offices(scope, canonical_name) AS (
    VALUES
      ('statewide', 'State Level Judge'),
      ('county', 'County Level Judge'),
      ('place', 'Place Level Judge')
  )
  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM target_offices target
  JOIN public.offices office
    ON office.scope = target.scope
   AND office.canonical_name = target.canonical_name
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

COMMIT;
