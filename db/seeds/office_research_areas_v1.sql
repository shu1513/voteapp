DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'national_defense',
    'peaceful_foreign_policy',
    'foreign_trade',
    'government_spending_and_deficit_reduction',
    'personal_income_tax_relief',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'immigration',
    'civil_rights',
    'womens_reproductive_rights',
    'environment_and_public_health',
    'public_infrastructure',
    'corporate_accountability',
    'data_privacy',
    'anti_corruption',
    'government_efficiency'
  ]::text[];
  expected_area_count integer;
  office_count integer;
  area_count integer;
BEGIN
  expected_area_count := COALESCE(array_length(desired_slugs, 1), 0);

  SELECT COUNT(*)
  INTO office_count
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'United States Senator';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=United States Senator, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for senate mapping, found %',
      expected_area_count,
      area_count;
  END IF;
  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM (
    SELECT id
    FROM public.offices
    WHERE scope = 'statewide'
      AND canonical_name = 'United States Senator'
    LIMIT 1
  ) office
  JOIN (
    SELECT id
    FROM public.research_areas
    WHERE slug = ANY (desired_slugs)
  ) area
    ON true
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_education_quality',
    'public_safety_and_crime_control',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'government_spending_and_deficit_reduction',
    'government_efficiency',
    'public_infrastructure',
    'environment_and_public_health',
    'housing_affordability',
    'personal_income_tax_relief',
    'womens_reproductive_rights',
    'civil_rights',
    'data_privacy',
    'corporate_accountability',
    'anti_corruption'
  ]::text[];
  expected_area_count integer;
  office_count integer;
  area_count integer;
BEGIN
  expected_area_count := COALESCE(array_length(desired_slugs, 1), 0);

  SELECT COUNT(*)
  INTO office_count
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'Governor';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Governor, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for governor mapping, found %',
      expected_area_count,
      area_count;
  END IF;
  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM (
    SELECT id
    FROM public.offices
    WHERE scope = 'statewide'
      AND canonical_name = 'Governor'
    LIMIT 1
  ) office
  JOIN (
    SELECT id
    FROM public.research_areas
    WHERE slug = ANY (desired_slugs)
  ) area
    ON true
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_spending_and_deficit_reduction',
    'personal_income_tax_relief',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'public_infrastructure',
    'national_defense',
    'foreign_trade',
    'immigration',
    'environment_and_public_health',
    'corporate_accountability',
    'data_privacy',
    'anti_corruption',
    'government_efficiency',
    'civil_rights',
    'womens_reproductive_rights',
    'peaceful_foreign_policy'
  ]::text[];
  expected_area_count integer;
  office_count integer;
  area_count integer;
BEGIN
  expected_area_count := COALESCE(array_length(desired_slugs, 1), 0);

  SELECT COUNT(*)
  INTO office_count
  FROM public.offices
  WHERE scope = 'us_house'
    AND canonical_name = 'United States Representative';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=us_house canonical_name=United States Representative, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for us_house mapping, found %',
      expected_area_count,
      area_count;
  END IF;
  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM (
    SELECT id
    FROM public.offices
    WHERE scope = 'us_house'
      AND canonical_name = 'United States Representative'
    LIMIT 1
  ) office
  JOIN (
    SELECT id
    FROM public.research_areas
    WHERE slug = ANY (desired_slugs)
  ) area
    ON true
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;
