DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'civil_rights',
    'election_integrity',
    'womens_reproductive_rights',
    'public_safety_and_crime_control',
    'corporate_accountability',
    'data_privacy',
    'environment_and_public_health',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'housing_affordability',
    'public_education_quality',
    'anti_corruption',
    'government_efficiency',
    'immigration',
    'legal_competence',
    'impartiality'
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
    AND canonical_name = 'State Level Judge';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=State Level Judge, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for state-level judge mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'State Level Judge'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_safety_and_crime_control',
    'civil_rights',
    'housing_affordability',
    'corporate_accountability',
    'data_privacy',
    'anti_corruption',
    'government_efficiency',
    'election_integrity',
    'environment_and_public_health',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'womens_reproductive_rights',
    'public_education_quality',
    'immigration',
    'legal_competence',
    'impartiality'
  ]::text[];
  expected_area_count integer;
  office_count integer;
  area_count integer;
BEGIN
  expected_area_count := COALESCE(array_length(desired_slugs, 1), 0);

  SELECT COUNT(*)
  INTO office_count
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'County Level Judge';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Level Judge, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county-level judge mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Level Judge'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_safety_and_crime_control',
    'civil_rights',
    'government_efficiency',
    'housing_affordability',
    'data_privacy',
    'corporate_accountability',
    'anti_corruption',
    'legal_competence',
    'impartiality'
  ]::text[];
  expected_area_count integer;
  office_count integer;
  area_count integer;
BEGIN
  expected_area_count := COALESCE(array_length(desired_slugs, 1), 0);

  SELECT COUNT(*)
  INTO office_count
  FROM public.offices
  WHERE scope = 'place'
    AND canonical_name = 'Place Level Judge';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=place canonical_name=Place Level Judge, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for place-level judge mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'place'
    AND office.canonical_name = 'Place Level Judge'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'national_defense',
    'peaceful_foreign_policy',
    'foreign_trade',
    'government_spending_reduction',
    'personal_income_tax_reduction',
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
    'government_spending_reduction',
    'personal_income_tax_reduction',
    'public_education_quality',
    'public_safety_and_crime_control',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'public_infrastructure',
    'housing_affordability',
    'environment_and_public_health',
    'womens_reproductive_rights',
    'election_integrity',
    'civil_rights',
    'data_privacy',
    'corporate_accountability',
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
  WHERE scope = 'state_upper'
    AND canonical_name = 'State Senator';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=state_upper canonical_name=State Senator, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for state_upper mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM (
    SELECT id
    FROM public.offices
    WHERE scope = 'state_upper'
      AND canonical_name = 'State Senator'
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
    'government_spending_reduction',
    'personal_income_tax_reduction',
    'public_education_quality',
    'public_safety_and_crime_control',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'public_infrastructure',
    'housing_affordability',
    'environment_and_public_health',
    'womens_reproductive_rights',
    'election_integrity',
    'civil_rights',
    'data_privacy',
    'corporate_accountability',
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
  WHERE scope = 'state_lower'
    AND canonical_name = 'State Lower Chamber Legislator';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=state_lower canonical_name=State Lower Chamber Legislator, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for state_lower mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM (
    SELECT id
    FROM public.offices
    WHERE scope = 'state_lower'
      AND canonical_name = 'State Lower Chamber Legislator'
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
    'government_spending_reduction',
    'government_efficiency',
    'public_infrastructure',
    'environment_and_public_health',
    'housing_affordability',
    'personal_income_tax_reduction',
    'womens_reproductive_rights',
    'civil_rights',
    'data_privacy',
    'corporate_accountability',
    'anti_corruption',
    'legal_competence',
    'impartiality'
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
    'government_spending_reduction',
    'personal_income_tax_reduction',
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
