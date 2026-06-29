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
  expected_pair_count integer;
  resolved_pair_count integer;
BEGIN
  WITH desired(scope, canonical_name, slug) AS (
    VALUES
      ('presidential', 'President of the United States', 'national_defense'),
      ('presidential', 'President of the United States', 'peaceful_foreign_policy'),
      ('presidential', 'President of the United States', 'foreign_trade'),
      ('presidential', 'President of the United States', 'immigration'),
      ('presidential', 'President of the United States', 'government_spending_reduction'),
      ('presidential', 'President of the United States', 'personal_income_tax_reduction'),
      ('presidential', 'President of the United States', 'cost_of_living_reduction'),
      ('presidential', 'President of the United States', 'healthcare_affordability'),
      ('presidential', 'President of the United States', 'social_programs_and_welfare'),
      ('presidential', 'President of the United States', 'environment_and_public_health'),
      ('presidential', 'President of the United States', 'public_infrastructure'),
      ('presidential', 'President of the United States', 'public_safety_and_crime_control'),
      ('presidential', 'President of the United States', 'civil_rights'),
      ('presidential', 'President of the United States', 'womens_reproductive_rights'),
      ('presidential', 'President of the United States', 'corporate_accountability'),
      ('presidential', 'President of the United States', 'data_privacy'),
      ('presidential', 'President of the United States', 'anti_corruption'),
      ('presidential', 'President of the United States', 'government_efficiency'),
      ('presidential', 'President of the United States', 'housing_affordability'),
      ('presidential', 'President of the United States', 'public_education_quality'),
      ('presidential', 'President of the United States', 'election_integrity'),
      ('presidential', 'Vice President of the United States', 'government_spending_reduction'),
      ('presidential', 'Vice President of the United States', 'government_efficiency'),
      ('presidential', 'Vice President of the United States', 'anti_corruption'),
      ('presidential', 'Vice President of the United States', 'election_integrity'),
      ('presidential', 'Vice President of the United States', 'civil_rights'),
      ('presidential', 'Vice President of the United States', 'national_defense'),
      ('presidential', 'Vice President of the United States', 'peaceful_foreign_policy'),
      ('presidential', 'Vice President of the United States', 'foreign_trade'),
      ('presidential', 'Vice President of the United States', 'immigration'),
      ('presidential', 'Vice President of the United States', 'public_safety_and_crime_control'),
      ('presidential', 'Vice President of the United States', 'healthcare_affordability'),
      ('presidential', 'Vice President of the United States', 'social_programs_and_welfare'),
      ('presidential', 'Vice President of the United States', 'environment_and_public_health'),
      ('presidential', 'Vice President of the United States', 'public_infrastructure'),
      ('presidential', 'Vice President of the United States', 'personal_income_tax_reduction'),
      ('presidential', 'Vice President of the United States', 'cost_of_living_reduction'),
      ('presidential', 'Vice President of the United States', 'corporate_accountability'),
      ('presidential', 'Vice President of the United States', 'data_privacy'),
      ('presidential', 'Vice President of the United States', 'womens_reproductive_rights'),
      ('presidential', 'Vice President of the United States', 'housing_affordability'),
      ('presidential', 'Vice President of the United States', 'public_education_quality')
  )
  SELECT COUNT(*)
  INTO expected_pair_count
  FROM desired;

  WITH desired(scope, canonical_name, slug) AS (
    VALUES
      ('presidential', 'President of the United States', 'national_defense'),
      ('presidential', 'President of the United States', 'peaceful_foreign_policy'),
      ('presidential', 'President of the United States', 'foreign_trade'),
      ('presidential', 'President of the United States', 'immigration'),
      ('presidential', 'President of the United States', 'government_spending_reduction'),
      ('presidential', 'President of the United States', 'personal_income_tax_reduction'),
      ('presidential', 'President of the United States', 'cost_of_living_reduction'),
      ('presidential', 'President of the United States', 'healthcare_affordability'),
      ('presidential', 'President of the United States', 'social_programs_and_welfare'),
      ('presidential', 'President of the United States', 'environment_and_public_health'),
      ('presidential', 'President of the United States', 'public_infrastructure'),
      ('presidential', 'President of the United States', 'public_safety_and_crime_control'),
      ('presidential', 'President of the United States', 'civil_rights'),
      ('presidential', 'President of the United States', 'womens_reproductive_rights'),
      ('presidential', 'President of the United States', 'corporate_accountability'),
      ('presidential', 'President of the United States', 'data_privacy'),
      ('presidential', 'President of the United States', 'anti_corruption'),
      ('presidential', 'President of the United States', 'government_efficiency'),
      ('presidential', 'President of the United States', 'housing_affordability'),
      ('presidential', 'President of the United States', 'public_education_quality'),
      ('presidential', 'President of the United States', 'election_integrity'),
      ('presidential', 'Vice President of the United States', 'government_spending_reduction'),
      ('presidential', 'Vice President of the United States', 'government_efficiency'),
      ('presidential', 'Vice President of the United States', 'anti_corruption'),
      ('presidential', 'Vice President of the United States', 'election_integrity'),
      ('presidential', 'Vice President of the United States', 'civil_rights'),
      ('presidential', 'Vice President of the United States', 'national_defense'),
      ('presidential', 'Vice President of the United States', 'peaceful_foreign_policy'),
      ('presidential', 'Vice President of the United States', 'foreign_trade'),
      ('presidential', 'Vice President of the United States', 'immigration'),
      ('presidential', 'Vice President of the United States', 'public_safety_and_crime_control'),
      ('presidential', 'Vice President of the United States', 'healthcare_affordability'),
      ('presidential', 'Vice President of the United States', 'social_programs_and_welfare'),
      ('presidential', 'Vice President of the United States', 'environment_and_public_health'),
      ('presidential', 'Vice President of the United States', 'public_infrastructure'),
      ('presidential', 'Vice President of the United States', 'personal_income_tax_reduction'),
      ('presidential', 'Vice President of the United States', 'cost_of_living_reduction'),
      ('presidential', 'Vice President of the United States', 'corporate_accountability'),
      ('presidential', 'Vice President of the United States', 'data_privacy'),
      ('presidential', 'Vice President of the United States', 'womens_reproductive_rights'),
      ('presidential', 'Vice President of the United States', 'housing_affordability'),
      ('presidential', 'Vice President of the United States', 'public_education_quality')
  )
  SELECT COUNT(*)
  INTO resolved_pair_count
  FROM desired
  JOIN public.offices AS office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name
  JOIN public.research_areas AS area
    ON area.slug = desired.slug;

  IF resolved_pair_count <> expected_pair_count THEN
    RAISE EXCEPTION
      'Expected % presidential office research-area pairs to resolve, found %',
      expected_pair_count,
      resolved_pair_count;
  END IF;

  WITH desired(scope, canonical_name, slug) AS (
    VALUES
      ('presidential', 'President of the United States', 'national_defense'),
      ('presidential', 'President of the United States', 'peaceful_foreign_policy'),
      ('presidential', 'President of the United States', 'foreign_trade'),
      ('presidential', 'President of the United States', 'immigration'),
      ('presidential', 'President of the United States', 'government_spending_reduction'),
      ('presidential', 'President of the United States', 'personal_income_tax_reduction'),
      ('presidential', 'President of the United States', 'cost_of_living_reduction'),
      ('presidential', 'President of the United States', 'healthcare_affordability'),
      ('presidential', 'President of the United States', 'social_programs_and_welfare'),
      ('presidential', 'President of the United States', 'environment_and_public_health'),
      ('presidential', 'President of the United States', 'public_infrastructure'),
      ('presidential', 'President of the United States', 'public_safety_and_crime_control'),
      ('presidential', 'President of the United States', 'civil_rights'),
      ('presidential', 'President of the United States', 'womens_reproductive_rights'),
      ('presidential', 'President of the United States', 'corporate_accountability'),
      ('presidential', 'President of the United States', 'data_privacy'),
      ('presidential', 'President of the United States', 'anti_corruption'),
      ('presidential', 'President of the United States', 'government_efficiency'),
      ('presidential', 'President of the United States', 'housing_affordability'),
      ('presidential', 'President of the United States', 'public_education_quality'),
      ('presidential', 'President of the United States', 'election_integrity'),
      ('presidential', 'Vice President of the United States', 'government_spending_reduction'),
      ('presidential', 'Vice President of the United States', 'government_efficiency'),
      ('presidential', 'Vice President of the United States', 'anti_corruption'),
      ('presidential', 'Vice President of the United States', 'election_integrity'),
      ('presidential', 'Vice President of the United States', 'civil_rights'),
      ('presidential', 'Vice President of the United States', 'national_defense'),
      ('presidential', 'Vice President of the United States', 'peaceful_foreign_policy'),
      ('presidential', 'Vice President of the United States', 'foreign_trade'),
      ('presidential', 'Vice President of the United States', 'immigration'),
      ('presidential', 'Vice President of the United States', 'public_safety_and_crime_control'),
      ('presidential', 'Vice President of the United States', 'healthcare_affordability'),
      ('presidential', 'Vice President of the United States', 'social_programs_and_welfare'),
      ('presidential', 'Vice President of the United States', 'environment_and_public_health'),
      ('presidential', 'Vice President of the United States', 'public_infrastructure'),
      ('presidential', 'Vice President of the United States', 'personal_income_tax_reduction'),
      ('presidential', 'Vice President of the United States', 'cost_of_living_reduction'),
      ('presidential', 'Vice President of the United States', 'corporate_accountability'),
      ('presidential', 'Vice President of the United States', 'data_privacy'),
      ('presidential', 'Vice President of the United States', 'womens_reproductive_rights'),
      ('presidential', 'Vice President of the United States', 'housing_affordability'),
      ('presidential', 'Vice President of the United States', 'public_education_quality')
  )
  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM desired
  JOIN public.offices AS office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name
  JOIN public.research_areas AS area
    ON area.slug = desired.slug
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  expected_office_count integer := 5;
  expected_pair_count integer;
  office_count integer;
  pair_count integer;
BEGIN
  WITH desired(scope, canonical_name, slug) AS (
    VALUES
      ('statewide', 'Labor Commissioner', 'civil_rights'),
      ('statewide', 'Labor Commissioner', 'corporate_accountability'),
      ('statewide', 'Labor Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Labor Commissioner', 'healthcare_affordability'),
      ('statewide', 'Labor Commissioner', 'social_programs_and_welfare'),
      ('statewide', 'Labor Commissioner', 'government_efficiency'),
      ('statewide', 'Labor Commissioner', 'anti_corruption'),
      ('statewide', 'Labor Commissioner', 'data_privacy'),
      ('statewide', 'Land Commissioner', 'environment_and_public_health'),
      ('statewide', 'Land Commissioner', 'public_infrastructure'),
      ('statewide', 'Land Commissioner', 'housing_affordability'),
      ('statewide', 'Land Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Land Commissioner', 'government_efficiency'),
      ('statewide', 'Land Commissioner', 'government_spending_reduction'),
      ('statewide', 'Land Commissioner', 'anti_corruption'),
      ('statewide', 'Land Commissioner', 'corporate_accountability'),
      ('statewide', 'Land Commissioner', 'data_privacy'),
      ('statewide', 'Railroad Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Railroad Commissioner', 'public_infrastructure'),
      ('statewide', 'Railroad Commissioner', 'environment_and_public_health'),
      ('statewide', 'Railroad Commissioner', 'corporate_accountability'),
      ('statewide', 'Railroad Commissioner', 'government_efficiency'),
      ('statewide', 'Railroad Commissioner', 'anti_corruption'),
      ('statewide', 'Railroad Commissioner', 'data_privacy'),
      ('statewide', 'Railroad Commissioner', 'public_safety_and_crime_control'),
      ('county', 'County Auditor', 'government_spending_reduction'),
      ('county', 'County Auditor', 'government_efficiency'),
      ('county', 'County Auditor', 'anti_corruption'),
      ('county', 'County Auditor', 'data_privacy'),
      ('county', 'County Auditor', 'corporate_accountability'),
      ('county', 'County Auditor', 'civil_rights'),
      ('county', 'County Auditor', 'election_integrity'),
      ('county', 'Clerk of Court', 'civil_rights'),
      ('county', 'Clerk of Court', 'public_safety_and_crime_control'),
      ('county', 'Clerk of Court', 'government_efficiency'),
      ('county', 'Clerk of Court', 'data_privacy'),
      ('county', 'Clerk of Court', 'anti_corruption'),
      ('county', 'Clerk of Court', 'social_programs_and_welfare')
  )
  SELECT COUNT(DISTINCT scope || ':' || canonical_name), COUNT(*)
  INTO office_count, expected_pair_count
  FROM desired;

  IF office_count <> expected_office_count THEN
    RAISE EXCEPTION
      'Expected % new elected office mappings, found %',
      expected_office_count,
      office_count;
  END IF;

  WITH desired(scope, canonical_name, slug) AS (
    VALUES
      ('statewide', 'Labor Commissioner', 'civil_rights'),
      ('statewide', 'Labor Commissioner', 'corporate_accountability'),
      ('statewide', 'Labor Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Labor Commissioner', 'healthcare_affordability'),
      ('statewide', 'Labor Commissioner', 'social_programs_and_welfare'),
      ('statewide', 'Labor Commissioner', 'government_efficiency'),
      ('statewide', 'Labor Commissioner', 'anti_corruption'),
      ('statewide', 'Labor Commissioner', 'data_privacy'),
      ('statewide', 'Land Commissioner', 'environment_and_public_health'),
      ('statewide', 'Land Commissioner', 'public_infrastructure'),
      ('statewide', 'Land Commissioner', 'housing_affordability'),
      ('statewide', 'Land Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Land Commissioner', 'government_efficiency'),
      ('statewide', 'Land Commissioner', 'government_spending_reduction'),
      ('statewide', 'Land Commissioner', 'anti_corruption'),
      ('statewide', 'Land Commissioner', 'corporate_accountability'),
      ('statewide', 'Land Commissioner', 'data_privacy'),
      ('statewide', 'Railroad Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Railroad Commissioner', 'public_infrastructure'),
      ('statewide', 'Railroad Commissioner', 'environment_and_public_health'),
      ('statewide', 'Railroad Commissioner', 'corporate_accountability'),
      ('statewide', 'Railroad Commissioner', 'government_efficiency'),
      ('statewide', 'Railroad Commissioner', 'anti_corruption'),
      ('statewide', 'Railroad Commissioner', 'data_privacy'),
      ('statewide', 'Railroad Commissioner', 'public_safety_and_crime_control'),
      ('county', 'County Auditor', 'government_spending_reduction'),
      ('county', 'County Auditor', 'government_efficiency'),
      ('county', 'County Auditor', 'anti_corruption'),
      ('county', 'County Auditor', 'data_privacy'),
      ('county', 'County Auditor', 'corporate_accountability'),
      ('county', 'County Auditor', 'civil_rights'),
      ('county', 'County Auditor', 'election_integrity'),
      ('county', 'Clerk of Court', 'civil_rights'),
      ('county', 'Clerk of Court', 'public_safety_and_crime_control'),
      ('county', 'Clerk of Court', 'government_efficiency'),
      ('county', 'Clerk of Court', 'data_privacy'),
      ('county', 'Clerk of Court', 'anti_corruption'),
      ('county', 'Clerk of Court', 'social_programs_and_welfare')
  ),
  resolved AS (
    SELECT office.id AS office_id, area.id AS research_area_id
    FROM desired
    JOIN public.offices office
      ON office.scope = desired.scope
     AND office.canonical_name = desired.canonical_name
    JOIN public.research_areas area
      ON area.slug = desired.slug
  )
  SELECT COUNT(*)
  INTO pair_count
  FROM resolved;

  IF pair_count <> expected_pair_count THEN
    RAISE EXCEPTION
      'Expected % new elected office research area pairs, found %',
      expected_pair_count,
      pair_count;
  END IF;

  WITH desired(scope, canonical_name, slug) AS (
    VALUES
      ('statewide', 'Labor Commissioner', 'civil_rights'),
      ('statewide', 'Labor Commissioner', 'corporate_accountability'),
      ('statewide', 'Labor Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Labor Commissioner', 'healthcare_affordability'),
      ('statewide', 'Labor Commissioner', 'social_programs_and_welfare'),
      ('statewide', 'Labor Commissioner', 'government_efficiency'),
      ('statewide', 'Labor Commissioner', 'anti_corruption'),
      ('statewide', 'Labor Commissioner', 'data_privacy'),
      ('statewide', 'Land Commissioner', 'environment_and_public_health'),
      ('statewide', 'Land Commissioner', 'public_infrastructure'),
      ('statewide', 'Land Commissioner', 'housing_affordability'),
      ('statewide', 'Land Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Land Commissioner', 'government_efficiency'),
      ('statewide', 'Land Commissioner', 'government_spending_reduction'),
      ('statewide', 'Land Commissioner', 'anti_corruption'),
      ('statewide', 'Land Commissioner', 'corporate_accountability'),
      ('statewide', 'Land Commissioner', 'data_privacy'),
      ('statewide', 'Railroad Commissioner', 'cost_of_living_reduction'),
      ('statewide', 'Railroad Commissioner', 'public_infrastructure'),
      ('statewide', 'Railroad Commissioner', 'environment_and_public_health'),
      ('statewide', 'Railroad Commissioner', 'corporate_accountability'),
      ('statewide', 'Railroad Commissioner', 'government_efficiency'),
      ('statewide', 'Railroad Commissioner', 'anti_corruption'),
      ('statewide', 'Railroad Commissioner', 'data_privacy'),
      ('statewide', 'Railroad Commissioner', 'public_safety_and_crime_control'),
      ('county', 'County Auditor', 'government_spending_reduction'),
      ('county', 'County Auditor', 'government_efficiency'),
      ('county', 'County Auditor', 'anti_corruption'),
      ('county', 'County Auditor', 'data_privacy'),
      ('county', 'County Auditor', 'corporate_accountability'),
      ('county', 'County Auditor', 'civil_rights'),
      ('county', 'County Auditor', 'election_integrity'),
      ('county', 'Clerk of Court', 'civil_rights'),
      ('county', 'Clerk of Court', 'public_safety_and_crime_control'),
      ('county', 'Clerk of Court', 'government_efficiency'),
      ('county', 'Clerk of Court', 'data_privacy'),
      ('county', 'Clerk of Court', 'anti_corruption'),
      ('county', 'Clerk of Court', 'social_programs_and_welfare')
  )
  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM desired
  JOIN public.offices office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name
  JOIN public.research_areas area
    ON area.slug = desired.slug
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  expected_office_count integer := 9;
  expected_pair_count integer;
  office_count integer;
  pair_count integer;
BEGIN
  WITH desired(canonical_name, slug) AS (
    VALUES
      ('Alderman', 'government_spending_reduction'),
      ('Alderman', 'government_efficiency'),
      ('Alderman', 'public_infrastructure'),
      ('Alderman', 'housing_affordability'),
      ('Alderman', 'environment_and_public_health'),
      ('Alderman', 'public_safety_and_crime_control'),
      ('Alderman', 'social_programs_and_welfare'),
      ('Alderman', 'civil_rights'),
      ('Alderman', 'anti_corruption'),
      ('Alderman', 'corporate_accountability'),
      ('Alderman', 'data_privacy'),
      ('City Clerk', 'election_integrity'),
      ('City Clerk', 'government_efficiency'),
      ('City Clerk', 'data_privacy'),
      ('City Clerk', 'anti_corruption'),
      ('City Clerk', 'civil_rights'),
      ('City Clerk', 'corporate_accountability'),
      ('City Council Member', 'government_spending_reduction'),
      ('City Council Member', 'government_efficiency'),
      ('City Council Member', 'public_infrastructure'),
      ('City Council Member', 'housing_affordability'),
      ('City Council Member', 'environment_and_public_health'),
      ('City Council Member', 'public_safety_and_crime_control'),
      ('City Council Member', 'social_programs_and_welfare'),
      ('City Council Member', 'civil_rights'),
      ('City Council Member', 'anti_corruption'),
      ('City Council Member', 'corporate_accountability'),
      ('City Council Member', 'data_privacy'),
      ('City Treasurer', 'government_spending_reduction'),
      ('City Treasurer', 'government_efficiency'),
      ('City Treasurer', 'anti_corruption'),
      ('City Treasurer', 'data_privacy'),
      ('City Treasurer', 'corporate_accountability'),
      ('City Treasurer', 'public_infrastructure'),
      ('Municipal Assessor', 'housing_affordability'),
      ('Municipal Assessor', 'government_efficiency'),
      ('Municipal Assessor', 'anti_corruption'),
      ('Municipal Assessor', 'data_privacy'),
      ('Municipal Assessor', 'corporate_accountability'),
      ('Municipal Assessor', 'civil_rights'),
      ('Municipal Attorney', 'government_efficiency'),
      ('Municipal Attorney', 'civil_rights'),
      ('Municipal Attorney', 'anti_corruption'),
      ('Municipal Attorney', 'public_safety_and_crime_control'),
      ('Municipal Attorney', 'corporate_accountability'),
      ('Municipal Attorney', 'data_privacy'),
      ('Municipal Attorney', 'housing_affordability'),
      ('Municipal Attorney', 'environment_and_public_health'),
      ('Municipal Attorney', 'public_infrastructure'),
      ('Municipal Constable', 'public_safety_and_crime_control'),
      ('Municipal Constable', 'civil_rights'),
      ('Municipal Constable', 'housing_affordability'),
      ('Municipal Constable', 'government_efficiency'),
      ('Municipal Constable', 'data_privacy'),
      ('Municipal Constable', 'anti_corruption'),
      ('Town Council Member', 'government_spending_reduction'),
      ('Town Council Member', 'government_efficiency'),
      ('Town Council Member', 'public_infrastructure'),
      ('Town Council Member', 'housing_affordability'),
      ('Town Council Member', 'environment_and_public_health'),
      ('Town Council Member', 'public_safety_and_crime_control'),
      ('Town Council Member', 'social_programs_and_welfare'),
      ('Town Council Member', 'civil_rights'),
      ('Town Council Member', 'anti_corruption'),
      ('Town Council Member', 'corporate_accountability'),
      ('Town Council Member', 'data_privacy'),
      ('Town Moderator', 'government_efficiency'),
      ('Town Moderator', 'civil_rights'),
      ('Town Moderator', 'election_integrity'),
      ('Town Moderator', 'anti_corruption')
  )
  SELECT COUNT(DISTINCT canonical_name), COUNT(*)
  INTO office_count, expected_pair_count
  FROM desired;

  IF office_count <> expected_office_count THEN
    RAISE EXCEPTION
      'Expected % place offices in requested mapping, found %',
      expected_office_count,
      office_count;
  END IF;

  WITH desired(canonical_name, slug) AS (
    VALUES
      ('Alderman', 'government_spending_reduction'),
      ('Alderman', 'government_efficiency'),
      ('Alderman', 'public_infrastructure'),
      ('Alderman', 'housing_affordability'),
      ('Alderman', 'environment_and_public_health'),
      ('Alderman', 'public_safety_and_crime_control'),
      ('Alderman', 'social_programs_and_welfare'),
      ('Alderman', 'civil_rights'),
      ('Alderman', 'anti_corruption'),
      ('Alderman', 'corporate_accountability'),
      ('Alderman', 'data_privacy'),
      ('City Clerk', 'election_integrity'),
      ('City Clerk', 'government_efficiency'),
      ('City Clerk', 'data_privacy'),
      ('City Clerk', 'anti_corruption'),
      ('City Clerk', 'civil_rights'),
      ('City Clerk', 'corporate_accountability'),
      ('City Council Member', 'government_spending_reduction'),
      ('City Council Member', 'government_efficiency'),
      ('City Council Member', 'public_infrastructure'),
      ('City Council Member', 'housing_affordability'),
      ('City Council Member', 'environment_and_public_health'),
      ('City Council Member', 'public_safety_and_crime_control'),
      ('City Council Member', 'social_programs_and_welfare'),
      ('City Council Member', 'civil_rights'),
      ('City Council Member', 'anti_corruption'),
      ('City Council Member', 'corporate_accountability'),
      ('City Council Member', 'data_privacy'),
      ('City Treasurer', 'government_spending_reduction'),
      ('City Treasurer', 'government_efficiency'),
      ('City Treasurer', 'anti_corruption'),
      ('City Treasurer', 'data_privacy'),
      ('City Treasurer', 'corporate_accountability'),
      ('City Treasurer', 'public_infrastructure'),
      ('Municipal Assessor', 'housing_affordability'),
      ('Municipal Assessor', 'government_efficiency'),
      ('Municipal Assessor', 'anti_corruption'),
      ('Municipal Assessor', 'data_privacy'),
      ('Municipal Assessor', 'corporate_accountability'),
      ('Municipal Assessor', 'civil_rights'),
      ('Municipal Attorney', 'government_efficiency'),
      ('Municipal Attorney', 'civil_rights'),
      ('Municipal Attorney', 'anti_corruption'),
      ('Municipal Attorney', 'public_safety_and_crime_control'),
      ('Municipal Attorney', 'corporate_accountability'),
      ('Municipal Attorney', 'data_privacy'),
      ('Municipal Attorney', 'housing_affordability'),
      ('Municipal Attorney', 'environment_and_public_health'),
      ('Municipal Attorney', 'public_infrastructure'),
      ('Municipal Constable', 'public_safety_and_crime_control'),
      ('Municipal Constable', 'civil_rights'),
      ('Municipal Constable', 'housing_affordability'),
      ('Municipal Constable', 'government_efficiency'),
      ('Municipal Constable', 'data_privacy'),
      ('Municipal Constable', 'anti_corruption'),
      ('Town Council Member', 'government_spending_reduction'),
      ('Town Council Member', 'government_efficiency'),
      ('Town Council Member', 'public_infrastructure'),
      ('Town Council Member', 'housing_affordability'),
      ('Town Council Member', 'environment_and_public_health'),
      ('Town Council Member', 'public_safety_and_crime_control'),
      ('Town Council Member', 'social_programs_and_welfare'),
      ('Town Council Member', 'civil_rights'),
      ('Town Council Member', 'anti_corruption'),
      ('Town Council Member', 'corporate_accountability'),
      ('Town Council Member', 'data_privacy'),
      ('Town Moderator', 'government_efficiency'),
      ('Town Moderator', 'civil_rights'),
      ('Town Moderator', 'election_integrity'),
      ('Town Moderator', 'anti_corruption')
  ),
  resolved AS (
    SELECT office.id AS office_id, area.id AS research_area_id
    FROM desired
    JOIN public.offices office
      ON office.scope = 'place'
     AND office.canonical_name = desired.canonical_name
    JOIN public.research_areas area
      ON area.slug = desired.slug
  )
  SELECT COUNT(*)
  INTO pair_count
  FROM resolved;

  IF pair_count <> expected_pair_count THEN
    RAISE EXCEPTION
      'Expected % place office research area pairs, found %',
      expected_pair_count,
      pair_count;
  END IF;

  WITH desired(canonical_name, slug) AS (
    VALUES
      ('Alderman', 'government_spending_reduction'),
      ('Alderman', 'government_efficiency'),
      ('Alderman', 'public_infrastructure'),
      ('Alderman', 'housing_affordability'),
      ('Alderman', 'environment_and_public_health'),
      ('Alderman', 'public_safety_and_crime_control'),
      ('Alderman', 'social_programs_and_welfare'),
      ('Alderman', 'civil_rights'),
      ('Alderman', 'anti_corruption'),
      ('Alderman', 'corporate_accountability'),
      ('Alderman', 'data_privacy'),
      ('City Clerk', 'election_integrity'),
      ('City Clerk', 'government_efficiency'),
      ('City Clerk', 'data_privacy'),
      ('City Clerk', 'anti_corruption'),
      ('City Clerk', 'civil_rights'),
      ('City Clerk', 'corporate_accountability'),
      ('City Council Member', 'government_spending_reduction'),
      ('City Council Member', 'government_efficiency'),
      ('City Council Member', 'public_infrastructure'),
      ('City Council Member', 'housing_affordability'),
      ('City Council Member', 'environment_and_public_health'),
      ('City Council Member', 'public_safety_and_crime_control'),
      ('City Council Member', 'social_programs_and_welfare'),
      ('City Council Member', 'civil_rights'),
      ('City Council Member', 'anti_corruption'),
      ('City Council Member', 'corporate_accountability'),
      ('City Council Member', 'data_privacy'),
      ('City Treasurer', 'government_spending_reduction'),
      ('City Treasurer', 'government_efficiency'),
      ('City Treasurer', 'anti_corruption'),
      ('City Treasurer', 'data_privacy'),
      ('City Treasurer', 'corporate_accountability'),
      ('City Treasurer', 'public_infrastructure'),
      ('Municipal Assessor', 'housing_affordability'),
      ('Municipal Assessor', 'government_efficiency'),
      ('Municipal Assessor', 'anti_corruption'),
      ('Municipal Assessor', 'data_privacy'),
      ('Municipal Assessor', 'corporate_accountability'),
      ('Municipal Assessor', 'civil_rights'),
      ('Municipal Attorney', 'government_efficiency'),
      ('Municipal Attorney', 'civil_rights'),
      ('Municipal Attorney', 'anti_corruption'),
      ('Municipal Attorney', 'public_safety_and_crime_control'),
      ('Municipal Attorney', 'corporate_accountability'),
      ('Municipal Attorney', 'data_privacy'),
      ('Municipal Attorney', 'housing_affordability'),
      ('Municipal Attorney', 'environment_and_public_health'),
      ('Municipal Attorney', 'public_infrastructure'),
      ('Municipal Constable', 'public_safety_and_crime_control'),
      ('Municipal Constable', 'civil_rights'),
      ('Municipal Constable', 'housing_affordability'),
      ('Municipal Constable', 'government_efficiency'),
      ('Municipal Constable', 'data_privacy'),
      ('Municipal Constable', 'anti_corruption'),
      ('Town Council Member', 'government_spending_reduction'),
      ('Town Council Member', 'government_efficiency'),
      ('Town Council Member', 'public_infrastructure'),
      ('Town Council Member', 'housing_affordability'),
      ('Town Council Member', 'environment_and_public_health'),
      ('Town Council Member', 'public_safety_and_crime_control'),
      ('Town Council Member', 'social_programs_and_welfare'),
      ('Town Council Member', 'civil_rights'),
      ('Town Council Member', 'anti_corruption'),
      ('Town Council Member', 'corporate_accountability'),
      ('Town Council Member', 'data_privacy'),
      ('Town Moderator', 'government_efficiency'),
      ('Town Moderator', 'civil_rights'),
      ('Town Moderator', 'election_integrity'),
      ('Town Moderator', 'anti_corruption')
  ),
  target_offices AS (
    SELECT id
    FROM public.offices
    WHERE scope = 'place'
      AND canonical_name IN (SELECT DISTINCT canonical_name FROM desired)
  ),
  resolved AS (
    SELECT office.id AS office_id, area.id AS research_area_id
    FROM desired
    JOIN public.offices office
      ON office.scope = 'place'
     AND office.canonical_name = desired.canonical_name
    JOIN public.research_areas area
      ON area.slug = desired.slug
  )
  DELETE FROM public.office_research_areas existing
  WHERE existing.office_id IN (SELECT id FROM target_offices)
    AND NOT EXISTS (
      SELECT 1
      FROM resolved
      WHERE resolved.office_id = existing.office_id
        AND resolved.research_area_id = existing.research_area_id
    );

  WITH desired(canonical_name, slug) AS (
    VALUES
      ('Alderman', 'government_spending_reduction'),
      ('Alderman', 'government_efficiency'),
      ('Alderman', 'public_infrastructure'),
      ('Alderman', 'housing_affordability'),
      ('Alderman', 'environment_and_public_health'),
      ('Alderman', 'public_safety_and_crime_control'),
      ('Alderman', 'social_programs_and_welfare'),
      ('Alderman', 'civil_rights'),
      ('Alderman', 'anti_corruption'),
      ('Alderman', 'corporate_accountability'),
      ('Alderman', 'data_privacy'),
      ('City Clerk', 'election_integrity'),
      ('City Clerk', 'government_efficiency'),
      ('City Clerk', 'data_privacy'),
      ('City Clerk', 'anti_corruption'),
      ('City Clerk', 'civil_rights'),
      ('City Clerk', 'corporate_accountability'),
      ('City Council Member', 'government_spending_reduction'),
      ('City Council Member', 'government_efficiency'),
      ('City Council Member', 'public_infrastructure'),
      ('City Council Member', 'housing_affordability'),
      ('City Council Member', 'environment_and_public_health'),
      ('City Council Member', 'public_safety_and_crime_control'),
      ('City Council Member', 'social_programs_and_welfare'),
      ('City Council Member', 'civil_rights'),
      ('City Council Member', 'anti_corruption'),
      ('City Council Member', 'corporate_accountability'),
      ('City Council Member', 'data_privacy'),
      ('City Treasurer', 'government_spending_reduction'),
      ('City Treasurer', 'government_efficiency'),
      ('City Treasurer', 'anti_corruption'),
      ('City Treasurer', 'data_privacy'),
      ('City Treasurer', 'corporate_accountability'),
      ('City Treasurer', 'public_infrastructure'),
      ('Municipal Assessor', 'housing_affordability'),
      ('Municipal Assessor', 'government_efficiency'),
      ('Municipal Assessor', 'anti_corruption'),
      ('Municipal Assessor', 'data_privacy'),
      ('Municipal Assessor', 'corporate_accountability'),
      ('Municipal Assessor', 'civil_rights'),
      ('Municipal Attorney', 'government_efficiency'),
      ('Municipal Attorney', 'civil_rights'),
      ('Municipal Attorney', 'anti_corruption'),
      ('Municipal Attorney', 'public_safety_and_crime_control'),
      ('Municipal Attorney', 'corporate_accountability'),
      ('Municipal Attorney', 'data_privacy'),
      ('Municipal Attorney', 'housing_affordability'),
      ('Municipal Attorney', 'environment_and_public_health'),
      ('Municipal Attorney', 'public_infrastructure'),
      ('Municipal Constable', 'public_safety_and_crime_control'),
      ('Municipal Constable', 'civil_rights'),
      ('Municipal Constable', 'housing_affordability'),
      ('Municipal Constable', 'government_efficiency'),
      ('Municipal Constable', 'data_privacy'),
      ('Municipal Constable', 'anti_corruption'),
      ('Town Council Member', 'government_spending_reduction'),
      ('Town Council Member', 'government_efficiency'),
      ('Town Council Member', 'public_infrastructure'),
      ('Town Council Member', 'housing_affordability'),
      ('Town Council Member', 'environment_and_public_health'),
      ('Town Council Member', 'public_safety_and_crime_control'),
      ('Town Council Member', 'social_programs_and_welfare'),
      ('Town Council Member', 'civil_rights'),
      ('Town Council Member', 'anti_corruption'),
      ('Town Council Member', 'corporate_accountability'),
      ('Town Council Member', 'data_privacy'),
      ('Town Moderator', 'government_efficiency'),
      ('Town Moderator', 'civil_rights'),
      ('Town Moderator', 'election_integrity'),
      ('Town Moderator', 'anti_corruption')
  )
  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM desired
  JOIN public.offices office
    ON office.scope = 'place'
   AND office.canonical_name = desired.canonical_name
  JOIN public.research_areas area
    ON area.slug = desired.slug
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'election_integrity',
    'government_efficiency',
    'civil_rights',
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
  WHERE scope = 'place'
    AND canonical_name = 'Town Moderator';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=place canonical_name=Town Moderator, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for town moderator mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'place'
    AND office.canonical_name = 'Town Moderator'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'housing_affordability',
    'government_efficiency',
    'anti_corruption',
    'data_privacy',
    'corporate_accountability',
    'civil_rights'
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
    AND canonical_name = 'Municipal Assessor';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=place canonical_name=Municipal Assessor, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for municipal assessor mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'place'
    AND office.canonical_name = 'Municipal Assessor'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_efficiency',
    'civil_rights',
    'anti_corruption',
    'public_safety_and_crime_control',
    'corporate_accountability',
    'data_privacy',
    'housing_affordability',
    'environment_and_public_health',
    'public_infrastructure'
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
    AND canonical_name = 'Municipal Attorney';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=place canonical_name=Municipal Attorney, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for municipal attorney mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'place'
    AND office.canonical_name = 'Municipal Attorney'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_safety_and_crime_control',
    'civil_rights',
    'housing_affordability',
    'government_efficiency',
    'data_privacy',
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
  WHERE scope = 'place'
    AND canonical_name = 'Municipal Constable';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=place canonical_name=Municipal Constable, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for municipal constable mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'place'
    AND office.canonical_name = 'Municipal Constable'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'environment_and_public_health',
    'corporate_accountability',
    'government_efficiency',
    'social_programs_and_welfare',
    'cost_of_living_reduction',
    'public_infrastructure',
    'foreign_trade',
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
    AND canonical_name = 'Commissioner of Agriculture';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Commissioner of Agriculture, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for commissioner of agriculture mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Commissioner of Agriculture'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'healthcare_affordability',
    'cost_of_living_reduction',
    'corporate_accountability',
    'civil_rights',
    'data_privacy',
    'government_efficiency',
    'anti_corruption',
    'housing_affordability'
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
    AND canonical_name = 'Commissioner of Insurance';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Commissioner of Insurance, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for commissioner of insurance mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Commissioner of Insurance'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'cost_of_living_reduction',
    'public_infrastructure',
    'environment_and_public_health',
    'corporate_accountability',
    'government_efficiency',
    'anti_corruption',
    'data_privacy',
    'civil_rights'
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
    AND canonical_name = 'Corporation Commissioner';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Corporation Commissioner, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for corporation commissioner mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Corporation Commissioner'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'cost_of_living_reduction',
    'public_infrastructure',
    'environment_and_public_health',
    'corporate_accountability',
    'government_efficiency',
    'anti_corruption',
    'data_privacy',
    'civil_rights'
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
    AND canonical_name = 'Public Service Commissioner';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Public Service Commissioner, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for public service commissioner mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Public Service Commissioner'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_education_quality',
    'civil_rights',
    'government_efficiency',
    'government_spending_reduction',
    'data_privacy',
    'social_programs_and_welfare',
    'public_safety_and_crime_control',
    'healthcare_affordability',
    'anti_corruption',
    'environment_and_public_health',
    'public_infrastructure'
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
    AND canonical_name = 'State Board of Education Member';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=State Board of Education Member, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for state board of education member mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'State Board of Education Member'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_education_quality',
    'government_efficiency',
    'government_spending_reduction',
    'civil_rights',
    'social_programs_and_welfare',
    'data_privacy',
    'public_safety_and_crime_control',
    'healthcare_affordability',
    'anti_corruption',
    'environment_and_public_health',
    'public_infrastructure'
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
    AND canonical_name = 'Superintendent of Public Instruction';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Superintendent of Public Instruction, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for superintendent of public instruction mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Superintendent of Public Instruction'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_efficiency',
    'government_spending_reduction',
    'anti_corruption',
    'public_safety_and_crime_control',
    'civil_rights',
    'public_infrastructure',
    'corporate_accountability'
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
    AND canonical_name = 'Lieutenant Governor';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Lieutenant Governor, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for lieutenant governor mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Lieutenant Governor'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'election_integrity',
    'government_efficiency',
    'anti_corruption',
    'data_privacy',
    'civil_rights',
    'corporate_accountability'
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
    AND canonical_name = 'Secretary of State';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Secretary of State, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for secretary of state mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Secretary of State'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_efficiency',
    'anti_corruption',
    'government_spending_reduction',
    'data_privacy',
    'corporate_accountability',
    'civil_rights'
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
    AND canonical_name = 'State Auditor';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=State Auditor, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for state auditor mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'State Auditor'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_spending_reduction',
    'government_efficiency',
    'anti_corruption',
    'corporate_accountability',
    'data_privacy',
    'public_infrastructure'
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
    AND canonical_name = 'State Treasurer';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=State Treasurer, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for state treasurer mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'State Treasurer'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_spending_reduction',
    'government_efficiency',
    'anti_corruption',
    'corporate_accountability',
    'data_privacy'
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
    AND canonical_name = 'Comptroller';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Comptroller, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for comptroller mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Comptroller'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_safety_and_crime_control',
    'civil_rights',
    'corporate_accountability',
    'anti_corruption',
    'data_privacy',
    'environment_and_public_health',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'government_efficiency',
    'election_integrity',
    'womens_reproductive_rights',
    'immigration',
    'housing_affordability'
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
    AND canonical_name = 'Attorney General';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=Attorney General, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for attorney general mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'Attorney General'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_efficiency',
    'government_spending_reduction',
    'public_safety_and_crime_control',
    'public_infrastructure',
    'housing_affordability',
    'environment_and_public_health',
    'social_programs_and_welfare',
    'civil_rights',
    'anti_corruption',
    'corporate_accountability',
    'data_privacy'
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
    AND canonical_name = 'Mayor';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=place canonical_name=Mayor, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for mayor mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'place'
    AND office.canonical_name = 'Mayor'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_education_quality',
    'government_spending_reduction',
    'government_efficiency',
    'civil_rights',
    'data_privacy',
    'anti_corruption',
    'corporate_accountability'
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
    AND canonical_name = 'State Board of Regents Member';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=State Board of Regents Member, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for state board of regents mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'State Board of Regents Member'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'housing_affordability',
    'cost_of_living_reduction',
    'government_efficiency',
    'government_spending_reduction',
    'anti_corruption',
    'corporate_accountability',
    'civil_rights',
    'data_privacy'
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
    AND canonical_name = 'State Board of Equalization Member';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=statewide canonical_name=State Board of Equalization Member, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for state board of equalization mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'State Board of Equalization Member'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_safety_and_crime_control',
    'civil_rights',
    'government_efficiency',
    'anti_corruption',
    'data_privacy',
    'social_programs_and_welfare'
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
    AND canonical_name = 'Sheriff';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=Sheriff, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for sheriff mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'Sheriff'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'housing_affordability',
    'government_efficiency',
    'anti_corruption',
    'data_privacy',
    'corporate_accountability',
    'civil_rights'
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
    AND canonical_name = 'County Assessor';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Assessor, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county assessor mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Assessor'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_spending_reduction',
    'government_efficiency',
    'public_infrastructure',
    'housing_affordability',
    'environment_and_public_health',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'public_safety_and_crime_control',
    'anti_corruption',
    'corporate_accountability',
    'civil_rights',
    'data_privacy',
    'election_integrity'
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
    AND canonical_name = 'County Supervisor';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Supervisor, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county supervisor mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Supervisor'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'election_integrity',
    'government_efficiency',
    'data_privacy',
    'anti_corruption',
    'civil_rights',
    'corporate_accountability'
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
    AND canonical_name = 'County Clerk';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Clerk, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county clerk mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Clerk'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_spending_reduction',
    'government_efficiency',
    'public_infrastructure',
    'housing_affordability',
    'environment_and_public_health',
    'healthcare_affordability',
    'social_programs_and_welfare',
    'public_safety_and_crime_control',
    'anti_corruption',
    'corporate_accountability',
    'civil_rights',
    'data_privacy',
    'election_integrity'
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
    AND canonical_name = 'County Commissioner';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Commissioner, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county commissioner mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Commissioner'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_safety_and_crime_control',
    'environment_and_public_health',
    'civil_rights',
    'data_privacy',
    'anti_corruption',
    'government_efficiency',
    'social_programs_and_welfare'
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
    AND canonical_name = 'County Coroner';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Coroner, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county coroner mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Coroner'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'housing_affordability',
    'data_privacy',
    'government_efficiency',
    'anti_corruption',
    'civil_rights',
    'corporate_accountability'
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
    AND canonical_name = 'County Recorder';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Recorder, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county recorder mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Recorder'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_education_quality',
    'government_efficiency',
    'government_spending_reduction',
    'civil_rights',
    'social_programs_and_welfare',
    'data_privacy',
    'public_safety_and_crime_control',
    'healthcare_affordability',
    'anti_corruption',
    'environment_and_public_health',
    'public_infrastructure'
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
    AND canonical_name = 'County Superintendent of Schools';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Superintendent of Schools, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county superintendent of schools mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Superintendent of Schools'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'government_spending_reduction',
    'government_efficiency',
    'anti_corruption',
    'data_privacy',
    'housing_affordability',
    'corporate_accountability',
    'civil_rights'
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
    AND canonical_name = 'County Treasurer';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=County Treasurer, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for county treasurer mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Treasurer'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_scopes text[] := ARRAY[
    'school_elementary',
    'school_secondary',
    'school_unified'
  ]::text[];
  desired_slugs text[] := ARRAY[
    'public_education_quality',
    'government_spending_reduction',
    'government_efficiency',
    'civil_rights',
    'public_safety_and_crime_control',
    'social_programs_and_welfare',
    'data_privacy',
    'anti_corruption',
    'environment_and_public_health',
    'public_infrastructure'
  ]::text[];
  expected_scope_count integer;
  expected_area_count integer;
  office_count integer;
  area_count integer;
BEGIN
  expected_scope_count := COALESCE(array_length(desired_scopes, 1), 0);
  expected_area_count := COALESCE(array_length(desired_slugs, 1), 0);

  SELECT COUNT(*)
  INTO office_count
  FROM public.offices
  WHERE scope = ANY (desired_scopes)
    AND canonical_name = 'School Board Member';

  IF office_count <> expected_scope_count THEN
    RAISE EXCEPTION
      'Expected exactly % School Board Member office rows across school scopes, found %',
      expected_scope_count,
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for school board mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = ANY (desired_scopes)
    AND office.canonical_name = 'School Board Member'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

DO $$
DECLARE
  desired_slugs text[] := ARRAY[
    'public_safety_and_crime_control',
    'civil_rights',
    'anti_corruption',
    'government_efficiency',
    'corporate_accountability',
    'data_privacy',
    'social_programs_and_welfare'
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
    AND canonical_name = 'District Attorney';

  IF office_count <> 1 THEN
    RAISE EXCEPTION
      'Expected exactly 1 office row for scope=county canonical_name=District Attorney, found %',
      office_count;
  END IF;

  SELECT COUNT(*)
  INTO area_count
  FROM public.research_areas
  WHERE slug = ANY (desired_slugs);

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for district attorney mapping, found %',
      expected_area_count,
      area_count;
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (desired_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'District Attorney'
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

DELETE FROM public.office_research_areas ora
USING public.offices office, public.research_areas area
WHERE ora.office_id = office.id
  AND ora.research_area_id = area.id
  AND office.scope = 'statewide'
  AND office.canonical_name = 'Governor'
  AND area.slug IN ('legal_competence', 'impartiality');

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
    RAISE EXCEPTION
      'Expected % source research areas for reduce_wealth_gap backfill, found %',
      COALESCE(array_length(source_slugs, 1), 0),
      source_area_count;
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
    RAISE EXCEPTION
      'Expected at least 1 office for reduce_wealth_gap backfill, found 0';
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
