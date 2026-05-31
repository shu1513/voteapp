DO $$
DECLARE
  expected_area_count integer := 16;
  office_count integer;
  area_count integer;
BEGIN
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
  WHERE slug = ANY (
    ARRAY[
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
    ]::text[]
  );

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for senate mapping, found %',
      expected_area_count,
      area_count;
  END IF;
END
$$;

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'United States Senator'
  LIMIT 1
),
desired_research_area_slugs(slug) AS (
  VALUES
    ('national_defense'),
    ('peaceful_foreign_policy'),
    ('foreign_trade'),
    ('government_spending_and_deficit_reduction'),
    ('personal_income_tax_relief'),
    ('healthcare_affordability'),
    ('social_programs_and_welfare'),
    ('immigration'),
    ('civil_rights'),
    ('womens_reproductive_rights'),
    ('environment_and_public_health'),
    ('public_infrastructure'),
    ('corporate_accountability'),
    ('data_privacy'),
    ('anti_corruption'),
    ('government_efficiency')
),
target_areas AS (
  SELECT ra.id
  FROM public.research_areas ra
  JOIN desired_research_area_slugs desired
    ON desired.slug = ra.slug
)
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT office.id, area.id
FROM target_office office
CROSS JOIN target_areas area
ON CONFLICT (office_id, research_area_id) DO NOTHING;

DO $$
DECLARE
  expected_area_count integer := 16;
  office_count integer;
  area_count integer;
BEGIN
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
  WHERE slug = ANY (
    ARRAY[
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
    ]::text[]
  );

  IF area_count <> expected_area_count THEN
    RAISE EXCEPTION
      'Expected % research areas for us_house mapping, found %',
      expected_area_count,
      area_count;
  END IF;
END
$$;

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'us_house'
    AND canonical_name = 'United States Representative'
  LIMIT 1
),
desired_research_area_slugs(slug) AS (
  VALUES
    ('government_spending_and_deficit_reduction'),
    ('personal_income_tax_relief'),
    ('healthcare_affordability'),
    ('social_programs_and_welfare'),
    ('public_infrastructure'),
    ('national_defense'),
    ('foreign_trade'),
    ('immigration'),
    ('environment_and_public_health'),
    ('corporate_accountability'),
    ('data_privacy'),
    ('anti_corruption'),
    ('government_efficiency'),
    ('civil_rights'),
    ('womens_reproductive_rights'),
    ('peaceful_foreign_policy')
),
target_areas AS (
  SELECT ra.id
  FROM public.research_areas ra
  JOIN desired_research_area_slugs desired
    ON desired.slug = ra.slug
)
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT office.id, area.id
FROM target_office office
CROSS JOIN target_areas area
ON CONFLICT (office_id, research_area_id) DO NOTHING;
