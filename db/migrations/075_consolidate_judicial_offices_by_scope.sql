BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES
  (
    'statewide',
    'State Level Judge',
    'Serves in a statewide judicial role, reviewing cases and applying state constitutional, statutory, and procedural law.'
  ),
  (
    'county',
    'County Level Judge',
    'Serves in a county-level judicial role, hearing cases and issuing rulings under state and local court procedure.'
  ),
  (
    'place',
    'Place Level Judge',
    'Serves in a municipal or place-level judicial role, handling local court matters and applying relevant law and procedure.'
  )
ON CONFLICT (scope, canonical_name)
DO UPDATE SET
  summary = EXCLUDED.summary,
  updated_at = now();

WITH mapping(legacy_scope, legacy_name, canonical_scope, canonical_name) AS (
  VALUES
    ('statewide', 'State Supreme Court Justice', 'statewide', 'State Level Judge'),
    ('statewide', 'State Court of Appeals Judge', 'statewide', 'State Level Judge'),
    ('county', 'Superior Court Judge', 'county', 'County Level Judge'),
    ('county', 'Probate Judge', 'county', 'County Level Judge'),
    ('place', 'Municipal Judge', 'place', 'Place Level Judge')
),
legacy AS (
  SELECT legacy_office.id AS legacy_id, canonical_office.id AS canonical_id
  FROM mapping
  JOIN public.offices legacy_office
    ON legacy_office.scope = mapping.legacy_scope
   AND legacy_office.canonical_name = mapping.legacy_name
  JOIN public.offices canonical_office
    ON canonical_office.scope = mapping.canonical_scope
   AND canonical_office.canonical_name = mapping.canonical_name
)
UPDATE public.elections election
SET office_id = legacy.canonical_id
FROM legacy
WHERE election.office_id = legacy.legacy_id
  AND election.office_id <> legacy.canonical_id;

WITH county_judge AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'County Level Judge'
  LIMIT 1
),
district_attorney AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'District Attorney'
  LIMIT 1
)
UPDATE public.elections election
SET office_id = county_judge.id
FROM county_judge, district_attorney
WHERE election.office_id = district_attorney.id
  AND election.discovery_contest_family = 'judicial_office';

WITH district_attorney AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'District Attorney'
  LIMIT 1
)
UPDATE public.elections election
SET office_id = NULL
FROM district_attorney
WHERE election.office_id = district_attorney.id;

WITH mapping(legacy_scope, legacy_name, canonical_scope, canonical_name) AS (
  VALUES
    ('statewide', 'State Supreme Court Justice', 'statewide', 'State Level Judge'),
    ('statewide', 'State Court of Appeals Judge', 'statewide', 'State Level Judge'),
    ('county', 'Superior Court Judge', 'county', 'County Level Judge'),
    ('county', 'Probate Judge', 'county', 'County Level Judge'),
    ('place', 'Municipal Judge', 'place', 'Place Level Judge')
),
legacy AS (
  SELECT legacy_office.id AS legacy_id, canonical_office.id AS canonical_id
  FROM mapping
  JOIN public.offices legacy_office
    ON legacy_office.scope = mapping.legacy_scope
   AND legacy_office.canonical_name = mapping.legacy_name
  JOIN public.offices canonical_office
    ON canonical_office.scope = mapping.canonical_scope
   AND canonical_office.canonical_name = mapping.canonical_name
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT legacy.canonical_id, aliases.scope, aliases.alias_text, aliases.normalized_alias
FROM legacy
JOIN public.office_title_aliases aliases
  ON aliases.office_id = legacy.legacy_id
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

WITH explicit_aliases(scope, canonical_name, alias_text, normalized_alias) AS (
  VALUES
    ('statewide', 'State Level Judge', 'State Supreme Court Justice', 'state supreme court justice'),
    ('statewide', 'State Level Judge', 'State Court of Appeals Judge', 'state court of appeals judge'),
    ('statewide', 'State Level Judge', 'Judge', 'judge'),
    ('statewide', 'State Level Judge', 'Justice', 'justice'),
    ('county', 'County Level Judge', 'Superior Court Judge', 'superior court judge'),
    ('county', 'County Level Judge', 'Probate Judge', 'probate judge'),
    ('county', 'County Level Judge', 'Judge', 'judge'),
    ('place', 'Place Level Judge', 'Municipal Judge', 'municipal judge'),
    ('place', 'Place Level Judge', 'Judge', 'judge')
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT office.id, explicit_aliases.scope, explicit_aliases.alias_text, explicit_aliases.normalized_alias
FROM explicit_aliases
JOIN public.offices office
  ON office.scope = explicit_aliases.scope
 AND office.canonical_name = explicit_aliases.canonical_name
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

WITH obsolete AS (
  SELECT id
  FROM public.offices
  WHERE (scope = 'statewide' AND canonical_name IN ('State Supreme Court Justice', 'State Court of Appeals Judge'))
     OR (scope = 'county' AND canonical_name IN ('District Attorney', 'Superior Court Judge', 'Probate Judge'))
     OR (scope = 'place' AND canonical_name = 'Municipal Judge')
)
DELETE FROM public.office_title_aliases
WHERE office_id IN (SELECT id FROM obsolete);

WITH obsolete AS (
  SELECT id
  FROM public.offices
  WHERE (scope = 'statewide' AND canonical_name IN ('State Supreme Court Justice', 'State Court of Appeals Judge'))
     OR (scope = 'county' AND canonical_name IN ('District Attorney', 'Superior Court Judge', 'Probate Judge'))
     OR (scope = 'place' AND canonical_name = 'Municipal Judge')
)
DELETE FROM public.office_research_areas
WHERE office_id IN (SELECT id FROM obsolete);

WITH obsolete AS (
  SELECT id
  FROM public.offices
  WHERE (scope = 'statewide' AND canonical_name IN ('State Supreme Court Justice', 'State Court of Appeals Judge'))
     OR (scope = 'county' AND canonical_name IN ('District Attorney', 'Superior Court Judge', 'Probate Judge'))
     OR (scope = 'place' AND canonical_name = 'Municipal Judge')
)
DELETE FROM public.offices
WHERE id IN (SELECT id FROM obsolete);

WITH target_offices AS (
  SELECT id
  FROM public.offices
  WHERE (scope = 'statewide' AND canonical_name = 'State Level Judge')
     OR (scope = 'county' AND canonical_name = 'County Level Judge')
     OR (scope = 'place' AND canonical_name = 'Place Level Judge')
)
DELETE FROM public.office_research_areas
WHERE office_id IN (SELECT id FROM target_offices);

DO $$
DECLARE
  statewide_slugs text[] := ARRAY[
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
    'immigration'
  ]::text[];
  county_slugs text[] := ARRAY[
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
    'immigration'
  ]::text[];
  place_slugs text[] := ARRAY[
    'public_safety_and_crime_control',
    'civil_rights',
    'government_efficiency',
    'housing_affordability',
    'data_privacy',
    'corporate_accountability',
    'anti_corruption'
  ]::text[];
BEGIN
  IF (
    SELECT COUNT(*)
    FROM public.research_areas
    WHERE slug = ANY (statewide_slugs)
  ) <> COALESCE(array_length(statewide_slugs, 1), 0) THEN
    RAISE EXCEPTION 'Missing research areas for State Level Judge mapping';
  END IF;

  IF (
    SELECT COUNT(*)
    FROM public.research_areas
    WHERE slug = ANY (county_slugs)
  ) <> COALESCE(array_length(county_slugs, 1), 0) THEN
    RAISE EXCEPTION 'Missing research areas for County Level Judge mapping';
  END IF;

  IF (
    SELECT COUNT(*)
    FROM public.research_areas
    WHERE slug = ANY (place_slugs)
  ) <> COALESCE(array_length(place_slugs, 1), 0) THEN
    RAISE EXCEPTION 'Missing research areas for Place Level Judge mapping';
  END IF;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (statewide_slugs)
  WHERE office.scope = 'statewide'
    AND office.canonical_name = 'State Level Judge'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (county_slugs)
  WHERE office.scope = 'county'
    AND office.canonical_name = 'County Level Judge'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM public.offices office
  JOIN public.research_areas area
    ON area.slug = ANY (place_slugs)
  WHERE office.scope = 'place'
    AND office.canonical_name = 'Place Level Judge'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

COMMIT;
