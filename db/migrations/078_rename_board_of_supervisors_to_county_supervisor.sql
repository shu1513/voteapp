BEGIN;

WITH target_office AS (
  INSERT INTO public.offices (scope, canonical_name, summary)
  VALUES (
    'county',
    'County Supervisor',
    'Serves on the county governing board responsible for county budgets, ordinances, services, and administrative oversight.'
  )
  ON CONFLICT (scope, canonical_name)
  DO UPDATE SET
    summary = EXCLUDED.summary,
    updated_at = now()
  RETURNING id
),
resolved_target AS (
  SELECT id FROM target_office
  UNION ALL
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'County Supervisor'
  LIMIT 1
),
legacy_offices AS (
  SELECT legacy.id AS legacy_id, target.id AS target_id
  FROM public.offices legacy
  CROSS JOIN resolved_target target
  WHERE legacy.scope = 'county'
    AND legacy.canonical_name = 'Board of Supervisors'
)
UPDATE public.elections election
SET office_id = legacy_offices.target_id,
    updated_at = now()
FROM legacy_offices
WHERE election.office_id = legacy_offices.legacy_id;

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'County Supervisor'
  LIMIT 1
),
legacy_offices AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'Board of Supervisors'
),
legacy_links AS (
  SELECT target_office.id AS target_id, link.research_area_id
  FROM target_office
  JOIN public.office_research_areas link
    ON link.office_id IN (SELECT id FROM legacy_offices)
)
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT target_id, research_area_id
FROM legacy_links
ON CONFLICT (office_id, research_area_id) DO NOTHING;

WITH legacy_offices AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'Board of Supervisors'
)
DELETE FROM public.office_research_areas
WHERE office_id IN (SELECT id FROM legacy_offices);

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'County Supervisor'
  LIMIT 1
),
legacy_offices AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'Board of Supervisors'
),
legacy_aliases AS (
  SELECT target_office.id AS target_id, alias.scope, alias.alias_text, alias.normalized_alias
  FROM target_office
  JOIN public.office_title_aliases alias
    ON alias.office_id IN (SELECT id FROM legacy_offices)
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT target_id, scope, alias_text, normalized_alias
FROM legacy_aliases
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

WITH legacy_offices AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'Board of Supervisors'
)
DELETE FROM public.office_title_aliases
WHERE office_id IN (SELECT id FROM legacy_offices);

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'County Supervisor'
  LIMIT 1
),
aliases(alias_text, normalized_alias) AS (
  VALUES
    ('County Supervisor', 'county supervisor'),
    ('Supervisor', 'supervisor'),
    ('Member, Board of Supervisors', 'member board of supervisors'),
    ('Member of the Board of Supervisors', 'member of the board of supervisors'),
    ('Board of Supervisors', 'board of supervisors'),
    ('County Board Supervisor', 'county board supervisor')
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT target_office.id, 'county', aliases.alias_text, aliases.normalized_alias
FROM target_office
CROSS JOIN aliases
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

DELETE FROM public.offices
WHERE scope = 'county'
  AND canonical_name = 'Board of Supervisors';

COMMIT;
