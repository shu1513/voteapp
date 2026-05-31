BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'state_lower',
  'State Lower Chamber Legislator',
  'Represents a district in the state lower legislative chamber and votes on state laws and budget policy.'
)
ON CONFLICT (scope, canonical_name)
DO UPDATE SET
  summary = EXCLUDED.summary,
  updated_at = now();

WITH canonical AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'state_lower'
    AND canonical_name = 'State Lower Chamber Legislator'
  LIMIT 1
),
legacy AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'state_lower'
    AND canonical_name IN (
      'House Delegate',
      'State Assembly Member',
      'State Representative'
    )
)
UPDATE public.elections e
SET office_id = canonical.id
FROM canonical
WHERE e.office_id IN (SELECT id FROM legacy)
  AND e.office_id <> canonical.id;

WITH canonical AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'state_lower'
    AND canonical_name = 'State Lower Chamber Legislator'
  LIMIT 1
),
legacy AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'state_lower'
    AND canonical_name IN (
      'House Delegate',
      'State Assembly Member',
      'State Representative'
    )
)
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT canonical.id, legacy_links.research_area_id
FROM canonical
JOIN public.office_research_areas legacy_links
  ON legacy_links.office_id IN (SELECT id FROM legacy)
ON CONFLICT (office_id, research_area_id) DO NOTHING;

WITH legacy AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'state_lower'
    AND canonical_name IN (
      'House Delegate',
      'State Assembly Member',
      'State Representative'
    )
)
DELETE FROM public.office_research_areas
WHERE office_id IN (SELECT id FROM legacy);

WITH canonical AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'state_lower'
    AND canonical_name = 'State Lower Chamber Legislator'
  LIMIT 1
),
legacy AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'state_lower'
    AND canonical_name IN (
      'House Delegate',
      'State Assembly Member',
      'State Representative'
    )
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT canonical.id, aliases.scope, aliases.alias_text, aliases.normalized_alias
FROM canonical
JOIN public.office_title_aliases aliases
  ON aliases.office_id IN (SELECT id FROM legacy)
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

WITH legacy AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'state_lower'
    AND canonical_name IN (
      'House Delegate',
      'State Assembly Member',
      'State Representative'
    )
)
DELETE FROM public.office_title_aliases
WHERE office_id IN (SELECT id FROM legacy);

DELETE FROM public.offices
WHERE scope = 'state_lower'
  AND canonical_name IN (
    'House Delegate',
    'State Assembly Member',
    'State Representative'
  );

COMMIT;
