BEGIN;

WITH lieutenant_governor_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'Lieutenant Governor'
  LIMIT 1
)
DELETE FROM public.office_title_aliases a
USING lieutenant_governor_office
WHERE a.scope = 'statewide'
  AND a.normalized_alias IN (
    'governor and lieutenant governor',
    'governor lieutenant governor'
  )
  AND a.office_id = lieutenant_governor_office.id;

WITH governor_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'Governor'
  LIMIT 1
),
aliases(alias_text, normalized_alias) AS (
  VALUES
    ('Governor and Lieutenant Governor', 'governor and lieutenant governor'),
    ('Governor / Lieutenant Governor', 'governor lieutenant governor')
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT governor_office.id, 'statewide', aliases.alias_text, aliases.normalized_alias
FROM governor_office
CROSS JOIN aliases
ON CONFLICT (scope, normalized_alias)
DO NOTHING;

WITH governor_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'Governor'
  LIMIT 1
)
UPDATE public.elections
SET office_id = (SELECT id FROM governor_office),
    updated_at = now()
WHERE office_id IN (
    SELECT id
    FROM public.offices
    WHERE scope = 'statewide'
      AND canonical_name = 'Lieutenant Governor'
  )
  AND race_type = 'office'
  AND discovery_contest_family = 'non_judicial_office'
  AND official_ballot_title_key IN (
    'governor and lieutenant governor',
    'governor lieutenant governor'
  );

COMMIT;
