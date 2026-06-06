BEGIN;

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'Lieutenant Governor'
  LIMIT 1
),
aliases(alias_text, normalized_alias) AS (
  VALUES
    ('Lieutenant Governor', 'lieutenant governor'),
    ('Lt. Governor', 'lt governor')
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT target_office.id, 'statewide', aliases.alias_text, aliases.normalized_alias
FROM target_office
CROSS JOIN aliases
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

COMMIT;
