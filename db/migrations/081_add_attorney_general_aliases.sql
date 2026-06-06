BEGIN;

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'Attorney General'
  LIMIT 1
),
aliases(alias_text, normalized_alias) AS (
  VALUES
    ('Attorney General', 'attorney general'),
    ('State Attorney General', 'state attorney general'),
    ('Commonwealth Attorney General', 'commonwealth attorney general'),
    ('Attorney General and Reporter', 'attorney general and reporter')
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
