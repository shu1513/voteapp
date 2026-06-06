BEGIN;

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'State Treasurer'
  LIMIT 1
),
aliases(alias_text, normalized_alias) AS (
  VALUES
    ('State Treasurer', 'state treasurer'),
    ('Treasurer of State', 'treasurer of state'),
    ('Commonwealth Treasurer', 'commonwealth treasurer'),
    ('Treasurer', 'treasurer')
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
