BEGIN;

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'State Auditor'
  LIMIT 1
),
aliases(alias_text, normalized_alias) AS (
  VALUES
    ('State Auditor', 'state auditor'),
    ('Auditor of State', 'auditor of state'),
    ('Auditor of Public Accounts', 'auditor of public accounts'),
    ('State Auditor and Inspector', 'state auditor and inspector'),
    ('State Auditor of Accounts', 'state auditor of accounts')
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
