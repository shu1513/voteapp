BEGIN;

WITH target_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'County Supervisor'
  LIMIT 1
),
aliases(alias_text, normalized_alias) AS (
  VALUES
    ('Board of Supervisors Member', 'board of supervisors member'),
    ('County Council Member', 'county council member'),
    ('County Board Member', 'county board member'),
    ('County Legislator', 'county legislator'),
    ('Parish Police Juror', 'parish police juror'),
    ('Fiscal Court Member', 'fiscal court member'),
    ('Quorum Court Member', 'quorum court member')
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

COMMIT;
