BEGIN;

WITH target_office AS (
  INSERT INTO public.offices (scope, canonical_name, summary)
  VALUES (
    'county',
    'District Attorney',
    'Serves as the county prosecutor, making charging decisions and representing the public in criminal prosecutions.'
  )
  ON CONFLICT (scope, canonical_name)
  DO UPDATE SET
    summary = EXCLUDED.summary,
    updated_at = now()
  RETURNING id
),
resolved_office AS (
  SELECT id FROM target_office
  UNION ALL
  SELECT id
  FROM public.offices
  WHERE scope = 'county'
    AND canonical_name = 'District Attorney'
  LIMIT 1
),
aliases(alias_text, normalized_alias) AS (
  VALUES
    ('District Attorney', 'district attorney'),
    ('County District Attorney', 'county district attorney'),
    ('Prosecuting Attorney', 'prosecuting attorney'),
    ('County Prosecutor', 'county prosecutor')
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT resolved_office.id, 'county', aliases.alias_text, aliases.normalized_alias
FROM resolved_office
CROSS JOIN aliases
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

COMMIT;
