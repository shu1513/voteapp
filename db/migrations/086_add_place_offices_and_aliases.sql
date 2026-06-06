BEGIN;

WITH office_seeds(canonical_name, summary) AS (
  VALUES
    (
      'Town Moderator',
      'Presides over town meetings or local deliberative proceedings, helping manage procedure, debate, and public votes.'
    ),
    (
      'Municipal Assessor',
      'Determines local property valuations used for municipal taxation and maintains assessment records.'
    ),
    (
      'Municipal Attorney',
      'Provides legal representation and advice for municipal government and may handle local legal matters assigned by law.'
    ),
    (
      'Municipal Constable',
      'Performs local law enforcement, civil process, or public safety duties assigned by municipal or state law.'
    )
)
INSERT INTO public.offices (scope, canonical_name, summary)
SELECT 'place', canonical_name, summary
FROM office_seeds
ON CONFLICT (scope, canonical_name)
DO UPDATE SET
  summary = EXCLUDED.summary,
  updated_at = now();

WITH aliases(canonical_name, alias_text, normalized_alias) AS (
  VALUES
    ('City Council Member', 'City Council Member', 'city council member'),
    ('City Council Member', 'Council Member', 'council member'),
    ('City Council Member', 'City Councilor', 'city councilor'),
    ('City Council Member', 'Municipal Council Member', 'municipal council member'),
    ('City Council Member', 'Municipal Governing Board Member', 'municipal governing board member'),
    ('Town Council Member', 'Town Council Member', 'town council member'),
    ('Town Council Member', 'Town Board Member', 'town board member'),
    ('Town Council Member', 'Select Board Member', 'select board member'),
    ('Town Council Member', 'Town Select Board Member', 'town select board member'),
    ('City Clerk', 'City Clerk', 'city clerk'),
    ('City Clerk', 'Municipal Clerk', 'municipal clerk'),
    ('City Clerk', 'Town Clerk', 'town clerk'),
    ('City Clerk', 'Village Clerk', 'village clerk'),
    ('City Treasurer', 'City Treasurer', 'city treasurer'),
    ('City Treasurer', 'Municipal Treasurer', 'municipal treasurer'),
    ('City Treasurer', 'Town Treasurer', 'town treasurer'),
    ('City Treasurer', 'Village Treasurer', 'village treasurer'),
    ('Town Moderator', 'Town Moderator', 'town moderator'),
    ('Town Moderator', 'Moderator', 'moderator'),
    ('Municipal Assessor', 'Municipal Assessor', 'municipal assessor'),
    ('Municipal Assessor', 'City Assessor', 'city assessor'),
    ('Municipal Assessor', 'Town Assessor', 'town assessor'),
    ('Municipal Assessor', 'Village Assessor', 'village assessor'),
    ('Municipal Attorney', 'Municipal Attorney', 'municipal attorney'),
    ('Municipal Attorney', 'City Attorney', 'city attorney'),
    ('Municipal Attorney', 'Town Attorney', 'town attorney'),
    ('Municipal Attorney', 'Village Attorney', 'village attorney'),
    ('Municipal Constable', 'Municipal Constable', 'municipal constable'),
    ('Municipal Constable', 'City Constable', 'city constable'),
    ('Municipal Constable', 'Town Constable', 'town constable'),
    ('Municipal Constable', 'Village Constable', 'village constable')
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT office.id, 'place', aliases.alias_text, aliases.normalized_alias
FROM aliases
JOIN public.offices office
  ON office.scope = 'place'
 AND office.canonical_name = aliases.canonical_name
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

COMMIT;
