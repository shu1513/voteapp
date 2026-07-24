-- South Carolina counties such as Spartanburg elect a chair of their
-- legislative county council. Map this governing-body leadership title to
-- County Supervisor, not County Executive (the county administrator runs
-- day-to-day government).

BEGIN;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', 'County Council Chair', 'county council chair'
FROM public.offices o
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Supervisor'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
