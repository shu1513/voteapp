-- Colorado counties such as Weld elect one combined Clerk and Recorder.
-- It is neither the separate County Clerk nor County Recorder role, so retain
-- the official combined office as its own catalog record.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'County Clerk and Recorder',
  'Keeps county records, including recorded property documents; runs county elections and maintains voter records; and handles licenses, permits, and other filings that county law assigns.'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', 'Clerk and Recorder', 'clerk and recorder'
FROM public.offices o
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Clerk and Recorder'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
