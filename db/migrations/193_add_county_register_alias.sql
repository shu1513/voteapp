-- New Jersey's elected County Register is the county land-records officer
-- (Register of Deeds and Mortgages). Map the official title to the existing
-- County Recorder office; no new canonical office is needed.
--
-- The seed layer carries the same alias for fresh installs. Already-written
-- NULL-office shells are repaired separately through
-- manual:elections:repair-office-ids after this migration lands.

BEGIN;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', 'County Register', 'county register'
FROM public.offices o
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Recorder'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
