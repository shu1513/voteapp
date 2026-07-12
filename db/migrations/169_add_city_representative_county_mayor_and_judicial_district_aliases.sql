-- Three office-title alias gaps hit live by manual research runs, all of which
-- leave election shells with office_id = NULL (hard-blocking the
-- candidate-records stage) when the official ballot title has no catalog match:
--
-- 1. El Paso titles its city council members "City Representative"
--    ("City Representative District 1, City of El Paso"). The matcher's
--    jurisdiction/seat stripping reduces that title to "city representative",
--    so one generic place-scope alias covers every city using the form.
-- 2. Several Florida/Tennessee-style charters title the elected county
--    executive "County Mayor" (hit live: Orange County FL). The catalog's
--    canonical office is "County Executive".
-- 3. New York elects Supreme Court Justices by numbered Judicial District
--    (1st-13th). The aliases for the 2nd and 11th exist only as
--    runtime-learned rows on databases that already matched them; the
--    remaining districts (hit live: the 1st, in New York County's 2026
--    certification) have no alias anywhere. Seed all thirteen so any county's
--    Supreme Court vacancy matches the county-scope judge office.
--
-- The seed layer (seedOffices.ts) carries the same aliases for fresh installs;
-- on a fresh migrations-only database the offices do not exist yet, the
-- SELECTs insert zero rows by design, and the seed layer fills them in
-- afterward (same pattern as migrations 158 and 164).

BEGIN;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'place', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('City Representative', 'city representative')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'place'
  AND o.canonical_name = 'City Council Member'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('County Mayor', 'county mayor')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Executive'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Supreme Court Justice - 1st Judicial District', 'supreme court justice 1st judicial district'),
        ('Supreme Court Justice - 2nd Judicial District', 'supreme court justice 2nd judicial district'),
        ('Supreme Court Justice - 3rd Judicial District', 'supreme court justice 3rd judicial district'),
        ('Supreme Court Justice - 4th Judicial District', 'supreme court justice 4th judicial district'),
        ('Supreme Court Justice - 5th Judicial District', 'supreme court justice 5th judicial district'),
        ('Supreme Court Justice - 6th Judicial District', 'supreme court justice 6th judicial district'),
        ('Supreme Court Justice - 7th Judicial District', 'supreme court justice 7th judicial district'),
        ('Supreme Court Justice - 8th Judicial District', 'supreme court justice 8th judicial district'),
        ('Supreme Court Justice - 9th Judicial District', 'supreme court justice 9th judicial district'),
        ('Supreme Court Justice - 10th Judicial District', 'supreme court justice 10th judicial district'),
        ('Supreme Court Justice - 11th Judicial District', 'supreme court justice 11th judicial district'),
        ('Supreme Court Justice - 12th Judicial District', 'supreme court justice 12th judicial district'),
        ('Supreme Court Justice - 13th Judicial District', 'supreme court justice 13th judicial district')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Level Judge'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
