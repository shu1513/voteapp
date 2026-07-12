-- Two San Francisco 2026 county races hit live with no catalog office to
-- attach to: the city-county combines assessor and recorder duties into a
-- single elected "Assessor-Recorder", and the Public Defender is elected
-- (rare among counties, but SF votes on it). With no office row the election
-- shell persists with office_id = NULL, which hard-blocks the
-- candidate-records stage (same failure mode as migrations 158 and 164).
-- This migration seeds both county-scope offices and maps their ballot
-- titles. The seed layer (seedOffices.ts) carries the same rows for fresh
-- installs; on an already-seeded database every insert is a no-op.
--
-- The matcher normalizes titles by lowercasing and stripping punctuation, so
-- "Assessor-Recorder" normalizes to the hyphenless "assessor recorder" form
-- below.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES
  (
    'county',
    'County Assessor-Recorder',
    'Assesses taxable property and records deeds, liens, and other official land documents when a county combines assessor and recorder duties.'
  ),
  (
    'county',
    'Public Defender',
    'Provides constitutionally required defense representation to eligible people accused of crimes and unable to afford counsel.'
  )
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Assessor-Recorder', 'assessor recorder'),
        ('County Assessor-Recorder', 'county assessor recorder')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Assessor-Recorder'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', 'Public Defender', 'public defender'
FROM public.offices o
WHERE o.scope = 'county'
  AND o.canonical_name = 'Public Defender'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
