-- Migration 165 added the County Assessor-Recorder and Public Defender
-- offices and their title aliases, but aliases only help elections written
-- AFTER the alias exists: the elections upsert repairs office_id via
-- COALESCE(EXCLUDED.office_id, ...) only when discovery re-runs for the
-- district, and rediscovery is both feature-flagged off by default
-- (ELECTIONS_SEARCH_ROLLOVER_ENABLED) and subject to a 180-day cooldown. Any
-- election row for these titles that persisted with office_id = NULL before
-- migration 165 would therefore stay unlinked indefinitely. This backfill
-- links those rows deterministically.
--
-- Scope guards: exact normalized-title match (the same keys the matcher
-- resolves alias_exact), county districts only (so a hypothetical place-scope
-- "Public Defender" contest is not linked to the county office), office rows
-- only, and only where office_id is still NULL. Idempotent: zero rows on a
-- database with no stranded elections (including fresh installs, where
-- elections is empty).

BEGIN;

UPDATE public.elections AS e
SET office_id = o.id,
    updated_at = now()
FROM public.offices AS o,
     public.districts AS d
WHERE e.office_id IS NULL
  AND e.race_type = 'office'
  AND d.id = e.district_id
  AND d.district_type = 'county'
  AND o.scope = 'county'
  AND (
    (
      o.canonical_name = 'County Assessor-Recorder'
      AND e.official_ballot_title_key IN ('assessor recorder', 'county assessor recorder')
    )
    OR (
      o.canonical_name = 'Public Defender'
      AND e.official_ballot_title_key = 'public defender'
    )
  );

COMMIT;
