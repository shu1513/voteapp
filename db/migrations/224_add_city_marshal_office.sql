-- A Louisiana city marshal is the elected law-enforcement officer of the city
-- court (La. R.S. 13:1879 et seq.) — an officer OF the court, not a judge. The
-- catalog had no row for the office, and the gap did not fail loudly: because
-- the ballot title names the court the marshal serves ("City Marshal, City
-- Court of Shreveport"), a judicial-family entry tripped officeMatcher's
-- judicial allow-markers and the judge fast path returned Place Level Judge at
-- confidence 1.000. That writes cleanly and hands a law-enforcement contest a
-- judge's research areas, which the roster, profile, records and label stages
-- all then inherit — the same defect class as the NE/WI court-clerk bug
-- repaired in migration 218, and as the Alabama tax offices catalogued in
-- migration 223. Live hit 2026-08-08: three qualifiers for Shreveport City
-- Marshal.
--
-- The matching half of the fix is in officeMatcher.ts, which now reads
-- "marshal" and "constable" as NON-judicial title markers, so a database that
-- has not run this migration returns the honest method=none instead of a
-- confident wrong office. This migration supplies the office those titles land
-- on once it has.
--
-- The aliases cover the Louisiana city-court forms only. A town or village
-- marshal (Indiana, Colorado) is a different job — the municipality's chief
-- police officer, not an officer of the court — and the summary below, which
-- renders to voters as "City Marshal is responsible for:", would misdescribe
-- it. Those titles keep returning no-match, which stays honest until someone
-- catalogs that office on its own evidence.
--
-- The summary is byte-identical to backend/src/scripts/seedOffices.ts so the
-- migration and seed layers cannot fight over the text, and the research-area
-- links mirror db/seeds/office_research_areas_v1.sql. On a fresh
-- migrations-only database research_areas is still empty (DB_DEPLOYMENT.md runs
-- db:seed:research-areas AFTER db:migrate), so the research-area join inserts
-- zero rows by design and the seed layer fills them in afterward — same pattern
-- as migrations 184, 206, 216 and 223, and the reason the missing-areas case is
-- a NOTICE rather than an error.
--
-- Adding an office does not repair elections written before it existed:
-- `npm run manual:elections:repair-office-ids` re-runs the current matcher over
-- stranded NULL-office shells and backfills office_id.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'place',
  'City Marshal',
  'Carrying out the city court''s orders, such as warrants, evictions, and seizures of property
Serving court papers, including subpoenas and legal notices
Keeping order and providing security in the courtroom
Enforcing the law within the court''s area, as state law allows'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'place', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('City Marshal', 'city marshal'),
        ('Marshal', 'marshal'),
        ('City Court Marshal', 'city court marshal'),
        ('Marshal of the City Court', 'marshal of the city court')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'place'
  AND o.canonical_name = 'City Marshal'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.research_areas) THEN
    RAISE NOTICE 'migration 224: research_areas is empty (fresh install); City Marshal research areas will come from the seed layer';
  END IF;
END
$$;

-- Curated set mirrored from db/seeds/office_research_areas_v1.sql: the
-- Municipal Constable set, because it is the same civil-process job, with
-- housing_affordability because the marshal executes evictions.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'civil_rights',
       'housing_affordability',
       'public_safety_and_crime_control'
     ]::text[])
WHERE o.scope = 'place'
  AND o.canonical_name = 'City Marshal'
ON CONFLICT DO NOTHING;

COMMIT;
