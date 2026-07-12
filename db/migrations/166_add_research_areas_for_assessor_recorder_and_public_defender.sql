-- Migration 165 seeded the County Assessor-Recorder and Public Defender
-- offices (SF 2026 races) but linked no research areas, so on an
-- already-seeded database both offices sit outside every research-area-driven
-- flow until the next full re-seed. This migration fills the links, following
-- the migration 158 pattern (Public Administrator).
--
-- Research-area LINKS are owned by the seed layer (db/seeds/
-- office_research_areas_v1.sql), whose final curation pass now lists both
-- offices; the sets below are copies of those curated rows so the two layers
-- converge to the same state. Set rationale:
--
--   County Assessor-Recorder: union of the County Assessor and County
--   Recorder curated sets, since the combined office does both jobs.
--
--   Public Defender: defense-side justice set, NOT a District Attorney
--   mirror -- the DA slugs tracking prosecutorial charging discretion
--   (gun_control, womens_reproductive_rights, corporate_accountability,
--   anti_corruption) don't apply to an office whose job is representing the
--   accused. legal_competence mirrors the judge sets (quality of
--   representation); immigration covers crimmigration consequences of pleas
--   (Padilla).
--
-- On a fresh migrations-only database research_areas is still empty
-- (DB_DEPLOYMENT.md runs db:seed:research-areas AFTER db:migrate), the joins
-- below insert zero rows by design, and the seed layer fills the links
-- afterward. An exception here would brick every fresh install, so the
-- missing-areas case is a NOTICE, not an error.

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.research_areas) THEN
    RAISE NOTICE 'migration 166: research_areas is empty (fresh install); Assessor-Recorder and Public Defender research areas will come from the seed layer';
  END IF;
END
$$;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'corporate_accountability',
       'data_privacy',
       'government_efficiency',
       'housing_affordability'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Assessor-Recorder'
ON CONFLICT DO NOTHING;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'civil_rights',
       'immigration',
       'legal_competence',
       'public_safety_and_crime_control'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'Public Defender'
ON CONFLICT DO NOTHING;

COMMIT;
