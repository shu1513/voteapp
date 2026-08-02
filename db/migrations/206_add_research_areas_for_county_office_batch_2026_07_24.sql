-- The 2026-07-24 seedOffices batch (county clerk and recorder, county
-- engineer, St. Louis collector offices, recorder of deeds, board of review)
-- added six county offices with aliases but linked NO research areas — the
-- same defect class migration 166 repaired for Assessor-Recorder: on an
-- already-seeded database the offices sit outside every research-area-driven
-- flow, and a candidate-records label pass for them collapses to the
-- office-independent slugs (general, integrity_and_ethics). Observed live in
-- backfill wave 22: a Recorder of Deeds unit with 17 substantive records
-- needed a confirmed only_general_labels gap purely because of this catalog
-- hole.
--
-- Research-area LINKS are owned by the seed layer (db/seeds/
-- office_research_areas_v1.sql), whose curation pass now lists all six
-- offices; the sets below are copies of those curated rows so the two layers
-- converge to the same state. Set rationale lives with the seed rows:
-- Recorder of Deeds mirrors County Recorder (same job, Missouri/Pennsylvania
-- name); County Clerk and Recorder is the Clerk+Recorder union; Collector of
-- Revenue mirrors County Treasurer; License Collector adds
-- corporate_accountability to the records-office core; County Engineer takes
-- the Surveyor infrastructure set with government_spending_reduction in place
-- of the land-records housing slug; County Board of Review Member mirrors
-- County Assessor.
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
    RAISE NOTICE 'migration 206: research_areas is empty (fresh install); the 2026-07-24 county office batch research areas will come from the seed layer';
  END IF;
END
$$;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'data_privacy',
       'government_efficiency'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'Recorder of Deeds'
ON CONFLICT DO NOTHING;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'data_privacy',
       'election_integrity',
       'government_efficiency'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Clerk and Recorder'
ON CONFLICT DO NOTHING;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'government_efficiency',
       'government_spending_reduction',
       'housing_affordability'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'Collector of Revenue'
ON CONFLICT DO NOTHING;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'corporate_accountability',
       'government_efficiency'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'License Collector'
ON CONFLICT DO NOTHING;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'government_efficiency',
       'government_spending_reduction',
       'public_infrastructure'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Engineer'
ON CONFLICT DO NOTHING;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'corporate_accountability',
       'government_efficiency',
       'housing_affordability'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Board of Review Member'
ON CONFLICT DO NOTHING;

COMMIT;
