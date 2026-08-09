-- A Florida community development district is a special-purpose local
-- government created under Fla. Stat. ch. 190 to finance and maintain a
-- development's infrastructure. Its board has five members, and s. 190.006(4)
-- names the office outright: "Members of the board shall be known as
-- supervisors." The catalog had no row for it.
--
-- Only the qualified-elector elections reach a public ballot. A new district's
-- supervisors are chosen at a landowners' meeting by acre-weighted vote (s.
-- 190.006(2)); once the district passes the thresholds in s. 190.006(3)(a)2,
-- seats transfer to the qualified electors and are elected at the November
-- general election, nonpartisan, with candidates qualifying for individual
-- seats under s. 99.061. The county supervisor of elections prepares the
-- ballot and the county canvassing board certifies the result (s.
-- 190.006(3)(b)-(d)) — which is why this is filed at county scope, the same as
-- Fire Control District Commissioner (migration 216).
--
-- Unlike the City Marshal gap repaired in migration 224, this one failed
-- LOUDLY: every title form the researcher probed returned method=none rather
-- than a confident wrong office, so no CDD contest was ever written and there
-- is no learned alias to dislodge. That is why this migration has no repair or
-- alias-deletion clause — there is nothing mis-pointed to fix. Live hit
-- 2026-08-08: Bay County's Lake Powell Residential Golf Community Development
-- District, Seats 2 and 5, two qualified candidates each, all four stranded.
--
-- The matching half of the fix is in officeMatcher.ts. A CDD ballot title
-- names the district, not the office ("Lake Powell Residential Golf Community
-- Development District, Seat 2"), and the district's proper name cannot be
-- removed by the jurisdiction strip: that keys on the districts row, which is
-- the COUNTY (Bay), while the title names the CDD. The matcher now reduces the
-- title to the bare civic phrase, which the "Community Development District"
-- alias below then wins.
--
-- CAVEAT for whoever imports these contests, inherited from Fire Control
-- District Commissioner and sharper here: a CDD's boundaries are SUB-county —
-- often a single subdivision — and there is no districts row for one, so an
-- election attached to the county district shows to every voter in the county
-- rather than only the few hundred inside the district. Weigh that before
-- publishing a roster.
--
-- The summary is byte-identical to backend/src/scripts/seedOffices.ts so the
-- migration and seed layers cannot fight over the text, and the research-area
-- links mirror db/seeds/office_research_areas_v1.sql. On a fresh
-- migrations-only database research_areas is still empty (DB_DEPLOYMENT.md runs
-- db:seed:research-areas AFTER db:migrate), so the research-area join inserts
-- zero rows by design and the seed layer fills them in afterward — same pattern
-- as migrations 184, 206, 216, 223 and 224, and the reason the missing-areas
-- case is a NOTICE rather than an error.
--
-- Adding an office does not repair elections written before it existed:
-- `npm run manual:elections:repair-office-ids` re-runs the current matcher over
-- stranded NULL-office shells and backfills office_id.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'Community Development District Supervisor',
  'Setting the district''s budget and the assessments property owners inside it pay
Overseeing infrastructure the district financed or maintains, such as roads, drainage, and water and sewer lines
Managing shared amenities like parks, ponds, pools, and clubhouses
Approving contracts with the district''s manager, engineers, and vendors'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

-- "Community Development District" is the phrase that survives the matcher's
-- proper-name strip, so it carries the live ballot form. The others cover what
-- a ballot or a candidate list may print directly.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Community Development District Supervisor', 'community development district supervisor'),
        ('Community Development District', 'community development district'),
        ('CDD Supervisor', 'cdd supervisor'),
        ('Community Development District Board of Supervisors', 'community development district board of supervisors')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'Community Development District Supervisor'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.research_areas) THEN
    RAISE NOTICE 'migration 228: research_areas is empty (fresh install); Community Development District Supervisor research areas will come from the seed layer';
  END IF;
END
$$;

-- Curated set mirrored from db/seeds/office_research_areas_v1.sql: the Fire
-- Control District Commissioner set is the closest analogue (an independent
-- special taxing district), minus public_safety_and_crime_control, which a CDD
-- has no role in, plus housing_affordability, because CDD assessments ride on
-- the homes inside the district and are a standing cost of living there.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'environment_and_public_health',
       'government_efficiency',
       'government_spending_reduction',
       'housing_affordability',
       'public_infrastructure'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'Community Development District Supervisor'
ON CONFLICT DO NOTHING;

COMMIT;
