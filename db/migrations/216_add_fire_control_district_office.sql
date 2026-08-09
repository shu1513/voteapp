-- Independent fire-control districts elect their own boards, and the catalog
-- had no office for them. Live hit 2026-08-08: Florida's Notice of General
-- Election for Santa Rosa County
-- (https://files.floridados.gov/media/710515/2026_nge_san_english.pdf) puts 13
-- fire-district seats on the Nov 3 2026 ballot across five districts (Avalon
-- Beach-Mulat Fire Protection District, Holley-Navarre Fire District, Midway
-- Fire District, Navarre Beach Fire Rescue District, Pace Fire Rescue
-- District). Two are contested per the supervisor of elections' candidate
-- list: Navarre Beach Fire Rescue District Seat 5 and Holley-Navarre Fire
-- District Seat 3. With no offices row and no county-scope alias matching a
-- title like "Holley-Navarre Fire District Seat 3", both contests had to be
-- dropped from the elections payload rather than written as NULL-office
-- shells, which would have stranded every linked candidate at the records
-- stage.
--
-- These districts are their own elected taxing bodies, separate from county
-- and city government. The office is filed at county scope because a district
-- sits inside one county and its seats print on that county's ballot.
--
-- Aliases alone cannot cover the family: every ballot title carries the
-- district's own proper noun, which is unenumerable and dilutes token overlap
-- (the live titles scored 0.40-0.57 against the matcher's 0.56 floor). The
-- matching fix is in the code layer — officeMatcher.ts folds any
-- "<name> Fire [Control|Rescue|Protection] District ..." body form onto this
-- office's canonical key — and the self-alias below is what that folded key
-- lands on. The remaining aliases cover the bare district-flavor forms a
-- ballot may print without a district name.
--
-- The seed layer carries the same office, aliases, and curated research areas
-- for fresh installs (seedOffices.ts + db/seeds/office_research_areas_v1.sql);
-- the summary text below is byte-identical to the seed's so the two layers do
-- not fight over it. On a fresh migrations-only database research_areas is
-- still empty (DB_DEPLOYMENT.md runs db:seed:research-areas AFTER db:migrate),
-- so the research-area join inserts zero rows by design and the seed layer
-- fills the links afterward — same pattern as migrations 184 and 206. An
-- exception there would brick every fresh install, so the missing-areas case
-- is a NOTICE, not an error.
--
-- Adding the office does not repair elections written before it existed; the
-- manual:elections:repair-office-ids wrapper re-runs the matcher over stranded
-- shells and backfills office_id.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'Fire Control District Commissioner',
  'Setting the budget for an independent fire district and the taxes or assessments that pay for it
Overseeing the district''s fire stations, trucks, and emergency medical service
Hiring and supervising the fire chief
Approving contracts, staffing levels, and equipment purchases for the district'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Fire Control District Commissioner', 'fire control district commissioner'),
        ('Fire District Commissioner', 'fire district commissioner'),
        ('Fire Rescue District Commissioner', 'fire rescue district commissioner'),
        ('Fire Protection District Commissioner', 'fire protection district commissioner'),
        ('Fire Commissioner', 'fire commissioner')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'Fire Control District Commissioner'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.research_areas) THEN
    RAISE NOTICE 'migration 216: research_areas is empty (fresh install); Fire Control District Commissioner research areas will come from the seed layer';
  END IF;
END
$$;

-- Curated set mirrored from db/seeds/office_research_areas_v1.sql: the
-- emergency-response slugs the board actually controls (fire and EMS coverage,
-- stations and apparatus) plus the two spending slugs every self-taxing
-- special district owns. No housing_affordability — the district's charge
-- lands on the tax bill, but the board neither values nor administers
-- property.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'environment_and_public_health',
       'government_efficiency',
       'government_spending_reduction',
       'public_infrastructure',
       'public_safety_and_crime_control'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'Fire Control District Commissioner'
ON CONFLICT DO NOTHING;

COMMIT;
