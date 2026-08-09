-- Alabama's elected county tax offices had no catalog home, and the gap did
-- not fail loudly — it mis-matched.
--
-- Live hit 2026-08-08 during the Nov-2026 district backfill: the verbatim
-- ballot title "Lee County Revenue Commissioner" resolved to County
-- Commissioner at deterministic_fallback confidence 0.800. The jurisdiction
-- strip deliberately keeps the generic civic word, so the scorer saw "county
-- revenue commissioner" — three tokens, two of which are the WHOLE name of the
-- county's LEGISLATIVE body — giving F1 0.8, clear of the 0.56 floor and the
-- 0.12 margin. A method=none aborts a payload loudly; this writes cleanly and
-- attaches the county commission's research areas to a property-tax office, so
-- roster, profile, records and label stages all inherit the wrong policy
-- context. "<County> License Commissioner" (Tuscaloosa) mis-matched
-- identically, and "<County> Tax Commissioner" (Georgia, 159 counties) is the
-- same shape. Same defect class as the NE/WI court-clerk bug repaired in
-- migration 218.
--
-- The "commissioner of X" word order is worse still: there "county
-- commissioner" sits inside the stripped title as a CONTIGUOUS PHRASE and
-- takes the phrase-containment boost, so "<County> Commissioner of Licenses"
-- (Calhoun AL) and "<County> Commissioner of the Revenue" (Virginia)
-- mis-matched at 0.920, not 0.800.
--
-- Alabama counties elect ONE of two arrangements under Title 40 of the Code of
-- Alabama: a merged Revenue Commissioner (Lee and most counties), or a
-- separate Tax Assessor and Tax Collector (Jefferson, Madison and Tuscaloosa —
-- all four of those seats are on the Nov 3 2026 ballot), typically alongside an
-- elected License Commissioner. Sources: the ALDOR county office directory
-- (https://www.revenue.alabama.gov/property-tax/county-offices-appraisal-assessment-records/),
-- which lists Lee County under "Revenue Commissioner" and Jefferson/Madison/
-- Tuscaloosa under "Tax Assessor" + "Tax Collector", and Clarke County's own
-- description of the 1988 local act that merged the two offices
-- (https://clarkecountyal.com/revenue-commissioner-clarke-co/).
--
-- So: the merged office gets its own row (exactly as County Assessor-Recorder
-- does for the combined assess-and-record office), and the two halves get
-- aliases onto the catalog offices that already describe them — County
-- Assessor and Collector of Revenue. Before this, "Tax Collector" matched
-- NOTHING at county scope (0.400), which also left Florida's 67 elected tax
-- collectors uncatalogued.
--
-- Two more offices need their own rows rather than aliases:
--
--   * LICENSE COMMISSIONER is not the St. Louis-style License Collector
--     already in the catalog. That office issues business licences; Alabama's
--     is the county TAG office — motor-vehicle titles and tags, manufactured
--     homes, driver-licence renewals, conservation permits and the sales/use
--     tax on vehicle and boat sales, with business licences one line of the
--     job. Mobile County's license commission raises roughly 57% of that
--     county's general-fund revenue
--     (https://www.mobilecountylc.gov/about-us/about-the-commissioner/, which
--     also confirms the office is elected to a four-year term). Aliasing it
--     onto License Collector would print an office name the ballot does not
--     use and a duty list missing the office's largest function.
--
--   * COMMISSIONER OF THE REVENUE is Virginia's chief local tax ASSESSING
--     officer, one of the five constitutional officers every county and
--     independent city elects to a four-year term under Art. VII Sec. 4. It
--     assesses personal-property, business-licence, machinery-and-tools and
--     meals taxes but does NOT collect — that is the elected Treasurer's job —
--     so it is neither Alabama's merged Revenue Commissioner nor a plain
--     County Assessor. Without its own row the bare title scored 1.000 into
--     Revenue Commissioner once that office existed, so adding the Alabama
--     office alone would have created a NEW confident wrong match.
--
-- A code-layer backstop ships alongside: officeMatcher.ts now scores any
-- qualified commissioner title ("<revenue|tax|license> commissioner" or
-- "commissioner of (the) licenses|revenue") to zero against County
-- Commissioner, AND ignores a stored alias of that shape that points at County
-- Commissioner — an exact alias hit returns before the scorer runs, so the
-- score guard alone would not fail safe on a database that had already learned
-- one. Together they mean a database that has not run this migration returns
-- the honest method=none instead of a confident wrong match. The seed layer
-- carries the same office, aliases and curated research areas for fresh
-- installs (seedOffices.ts + db/seeds/office_research_areas_v1.sql); the
-- summary text below is byte-identical to the seed's so the two layers do not
-- fight over it.
--
-- On a fresh migrations-only database research_areas is still empty
-- (DB_DEPLOYMENT.md runs db:seed:research-areas AFTER db:migrate), so the
-- research-area join inserts zero rows by design and the seed layer fills the
-- links afterward — same pattern as migrations 184, 206 and 216. An exception
-- there would brick every fresh install, so the missing-areas case is a
-- NOTICE, not an error.
--
-- Adding the office does not repair elections written before it existed; the
-- manual:elections:repair-office-ids wrapper re-runs the matcher over stranded
-- NULL-office shells. Rows already written with the WRONG office keep it (the
-- elections upsert only overwrites office_id on re-injection, and the repair
-- script deliberately revisits only NULL rows), so the repair statements at
-- the end of this migration are the path for rows already on disk. Every
-- statement here is idempotent: after a successful run no predicate matches,
-- so a replay is a no-op.

BEGIN;

-- Drop learned aliases that cemented the mis-match BEFORE seeding the correct
-- ones: (scope, normalized_alias) is unique, so an ON CONFLICT DO NOTHING
-- insert would otherwise leave the wrong row in place. A trigger blocks
-- reassigning an alias's office_id, so deletion is the only repair available.
DELETE FROM public.office_title_aliases a
USING public.offices o
WHERE o.id = a.office_id
  AND a.scope = 'county'
  AND (
    a.normalized_alias ~ '\m(revenue|tax|license) commissioner\M'
    OR a.normalized_alias ~ '\mcommissioner of (the )?(licen[cs]es?|revenue)\M'
  )
  AND o.canonical_name = 'County Commissioner';

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'Revenue Commissioner',
  'Estimating what each property in the county is worth, which sets how much property tax each owner pays
Collecting property taxes and the taxes owed on vehicles, boats, and manufactured homes
Keeping property maps, ownership records, and assessment records
Handling tax exemptions, such as homeowner or over-65 discounts'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

-- The bare alias is what the matcher's civic-word-free lookup lands on; the
-- county-qualified alias is the live ballot form, seeded so it resolves by
-- exact alias rather than by a scored match.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Revenue Commissioner', 'revenue commissioner'),
        ('County Revenue Commissioner', 'county revenue commissioner')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'Revenue Commissioner'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- The assessing half of the split Alabama arrangement, and the Florida/Georgia
-- spelling of the same job. The county-qualified form already scored 0.800
-- into the right office; the bare form fell under the floor at 0.500.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Tax Assessor', 'tax assessor'),
        ('County Tax Assessor', 'county tax assessor')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Assessor'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- The collecting half. Matched nothing before (0.400): every collector office
-- in the catalog is worded "Collector of Revenue" / "License Collector", which
-- share a single token with the title.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Tax Collector', 'tax collector'),
        ('County Tax Collector', 'county tax collector')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'Collector of Revenue'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Alabama's county tag office, distinct from the St. Louis-style License
-- Collector already in the catalog (see the header note).
INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'License Commissioner',
  'Issuing motor vehicle tags and titles, and registering boats and manufactured homes
Collecting the taxes and fees owed when a vehicle or boat is bought, registered, or renewed
Issuing business licenses and hunting and fishing licenses, and renewing driver licenses in many counties
Keeping the county''s vehicle, license, and ownership records'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

-- "Commissioner of Licenses" is Calhoun County's spelling of the same office.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('License Commissioner', 'license commissioner'),
        ('County License Commissioner', 'county license commissioner'),
        ('Commissioner of Licenses', 'commissioner of licenses'),
        ('County Commissioner of Licenses', 'county commissioner of licenses')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'License Commissioner'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Virginia's constitutional tax-assessing officer. Both the "of the" and bare
-- "of" spellings are live across localities.
INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'Commissioner of the Revenue',
  'Deciding what local taxes each resident and business owes, such as the tax on cars and business property
Running the local business license system and setting each business''s license tax
Keeping local tax records and reviewing appeals when a taxpayer disputes an assessment
Helping residents file their state income tax returns in most localities'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Commissioner of the Revenue', 'commissioner of the revenue'),
        ('Commissioner of Revenue', 'commissioner of revenue'),
        ('County Commissioner of the Revenue', 'county commissioner of the revenue'),
        ('County Commissioner of Revenue', 'county commissioner of revenue')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'Commissioner of the Revenue'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.research_areas) THEN
    RAISE NOTICE 'migration 223: research_areas is empty (fresh install); the new offices'' research areas will come from the seed layer';
  END IF;
END
$$;

-- Curated set mirrored from db/seeds/office_research_areas_v1.sql: the union of
-- the County Assessor and Collector of Revenue sets, since the officeholder
-- does both jobs — the same rule County Assessor-Recorder follows.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'corporate_accountability',
       'government_efficiency',
       'government_spending_reduction',
       'housing_affordability'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'Revenue Commissioner'
ON CONFLICT DO NOTHING;

-- License Collector's set (this office issues business licences too) plus
-- data_privacy, because it holds the county's vehicle-ownership and
-- driver-licence records.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'corporate_accountability',
       'data_privacy',
       'government_efficiency'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'License Commissioner'
ON CONFLICT DO NOTHING;

-- The County Assessor set: the assessing job without the collection half.
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
  AND o.canonical_name = 'Commissioner of the Revenue'
ON CONFLICT DO NOTHING;

-- Repoint county contests the defect already attached to County Commissioner.
-- Only titles with a catalogued target are moved: a "<County> Tax
-- Commissioner" row (Georgia) is deliberately left alone, because this catalog
-- has no Georgia tax-commissioner office and NULLing a written row would
-- strand its linked candidates at the records stage. That gap is reported
-- rather than half-repaired.
UPDATE public.elections e
SET office_id = target.id,
    updated_at = now()
FROM public.districts d,
     public.offices current_office,
     public.offices target
WHERE d.id = e.district_id
  AND current_office.id = e.office_id
  AND d.district_type = 'county'
  AND current_office.canonical_name = 'County Commissioner'
  AND target.scope = 'county'
  AND (
    (e.official_ballot_title ~* '\mrevenue commissioner\M' AND target.canonical_name = 'Revenue Commissioner')
    OR (e.official_ballot_title ~* '\m(license commissioner|commissioner of licen[cs]es)\M' AND target.canonical_name = 'License Commissioner')
    OR (e.official_ballot_title ~* '\mcommissioner of (the )?revenue\M' AND target.canonical_name = 'Commissioner of the Revenue')
  );

COMMIT;
