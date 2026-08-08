-- Louisiana justice-of-the-peace and constable seats had no way into the
-- office catalog, which blocked ~12 contested Nov-3-2026 races across Orleans,
-- Jefferson and Caddo parishes during a manual-research sweep (2026-08-08).
-- Two distinct failures, both fixed here plus in the matcher's seat-strip
-- rules (backend/src/pipeline/elections/officeMatcher.ts):
--
--   1. Justice of the Peace had no catalog office at all. It is a real elected
--      office in Louisiana (ward-level, La. Const. art. V s 20), Texas,
--      Arizona, Delaware and others — the limited-jurisdiction court that hears
--      small claims and evictions — not a seat on the county trial court. The
--      matcher's judicial fallback was routing (and, in already-seeded
--      databases, had LEARNED aliases routing) every JP title to the generic
--      County Level Judge, so the new office also has to reclaim those aliases.
--
--   2. Constable existed but no Louisiana seat form reached it. Jefferson's
--      "Constable 2nd Justice Court" scored 0.520 (below the 0.56 floor) and
--      hard-failed the writer for the whole payload; Caddo's "Constable Justice
--      of the Peace Ward 7" is a third form again. Orleans' "Constable 1st City
--      Court" correctly hard-fails county validation — First City Court is
--      municipal, so that seat belongs to the New Orleans PLACE district, whose
--      catalog office is Municipal Constable and which had no bare-constable
--      alias.
--
-- The seed layer (seedOffices.ts + db/seeds/office_research_areas_v1.sql)
-- carries the same office, aliases, and curated research areas for fresh
-- installs; this migration applies them to already-seeded databases (same
-- pattern as migrations 158, 169 and 184).
--
-- Alias insertion does not repair elections written before the alias existed;
-- the manual:elections:repair-office-ids wrapper re-runs the matcher over
-- stranded NULL-office shells and backfills office_id.

BEGIN;

-- Summary is a newline-separated list of duty bullets, byte-identical to the
-- seedOffices.ts entry: the election page renders each line as a bullet under
-- "Justice of the Peace is responsible for:", so a prose paragraph here would
-- render as one run-on bullet on any database that runs migrations without the
-- seed layer.
INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'Justice of the Peace',
  'Hearing small-claims cases and disputes between neighbors, landlords, and tenants
Deciding eviction cases in many states
Handling traffic and minor criminal citations where state law allows it
Performing marriages and signing routine legal papers such as affidavits'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

-- Reclaim justice-of-the-peace aliases that were learned into County Level
-- Judge before this office existed (Texas precinct forms and the Louisiana
-- doubled form, both live). Leaving them in place would keep the generic judge
-- office shadowing the new one: the alias lookup runs before every fallback, so
-- the bare "justice of the peace" key alone would swallow every LA JP seat.
--
-- Rewritten as delete-then-insert, not UPDATE: trigger
-- trg_prevent_office_title_alias_reassignment (migration 087) blocks changing
-- an alias's office_id, and re-pointing a mis-learned alias onto a newly added
-- office is exactly the curated exception that guard is not meant to cover.
-- Staged through a temp table so the DELETE and the INSERT are separate
-- statements — a single data-modifying CTE would still see the deleted rows in
-- the unique index. Each alias keeps its original alias_text.
--
-- Anchored on the leading phrase so only titles that ARE a JP seat move:
-- "county justice of the supreme court 1st judicial district" (NY, live) and
-- every "constable justice of the peace ..." form are left alone.
CREATE TEMP TABLE justice_of_the_peace_alias_reclaim AS
SELECT a.alias_text, a.normalized_alias
FROM public.office_title_aliases a
JOIN public.offices o
  ON o.id = a.office_id
WHERE a.scope = 'county'
  AND o.canonical_name = 'County Level Judge'
  AND a.normalized_alias ~ '^justice of the peace( |$)';

DELETE FROM public.office_title_aliases a
USING justice_of_the_peace_alias_reclaim r
WHERE a.scope = 'county'
  AND a.normalized_alias = r.normalized_alias;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', r.alias_text, r.normalized_alias
FROM justice_of_the_peace_alias_reclaim r
CROSS JOIN public.offices o
WHERE o.scope = 'county'
  AND o.canonical_name = 'Justice of the Peace'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

DROP TABLE justice_of_the_peace_alias_reclaim;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o
JOIN (
  VALUES
    ('Justice of the Peace', 'Justice of the Peace', 'justice of the peace'),
    -- The LA SOS concatenates the office name onto the ballot title, so the
    -- raw form arrives doubled ("Justice of the Peace Justice of the Peace
    -- Ward 1", Caddo live). The matcher collapses the repeat, but the raw
    -- title is looked up first, so key the source form too.
    (
      'Justice of the Peace',
      'Justice of the Peace Justice of the Peace',
      'justice of the peace justice of the peace'
    ),
    -- Caddo Parish titles the constable seat by the justice court it serves;
    -- the ward number strips as a seat suffix, leaving the court words.
    ('Constable', 'Constable Justice of the Peace', 'constable justice of the peace')
) AS v(canonical_name, alias_text, normalized_alias)
  ON v.canonical_name = o.canonical_name
WHERE o.scope = 'county'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Place scope: Orleans Parish's "Constable 1st City Court" belongs to the
-- New Orleans place district, where the seat strip reduces it to the bare
-- office word. County scope has carried a bare 'constable' alias since
-- migration 184; place scope only had the qualified city/town/village forms.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'place', 'Constable', 'constable'
FROM public.offices o
WHERE o.scope = 'place'
  AND o.canonical_name = 'Municipal Constable'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Research areas for the new office: the County Level Judge curated set,
-- unchanged. A JP court is the limited-jurisdiction tier of the same judiciary
-- — the eviction and small-claims forum, and in states such as Texas the Class
-- C misdemeanor and traffic court. Fresh migration-only databases have no
-- research areas yet and receive these from the seed layer instead.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT office.id, area.id
FROM public.offices office
JOIN public.research_areas area
  ON area.slug = ANY (
    ARRAY[
      'civil_rights',
      'housing_affordability',
      'impartiality',
      'legal_competence',
      'public_safety_and_crime_control'
    ]::text[]
  )
WHERE office.scope = 'county'
  AND office.canonical_name = 'Justice of the Peace'
ON CONFLICT (office_id, research_area_id) DO NOTHING;

COMMIT;
