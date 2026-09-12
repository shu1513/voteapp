-- The District of Columbia's ward State Board of Education seats had no
-- catalog home, and the gap failed the whole contest rather than mis-matching.
--
-- DC's ward rows are typed state_upper: the ward's Council seat stands in for
-- a state senate seat, and the elections validator hard-rejects any title on a
-- state_upper row that names a board of education. But the same ward ballot
-- elects the ward's member of the DC State Board of Education (DC Code
-- § 38-2651: one member per ward plus one at large, four-year terms, wards
-- 1/3/5/6 in the midterm and 2/4/7/8 in the presidential year). The DCBOE
-- certified list for 2026-11-03 carries all four midterm seats ("Ward 5 Member
-- of the State Board of Education", contested in three wards), and the city
-- coverage sweep had to leave every one of them out as an uncatalogued office.
--
-- The board is the same policy job as the statewide boards already in the
-- catalog — it approves the academic standards, graduation requirements and
-- accountability plan that the Office of the State Superintendent of
-- Education carries out, and it runs the Office of the Ombudsman for Public
-- Education — but it is elected from a ward, so it needs a row at the scope
-- the ward district carries. Its summary is its own: unlike a statewide board
-- it does not oversee the education department, which reports to the Mayor.
--
-- Code ships alongside: officeMatcher.ts routes a DC state_upper title that
-- names the state board to this office (every other state_upper title still
-- takes the chamber seat, and the same title on another state's row still
-- fails loudly), and electionsValidator.ts stops treating that one title as a
-- mis-scoped school race. The seed layer (seedOffices.ts +
-- db/seeds/office_research_areas_v1.sql) carries the office, summary and
-- curated research areas for fresh installs; the summary text below is
-- byte-identical to the seed's so the two layers do not fight over it.
--
-- Also here: Yellowstone County MT consolidates its treasurer, assessor and
-- superintendent of schools into one elected seat (MCA 7-4-2301) and titles it
-- by all three. The function-noun veto drops County Treasurer for that title
-- and the schools office wins on token count, so both live spellings are
-- seeded as aliases onto County Treasurer — the office is the county's
-- tax-collection and vehicle-licensing department with the other two duties
-- annexed. Nothing mis-matched on disk (the contest was excluded), so no
-- repair statement is needed.
--
-- On a fresh migrations-only database research_areas is still empty
-- (DB_DEPLOYMENT.md runs db:seed:research-areas AFTER db:migrate), so the
-- research-area join inserts zero rows by design and the seed layer fills the
-- links afterward — same pattern as migrations 184, 206, 216 and 223. Every
-- statement is idempotent: after a successful run no predicate matches, so a
-- replay is a no-op.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'state_upper',
  'State Board of Education Member',
  'Approving what students must learn in each grade
Approving the rules for graduating from high school
Running the office that takes your complaints about public schools'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Superintendent of Schools/Assessor/Treasurer', 'superintendent of schools assessor treasurer'),
        ('Treasurer/Assessor/Superintendent of Schools', 'treasurer assessor superintendent of schools')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Treasurer'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.research_areas) THEN
    RAISE NOTICE 'migration 280: research_areas is empty (fresh install); the new office''s research areas will come from the seed layer';
  END IF;
END
$$;

-- Curated set mirrored from db/seeds/office_research_areas_v1.sql: the
-- statewide State Board of Education Member's set.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'civil_rights',
       'data_privacy',
       'government_efficiency',
       'government_spending_reduction',
       'public_education_quality'
     ]::text[])
WHERE o.scope = 'state_upper'
  AND o.canonical_name = 'State Board of Education Member'
ON CONFLICT DO NOTHING;

COMMIT;
