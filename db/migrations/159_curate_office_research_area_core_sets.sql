-- Curate office research-area links down to the areas each office has REAL,
-- direct influence over. The original seeds were generous: near-universal
-- filler (anti_corruption / civil_rights / data_privacy / reduce_wealth_gap
-- on almost every office) diluted both candidate-record labeling and
-- area-based ballot personalization. Principles applied per office:
--   * Keep an area only if the office's formal powers move outcomes in it
--     (a sheriff sets jail/ICE policy; a town moderator does not move
--     public safety).
--   * Corruption/ethics RECORDS remain labelable for every office through
--     the code-level universal areas (general, integrity_and_ethics), so
--     anti_corruption is kept only where fighting corruption is part of the
--     office's own portfolio (auditors, AGs, DAs, records custodians,
--     treasurers, election officers).
--   * Judicial offices keep impartiality/legal_competence plus the dockets
--     they actually decide (eviction -> housing, sentencing -> public
--     safety; state high courts also decide election and abortion cases).
--   * Two corrections ADD a missing core area: Sheriff + immigration
--     (287(g)/detainer cooperation is the sheriff's call) and District
--     Attorney + womens_reproductive_rights (charging discretion under
--     state abortion laws).
-- Deliberately untouched: plenary offices whose breadth is real (President/
-- VP, Governor, U.S. Senator/Representative, state legislators). The
-- federal 17-area set intentionally excludes state-owned areas
-- (public_education_quality, election_integrity) per existing design.
--
-- Research-area LINKS are owned by the seed layer
-- (db/seeds/office_research_areas_v1.sql), which now ends with this same
-- curation block. This migration applies the same statement to
-- already-seeded databases; on a fresh migrations-only database it
-- no-ops by design and the seed layer produces the curated state.

BEGIN;

-- The curated data lives in exactly ONE place per file: this temp table.
-- Both the guard and the reconcile statement below read from it, so a future
-- single-office edit cannot leave a DELETE copy and an INSERT copy out of sync
-- (a slug present only in a DELETE copy would be pruned and never restored).
-- No ON COMMIT DROP: the table is dropped explicitly so the file behaves the
-- same whether the caller wraps it in a transaction (the migration and seed
-- runners both do) or runs it statement-at-a-time.
DROP TABLE IF EXISTS curated_office_core_areas;

CREATE TEMP TABLE curated_office_core_areas (
    scope text NOT NULL,
    canonical_name text NOT NULL,
    slugs text[] NOT NULL
);

INSERT INTO curated_office_core_areas (scope, canonical_name, slugs) VALUES
    ('county', 'Clerk of Court', ARRAY['anti_corruption', 'data_privacy', 'government_efficiency']::text[]),
    ('county', 'County Assessor', ARRAY['anti_corruption', 'corporate_accountability', 'government_efficiency', 'housing_affordability']::text[]),
    ('county', 'County Auditor', ARRAY['anti_corruption', 'corporate_accountability', 'election_integrity', 'government_efficiency', 'government_spending_reduction']::text[]),
    ('county', 'County Clerk', ARRAY['anti_corruption', 'data_privacy', 'election_integrity', 'government_efficiency']::text[]),
    ('county', 'County Commissioner', ARRAY['environment_and_public_health', 'government_efficiency', 'government_spending_reduction', 'healthcare_affordability', 'housing_affordability', 'public_infrastructure', 'public_safety_and_crime_control', 'social_programs_and_welfare']::text[]),
    ('county', 'County Coroner', ARRAY['civil_rights', 'environment_and_public_health', 'government_efficiency', 'public_safety_and_crime_control']::text[]),
    ('county', 'County Executive', ARRAY['environment_and_public_health', 'government_efficiency', 'government_spending_reduction', 'healthcare_affordability', 'housing_affordability', 'public_infrastructure', 'public_safety_and_crime_control', 'social_programs_and_welfare']::text[]),
    ('county', 'County Level Judge', ARRAY['civil_rights', 'housing_affordability', 'impartiality', 'legal_competence', 'public_safety_and_crime_control']::text[]),
    ('county', 'County Recorder', ARRAY['anti_corruption', 'data_privacy', 'government_efficiency']::text[]),
    ('county', 'County Superintendent of Schools', ARRAY['civil_rights', 'government_efficiency', 'government_spending_reduction', 'public_education_quality']::text[]),
    ('county', 'County Supervisor', ARRAY['environment_and_public_health', 'government_efficiency', 'government_spending_reduction', 'healthcare_affordability', 'housing_affordability', 'public_infrastructure', 'public_safety_and_crime_control', 'social_programs_and_welfare']::text[]),
    ('county', 'County Treasurer', ARRAY['anti_corruption', 'government_efficiency', 'government_spending_reduction', 'housing_affordability']::text[]),
    ('county', 'District Attorney', ARRAY['anti_corruption', 'civil_rights', 'corporate_accountability', 'public_safety_and_crime_control', 'womens_reproductive_rights']::text[]),
    ('county', 'Public Administrator', ARRAY['anti_corruption', 'data_privacy', 'government_efficiency']::text[]),
    ('county', 'Sheriff', ARRAY['civil_rights', 'data_privacy', 'immigration', 'public_safety_and_crime_control']::text[]),
    ('place', 'Alderman', ARRAY['civil_rights', 'environment_and_public_health', 'government_efficiency', 'government_spending_reduction', 'housing_affordability', 'public_infrastructure', 'public_safety_and_crime_control', 'social_programs_and_welfare']::text[]),
    ('place', 'City Clerk', ARRAY['anti_corruption', 'data_privacy', 'election_integrity', 'government_efficiency']::text[]),
    ('place', 'City Council Member', ARRAY['civil_rights', 'environment_and_public_health', 'government_efficiency', 'government_spending_reduction', 'housing_affordability', 'public_infrastructure', 'public_safety_and_crime_control', 'social_programs_and_welfare']::text[]),
    ('place', 'City Treasurer', ARRAY['anti_corruption', 'government_efficiency', 'government_spending_reduction']::text[]),
    ('place', 'Mayor', ARRAY['civil_rights', 'environment_and_public_health', 'government_efficiency', 'government_spending_reduction', 'housing_affordability', 'public_infrastructure', 'public_safety_and_crime_control', 'social_programs_and_welfare']::text[]),
    ('place', 'Municipal Assessor', ARRAY['anti_corruption', 'corporate_accountability', 'government_efficiency', 'housing_affordability']::text[]),
    ('place', 'Municipal Attorney', ARRAY['civil_rights', 'government_efficiency', 'housing_affordability', 'public_safety_and_crime_control']::text[]),
    ('place', 'Municipal Constable', ARRAY['civil_rights', 'housing_affordability', 'public_safety_and_crime_control']::text[]),
    ('place', 'Place Level Judge', ARRAY['civil_rights', 'housing_affordability', 'impartiality', 'legal_competence', 'public_safety_and_crime_control']::text[]),
    ('place', 'Town Council Member', ARRAY['civil_rights', 'environment_and_public_health', 'government_efficiency', 'government_spending_reduction', 'housing_affordability', 'public_infrastructure', 'public_safety_and_crime_control', 'social_programs_and_welfare']::text[]),
    ('place', 'Town Moderator', ARRAY['election_integrity', 'government_efficiency']::text[]),
    ('school_elementary', 'School Board Member', ARRAY['civil_rights', 'data_privacy', 'government_efficiency', 'government_spending_reduction', 'public_education_quality', 'public_infrastructure', 'public_safety_and_crime_control']::text[]),
    ('school_secondary', 'School Board Member', ARRAY['civil_rights', 'data_privacy', 'government_efficiency', 'government_spending_reduction', 'public_education_quality', 'public_infrastructure', 'public_safety_and_crime_control']::text[]),
    ('school_unified', 'School Board Member', ARRAY['civil_rights', 'data_privacy', 'government_efficiency', 'government_spending_reduction', 'public_education_quality', 'public_infrastructure', 'public_safety_and_crime_control']::text[]),
    ('statewide', 'Attorney General', ARRAY['anti_corruption', 'civil_rights', 'corporate_accountability', 'data_privacy', 'election_integrity', 'environment_and_public_health', 'healthcare_affordability', 'immigration', 'public_safety_and_crime_control', 'womens_reproductive_rights']::text[]),
    ('statewide', 'Commissioner of Agriculture', ARRAY['corporate_accountability', 'cost_of_living_reduction', 'environment_and_public_health', 'foreign_trade', 'social_programs_and_welfare']::text[]),
    ('statewide', 'Commissioner of Insurance', ARRAY['corporate_accountability', 'cost_of_living_reduction', 'healthcare_affordability', 'housing_affordability']::text[]),
    ('statewide', 'Comptroller', ARRAY['anti_corruption', 'corporate_accountability', 'government_efficiency', 'government_spending_reduction']::text[]),
    ('statewide', 'Corporation Commissioner', ARRAY['corporate_accountability', 'cost_of_living_reduction', 'environment_and_public_health', 'public_infrastructure']::text[]),
    ('statewide', 'Labor Commissioner', ARRAY['civil_rights', 'corporate_accountability', 'reduce_wealth_gap', 'social_programs_and_welfare']::text[]),
    ('statewide', 'Land Commissioner', ARRAY['corporate_accountability', 'environment_and_public_health', 'government_spending_reduction', 'housing_affordability']::text[]),
    ('statewide', 'Lieutenant Governor', ARRAY['government_efficiency', 'government_spending_reduction', 'public_infrastructure']::text[]),
    ('statewide', 'Public Service Commissioner', ARRAY['corporate_accountability', 'cost_of_living_reduction', 'environment_and_public_health', 'public_infrastructure']::text[]),
    ('statewide', 'Railroad Commissioner', ARRAY['corporate_accountability', 'cost_of_living_reduction', 'environment_and_public_health', 'public_infrastructure']::text[]),
    ('statewide', 'Secretary of State', ARRAY['anti_corruption', 'civil_rights', 'data_privacy', 'election_integrity', 'government_efficiency']::text[]),
    ('statewide', 'State Auditor', ARRAY['anti_corruption', 'corporate_accountability', 'government_efficiency', 'government_spending_reduction']::text[]),
    ('statewide', 'State Board of Education Member', ARRAY['civil_rights', 'data_privacy', 'government_efficiency', 'government_spending_reduction', 'public_education_quality']::text[]),
    ('statewide', 'State Board of Equalization Member', ARRAY['corporate_accountability', 'cost_of_living_reduction', 'government_efficiency', 'housing_affordability']::text[]),
    ('statewide', 'State Board of Regents Member', ARRAY['civil_rights', 'government_efficiency', 'government_spending_reduction', 'public_education_quality']::text[]),
    ('statewide', 'State Level Judge', ARRAY['civil_rights', 'election_integrity', 'impartiality', 'legal_competence', 'public_safety_and_crime_control', 'womens_reproductive_rights']::text[]),
    ('statewide', 'State Treasurer', ARRAY['anti_corruption', 'government_efficiency', 'government_spending_reduction']::text[]),
    ('statewide', 'Superintendent of Public Instruction', ARRAY['civil_rights', 'data_privacy', 'government_efficiency', 'government_spending_reduction', 'public_education_quality']::text[]);

-- Fail fast rather than silently shrinking an office. The reconcile below
-- deletes every link outside the curated set and re-inserts the curated ones by
-- joining research_areas on slug; a curated slug that resolves to no research
-- area would therefore be deleted-and-not-restored, with no error.
--
-- The check is deliberately scoped to curated offices that ALREADY have links,
-- i.e. exactly the offices the DELETE can damage. `db:migrate` runs before
-- `db:seed:research-areas` and `elections:offices:seed` (see DB_DEPLOYMENT.md),
-- so on a fresh database research_areas is still incomplete; an unscoped
-- "every curated slug must exist" guard would abort the migration there. On a
-- fresh database no office has links yet, so this guard correctly checks
-- nothing and the seed layer produces the curated state afterwards. On an
-- already-seeded database (this migration's real target, and the state the seed
-- file's own curation pass runs in) every curated office has links, so coverage
-- is complete.
DO $$
DECLARE
    missing_slugs text;
BEGIN
    SELECT string_agg(DISTINCT bad.slug, ', ' ORDER BY bad.slug)
    INTO missing_slugs
    FROM (
        SELECT unnest(c.slugs) AS slug
        FROM curated_office_core_areas c
        JOIN public.offices o
          ON o.scope = c.scope AND o.canonical_name = c.canonical_name
        WHERE EXISTS (
            SELECT 1 FROM public.office_research_areas ora WHERE ora.office_id = o.id
        )
    ) bad
    WHERE NOT EXISTS (
        SELECT 1 FROM public.research_areas ra WHERE ra.slug = bad.slug
    );

    IF missing_slugs IS NOT NULL THEN
        RAISE EXCEPTION
            'Curated research-area slugs not found in public.research_areas: %. Refusing to reconcile: these links would be deleted and never restored.',
            missing_slugs;
    END IF;
END
$$;

-- One statement, one snapshot: the DELETE removes links outside each office's
-- curated set and the INSERT adds any missing curated links. The two operate on
-- disjoint rows (non-curated vs curated), so combining them in a single
-- data-modifying CTE is safe and keeps both driven by the same `targets`.
WITH targets AS (
    SELECT o.id AS office_id, c.slugs
    FROM public.offices o
    JOIN curated_office_core_areas c
      ON c.scope = o.scope AND c.canonical_name = o.canonical_name
),
deleted AS (
    DELETE FROM public.office_research_areas ora
    USING targets t, public.research_areas ra
    WHERE ora.office_id = t.office_id
      AND ra.id = ora.research_area_id
      AND NOT (ra.slug = ANY (t.slugs))
    RETURNING 1
)
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT t.office_id, ra.id
FROM targets t
JOIN public.research_areas ra
  ON ra.slug = ANY (t.slugs)
ON CONFLICT (office_id, research_area_id) DO NOTHING;

DROP TABLE curated_office_core_areas;

COMMIT;
