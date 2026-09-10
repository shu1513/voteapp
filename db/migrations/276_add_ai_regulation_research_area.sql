-- Add the ai_regulation research area and link it to the offices whose formal
-- powers set or enforce rules for artificial-intelligence systems, following
-- migration 159's curation principle and migration 161's shape:
--   * Legislative/plenary: President, VP, U.S. Senator, U.S. Representative,
--     Governor, State Senator, State Lower Chamber Legislator (AI statutes —
--     deepfake bans, automated-decision rules, frontier-model transparency —
--     are written and signed here), plus Lieutenant Governor (broad
--     union-of-powers set per 159's rationale).
--   * Attorney General: the enforcement authority under every state AI act
--     passed so far (Colorado, Texas, Utah, California), and the office that
--     brings consumer-protection actions over AI products.
-- Deliberately NOT linked yet: mayors/city councils (a few cities regulate
-- automated hiring or facial recognition, but the shared office rows would
-- carry the area to every council in the country while the record base holds
-- no city-level AI votes — the filler 159 removed), judges (no AI docket),
-- Sheriff/District Attorney (deepfake and synthetic-image crimes are already
-- covered by public_safety_and_crime_control). Add local offices when their
-- records exist.
--
-- The seed layer (db/seeds/research_areas_v1.sql +
-- db/seeds/office_research_areas_v1.sql, including its curated reconcile
-- tail) is updated in the same change and remains authoritative for links;
-- this migration applies the identical state to already-seeded databases so
-- the area is usable without re-running seeds.

BEGIN;

INSERT INTO public.research_areas (slug, name, description)
VALUES (
  'ai_regulation',
  'AI Regulation',
  'Set and enforce safety, transparency, and accountability rules for AI systems, including automated decisions and deepfakes.'
)
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  updated_at = now();

-- Best-effort link copy for already-seeded databases. On a fresh
-- migrations-only database most of these offices do not exist yet (offices
-- are created by elections:offices:seed, which DB_DEPLOYMENT.md runs AFTER
-- db:migrate), so unresolved pairs are expected there and must not raise —
-- an exception would brick every fresh install before the seeds could run.
-- The seed layer produces the complete link set either way; the NOTICE keeps
-- partial resolution visible on live databases.
DO $$
DECLARE
  expected_pair_count integer;
  inserted_or_existing_count integer;
BEGIN
  CREATE TEMP TABLE desired_ai_regulation_offices (scope text, canonical_name text)
  ON COMMIT DROP;

  INSERT INTO desired_ai_regulation_offices (scope, canonical_name) VALUES
    ('presidential', 'President of the United States'),
    ('presidential', 'Vice President of the United States'),
    ('statewide', 'United States Senator'),
    ('us_house', 'United States Representative'),
    ('statewide', 'Governor'),
    ('statewide', 'Lieutenant Governor'),
    ('statewide', 'Attorney General'),
    ('state_upper', 'State Senator'),
    ('state_lower', 'State Lower Chamber Legislator');

  SELECT COUNT(*) INTO expected_pair_count FROM desired_ai_regulation_offices;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM desired_ai_regulation_offices desired
  JOIN public.offices office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name
  JOIN public.research_areas area
    ON area.slug = 'ai_regulation'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;

  SELECT COUNT(*)
  INTO inserted_or_existing_count
  FROM desired_ai_regulation_offices desired
  JOIN public.offices office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name;

  IF inserted_or_existing_count <> expected_pair_count THEN
    RAISE NOTICE
      'migration 276: % of % ai_regulation office links resolved; the rest are created by the seed layer (fresh install path)',
      inserted_or_existing_count,
      expected_pair_count;
  END IF;
END
$$;

COMMIT;
