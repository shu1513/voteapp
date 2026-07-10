-- Add the gun_control research area and link it to the offices whose formal
-- powers move firearm policy, following migration 159's curation principle:
--   * Legislative/plenary: President, VP, U.S. Senator, U.S. Representative,
--     Governor, State Senator, State Lower Chamber Legislator (gun statutes
--     are written and signed here), plus Lieutenant Governor (broad
--     union-of-powers set per 159's rationale).
--   * Attorney General: enforces state firearm laws and runs permit/
--     background-check systems in several states.
--   * Sheriff: concealed-carry permitting and enforcement discretion are the
--     sheriff's call in most permit states.
--   * District Attorney: charging discretion on firearms offenses (same
--     logic as 159 adding womens_reproductive_rights to DAs).
--   * State Level Judge: state high courts decide state-constitutional and
--     post-Bruen firearm cases (same logic as their election/abortion areas).
-- Deliberately NOT linked: mayors/city councils (state preemption bars local
-- firearm regulation in most states), school boards, county/place judges
-- (firearm sentencing is already covered by public_safety_and_crime_control).
--
-- The seed layer (db/seeds/research_areas_v1.sql +
-- db/seeds/office_research_areas_v1.sql, including its curated reconcile
-- tail) is updated in the same change; this migration applies the identical
-- state to already-migrated databases.

BEGIN;

INSERT INTO public.research_areas (slug, name, description)
VALUES (
  'gun_control',
  'Gun Control',
  'Regulate firearm access through background checks, licensing, and safe-storage requirements to reduce gun violence.'
)
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  updated_at = now();

DO $$
DECLARE
  expected_pair_count integer;
  resolved_pair_count integer;
BEGIN
  WITH desired(scope, canonical_name) AS (
    VALUES
      ('presidential', 'President of the United States'),
      ('presidential', 'Vice President of the United States'),
      ('statewide', 'United States Senator'),
      ('us_house', 'United States Representative'),
      ('statewide', 'Governor'),
      ('statewide', 'Lieutenant Governor'),
      ('statewide', 'Attorney General'),
      ('statewide', 'State Level Judge'),
      ('state_upper', 'State Senator'),
      ('state_lower', 'State Lower Chamber Legislator'),
      ('county', 'Sheriff'),
      ('county', 'District Attorney')
  )
  SELECT COUNT(*)
  INTO expected_pair_count
  FROM desired;

  -- Fail fast if any office row or the research area is missing: a silent
  -- partial link set would surface later as inexplicable labeling gaps.
  WITH desired(scope, canonical_name) AS (
    VALUES
      ('presidential', 'President of the United States'),
      ('presidential', 'Vice President of the United States'),
      ('statewide', 'United States Senator'),
      ('us_house', 'United States Representative'),
      ('statewide', 'Governor'),
      ('statewide', 'Lieutenant Governor'),
      ('statewide', 'Attorney General'),
      ('statewide', 'State Level Judge'),
      ('state_upper', 'State Senator'),
      ('state_lower', 'State Lower Chamber Legislator'),
      ('county', 'Sheriff'),
      ('county', 'District Attorney')
  )
  SELECT COUNT(*)
  INTO resolved_pair_count
  FROM desired
  JOIN public.offices office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name
  JOIN public.research_areas area
    ON area.slug = 'gun_control';

  IF resolved_pair_count <> expected_pair_count THEN
    RAISE EXCEPTION
      'Expected % office/gun_control pairs to resolve, found %',
      expected_pair_count,
      resolved_pair_count;
  END IF;

  WITH desired(scope, canonical_name) AS (
    VALUES
      ('presidential', 'President of the United States'),
      ('presidential', 'Vice President of the United States'),
      ('statewide', 'United States Senator'),
      ('us_house', 'United States Representative'),
      ('statewide', 'Governor'),
      ('statewide', 'Lieutenant Governor'),
      ('statewide', 'Attorney General'),
      ('statewide', 'State Level Judge'),
      ('state_upper', 'State Senator'),
      ('state_lower', 'State Lower Chamber Legislator'),
      ('county', 'Sheriff'),
      ('county', 'District Attorney')
  )
  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM desired
  JOIN public.offices office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name
  JOIN public.research_areas area
    ON area.slug = 'gun_control'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;
END
$$;

COMMIT;
