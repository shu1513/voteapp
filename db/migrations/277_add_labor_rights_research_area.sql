-- Add the labor_rights research area and link it to the offices whose formal
-- powers set or enforce rules on wages, leave, workplace safety and
-- collective bargaining, following migration 159's curation principle and
-- migration 276's shape:
--   * Legislative/plenary: President, VP, U.S. Senator, U.S. Representative,
--     Governor, Lieutenant Governor, State Senator, State Lower Chamber
--     Legislator (minimum wage, paid leave, right-to-work and public-sector
--     bargaining statutes are written and signed here).
--   * Attorney General: brings wage-theft and misclassification actions in
--     many states.
--   * Labor Commissioner: the statewide office that enforces wage and hour law
--     where voters elect one.
-- Why the area exists: the roll-call campaign dropped labor measures in at
-- least eight states (NV, KS, IL, CO, OR, WV, CA, MI) because no area could
-- carry an honest direction for them, including Michigan's 2025 earned sick
-- time and minimum wage laws. A yes vote is "for" when a measure expands
-- worker pay, leave, safety or bargaining rights, and "against" when it
-- narrows them.
-- Deliberately NOT linked yet: local offices and judges, for the same reason
-- 276 gives — no local labor votes in the record base.
--
-- The seed layer (db/seeds/research_areas_v1.sql +
-- db/seeds/office_research_areas_v1.sql, including its curated reconcile
-- tail) is updated in the same change and remains authoritative for links;
-- this migration applies the identical state to already-seeded databases.

BEGIN;

INSERT INTO public.research_areas (slug, name, description)
VALUES (
  'labor_rights',
  'Labor Rights',
  'Protect workers through fair wages, paid leave, safe workplaces, and the right to organize and bargain collectively.'
)
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  updated_at = now();

-- Best-effort link copy for already-seeded databases; unresolved pairs are
-- expected on a fresh migrations-only database and must not raise (see 276).
DO $$
DECLARE
  expected_pair_count integer;
  inserted_or_existing_count integer;
BEGIN
  CREATE TEMP TABLE desired_labor_rights_offices (scope text, canonical_name text)
  ON COMMIT DROP;

  INSERT INTO desired_labor_rights_offices (scope, canonical_name) VALUES
    ('presidential', 'President of the United States'),
    ('presidential', 'Vice President of the United States'),
    ('statewide', 'United States Senator'),
    ('us_house', 'United States Representative'),
    ('statewide', 'Governor'),
    ('statewide', 'Lieutenant Governor'),
    ('statewide', 'Attorney General'),
    ('statewide', 'Labor Commissioner'),
    ('state_upper', 'State Senator'),
    ('state_lower', 'State Lower Chamber Legislator');

  SELECT COUNT(*) INTO expected_pair_count FROM desired_labor_rights_offices;

  INSERT INTO public.office_research_areas (office_id, research_area_id)
  SELECT office.id, area.id
  FROM desired_labor_rights_offices desired
  JOIN public.offices office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name
  JOIN public.research_areas area
    ON area.slug = 'labor_rights'
  ON CONFLICT (office_id, research_area_id) DO NOTHING;

  SELECT COUNT(*)
  INTO inserted_or_existing_count
  FROM desired_labor_rights_offices desired
  JOIN public.offices office
    ON office.scope = desired.scope
   AND office.canonical_name = desired.canonical_name;

  IF inserted_or_existing_count <> expected_pair_count THEN
    RAISE NOTICE
      'migration 277: % of % labor_rights office links resolved; the rest are created by the seed layer (fresh install path)',
      inserted_or_existing_count,
      expected_pair_count;
  END IF;
END
$$;

COMMIT;
