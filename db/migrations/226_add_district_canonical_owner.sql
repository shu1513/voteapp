-- Mark districts rows that are not governments.
--
-- A Census Designated Place is a statistical geography with no government of
-- any kind. Two of them are coextensive with a county-equivalent AND share its
-- name and population, which makes them a trap: `Arlington CDP, Virginia`
-- (5103000) looks exactly like `Arlington County, Virginia` (51013), and
-- `Yakutat CDP, Alaska` (0286490) like `Yakutat City and Borough` (02282).
-- Research claimed against the CDP row is wasted, and contests written there
-- would sit under a row no ballot reader should ever reach.
--
-- `canonical_district_id` is NULL for a real district (every row but these two)
-- and otherwise points at the row that actually holds the government. District
-- selection collapses onto the target and the research queue stops offering the
-- alias.
--
-- WHAT THIS DELIBERATELY DOES NOT DO
--
-- Census also gives some real governments two rows: a county-equivalent FIPS
-- and a place FIPS. Virginia's 38 independent cities are the largest group
-- (Richmond is county-equivalent 51760 and place 5167000, both population
-- 229,359), joined by consolidated city-counties (Denver, Philadelphia, San
-- Francisco, Baltimore city, St. Louis city, Nashville-Davidson, Louisville
-- Metro, ...). It is tempting to collapse those too, since one city has one
-- mayor and one council. Do not.
--
-- Offices are scoped, and county and place scopes do not overlap at all:
-- `Mayor` and `City Council Member` exist only at place scope, `Sheriff` and
-- `District Attorney` only at county scope, and OfficeMatcher.loadOffices
-- filters `WHERE scope = $1` taken straight from the payload's district_type.
-- Collapsing such a pair would make half of that one government's offices
-- permanently unmatchable.
--
-- The two rows are complementary, not duplicated, and the existing data shows
-- it: San Francisco's county row holds the Board of Supervisors, Assessor-
-- Recorder and Public Defender while its place row holds the city ballot
-- measures; Lexington-Fayette's county row holds Sheriff, Coroner and Property
-- Valuation Administrator while its place row holds the Mayor. Virginia is the
-- clearest case — under Art. VII Sec. 4 every independent city elects a
-- sheriff, Commonwealth's attorney, treasurer, commissioner of revenue and
-- circuit court clerk, all county-scoped offices, in November of ODD years.
-- Those 38 county rows are empty today because their next election is 2027,
-- not because they duplicate anything.
--
-- scripts/reportDistrictCanonicalPairs enumerates every such pair so the shape
-- stays visible without being suppressed.

ALTER TABLE public.districts
  ADD COLUMN IF NOT EXISTS canonical_district_id uuid REFERENCES public.districts(id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_districts_canonical_not_self'
  ) THEN
    ALTER TABLE public.districts
      ADD CONSTRAINT chk_districts_canonical_not_self
      CHECK (canonical_district_id IS NULL OR canonical_district_id <> id);
  END IF;
END $$;

COMMENT ON COLUMN public.districts.canonical_district_id IS
  'NULL = a real district. Non-NULL = this row is not a government (a CDP coextensive with, and named like, a real county-equivalent); the referenced row holds the government, its contests, and its research.';

CREATE INDEX IF NOT EXISTS idx_districts_canonical_district_id
  ON public.districts (canonical_district_id)
  WHERE canonical_district_id IS NOT NULL;

-- Keyed by (district_type, geoid_compact), never by id: district UUIDs are
-- generated per environment and differ between local and prod.
CREATE TEMP TABLE tmp_district_aliases (
  alias_geoid text NOT NULL,
  owner_geoid text NOT NULL
) ON COMMIT DROP;

INSERT INTO tmp_district_aliases (alias_geoid, owner_geoid) VALUES
  ('5103000', '51013'),  -- Arlington CDP -> Arlington County, Virginia
  ('0286490', '02282');  -- Yakutat CDP -> Yakutat City and Borough, Alaska

UPDATE public.districts AS alias
SET canonical_district_id = owner.id
FROM tmp_district_aliases AS pair
JOIN public.districts AS owner
  ON owner.district_type = 'county' AND owner.geoid_compact = pair.owner_geoid
WHERE alias.district_type = 'place'
  AND alias.geoid_compact = pair.alias_geoid
  AND alias.canonical_district_id IS DISTINCT FROM owner.id;

-- An alias must never itself be the target of another alias: district selection
-- resolves exactly one hop, so a chain would leave a suppressed row reachable.
DO $$
DECLARE
  chained int;
BEGIN
  SELECT count(*) INTO chained
  FROM public.districts AS a
  JOIN public.districts AS b ON b.id = a.canonical_district_id
  WHERE b.canonical_district_id IS NOT NULL;
  IF chained > 0 THEN
    RAISE EXCEPTION 'districts.canonical_district_id chain detected on % row(s)', chained;
  END IF;
END $$;

-- Contests written under a row this migration suppresses would become
-- unreachable. Both CDPs are empty; fail loudly rather than hide a ballot.
DO $$
DECLARE
  stranded int;
BEGIN
  SELECT count(*) INTO stranded
  FROM public.elections e
  JOIN public.districts d ON d.id = e.district_id
  WHERE d.canonical_district_id IS NOT NULL;
  IF stranded > 0 THEN
    RAISE EXCEPTION 'refusing to suppress districts holding % election(s)', stranded;
  END IF;
END $$;

-- Retire open research for suppressed rows. Only open requests are touched;
-- finished history stays intact.
UPDATE public.manual_district_research_requests AS r
SET status = 'skipped',
    finished_at = now(),
    summary = 'not a government (Census designated place); research belongs to the canonical district',
    updated_at = now()
FROM public.districts AS d
WHERE d.id = r.district_id
  AND d.canonical_district_id IS NOT NULL
  AND r.status IN ('queued', 'claimed', 'running');

-- Existing followers still point at the alias. Address resolution collapses new
-- lookups, but /api/me/ballot and the notification joins read user_districts
-- directly, so a stored alias link would show a district with no contests.
-- Repoint first (skipping anyone who already holds the owner, which is the
-- common case since a Census address resolves into both layers), then drop the
-- rows that are now redundant.
INSERT INTO public.user_districts (user_id, district_id, district_type)
SELECT ud.user_id, owner.id, owner.district_type
FROM public.user_districts AS ud
JOIN public.districts AS alias ON alias.id = ud.district_id
JOIN public.districts AS owner ON owner.id = alias.canonical_district_id
WHERE alias.canonical_district_id IS NOT NULL
ON CONFLICT DO NOTHING;

DELETE FROM public.user_districts AS ud
USING public.districts AS alias
WHERE alias.id = ud.district_id
  AND alias.canonical_district_id IS NOT NULL;
