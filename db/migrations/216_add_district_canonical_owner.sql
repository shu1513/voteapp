-- One government, one districts row.
--
-- Census gives some governments two rows in `districts`: a county-equivalent
-- FIPS and a place FIPS. Virginia's 38 independent cities are the largest
-- group (Richmond is 51760 as a county-equivalent and 5167000 as a place, both
-- population 229,359), but the shape repeats for consolidated city-counties
-- (Denver, Philadelphia, Nashville-Davidson, Louisville Metro, ...). Richmond
-- has one mayor, one council, one school board; writing its contests under
-- both rows duplicates every election and candidate, and the research queue
-- offers the same city as two separate jobs.
--
-- `canonical_district_id` names the row that owns the government's contests.
-- NULL means "this row is canonical" (the normal case for the other ~50k
-- districts). A non-NULL value marks a suppressed alias and points at the row
-- that owns the work: district selection collapses onto the target, and the
-- manual research queue stops offering the alias.
--
-- Pairs are keyed by (district_type, geoid_compact), never by id: district
-- UUIDs are generated per environment and differ between local and prod.

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
  'NULL = canonical row. Non-NULL = this Census row duplicates another row''s government; the target owns all contests and all research work.';

CREATE INDEX IF NOT EXISTS idx_districts_canonical_district_id
  ON public.districts (canonical_district_id)
  WHERE canonical_district_id IS NOT NULL;

-- Seed set. `winner` is the side that owns contests:
--   * 'balance'      -> county wins. The place row is the consolidated
--                       government's "(balance)" area, which excludes satellite
--                       cities whose residents still vote in the metro-wide
--                       races. Only the county row covers the full electorate.
--   * CDP place rows -> county wins. A Census Designated Place has no
--                       government at all (Arlington CDP, Yakutat CDP).
--   * contests already written on exactly one side -> that side wins, so this
--                       migration never has to move existing rows.
--   * otherwise      -> place wins, matching the 12 Virginia cities already
--                       written place-side and the municipal character of the
--                       offices involved.
--
-- 'unresolved' pairs already have contests on BOTH sides (San Francisco,
-- Lexington-Fayette, Louisville Metro, New Orleans). Collapsing them means
-- moving elections between districts, which is a guarded data change and not
-- something a schema migration should do silently. They are listed here for
-- the record and deliberately left unmarked; scripts/reportDistrictCanonicalPairs
-- keeps reporting them until someone decides.
CREATE TEMP TABLE tmp_district_canonical_pairs (
  state text NOT NULL,
  county_geoid text NOT NULL,
  place_geoid text NOT NULL,
  winner text NOT NULL,
  shape text NOT NULL
) ON COMMIT DROP;

INSERT INTO tmp_district_canonical_pairs (state, county_geoid, place_geoid, winner, shape) VALUES
  ('AK', '02020', '0203000', 'place', 'coextensive'),
  ('AK', '02110', '0236400', 'place', 'coextensive'),
  ('AK', '02220', '0270540', 'place', 'coextensive'),
  ('AK', '02275', '0286380', 'place', 'coextensive'),
  ('AK', '02282', '0286490', 'county', 'coextensive'),
  ('CA', '06075', '0667000', 'unresolved', 'coextensive'),
  ('CO', '08014', '0809280', 'place', 'coextensive'),
  ('CO', '08031', '0820000', 'place', 'coextensive'),
  ('DC', '11001', '1150000', 'place', 'coextensive'),
  ('FL', '12031', '1235000', 'county', 'balance'),
  ('GA', '13021', '1349008', 'place', 'coextensive'),
  ('GA', '13053', '1321017', 'place', 'coextensive'),
  ('GA', '13059', '1303440', 'county', 'balance'),
  ('GA', '13101', '1326156', 'place', 'coextensive'),
  ('GA', '13215', '1319000', 'place', 'coextensive'),
  ('GA', '13239', '1332528', 'place', 'coextensive'),
  ('GA', '13245', '1304204', 'county', 'balance'),
  ('GA', '13307', '1381128', 'place', 'coextensive'),
  ('IN', '18097', '1836003', 'county', 'balance'),
  ('KS', '20071', '2028412', 'county', 'balance'),
  ('KY', '21067', '2146027', 'unresolved', 'coextensive'),
  ('KY', '21111', '2148006', 'unresolved', 'balance'),
  ('LA', '22071', '2255000', 'unresolved', 'coextensive'),
  ('MD', '24510', '2404000', 'county', 'coextensive'),
  ('MO', '29510', '2965000', 'county', 'coextensive'),
  ('MT', '30023', '3001675', 'place', 'coextensive'),
  ('MT', '30093', '3011397', 'county', 'balance'),
  ('NV', '32510', '3209700', 'place', 'coextensive'),
  ('PA', '42101', '4260000', 'place', 'coextensive'),
  ('TN', '47037', '4752006', 'county', 'balance'),
  ('TN', '47127', '4744382', 'place', 'coextensive'),
  ('TN', '47169', '4732742', 'place', 'coextensive'),
  ('VA', '51013', '5103000', 'county', 'coextensive'),
  ('VA', '51510', '5101000', 'place', 'coextensive'),
  ('VA', '51520', '5109816', 'place', 'coextensive'),
  ('VA', '51530', '5111032', 'place', 'coextensive'),
  ('VA', '51540', '5114968', 'place', 'coextensive'),
  ('VA', '51550', '5116000', 'place', 'coextensive'),
  ('VA', '51570', '5118448', 'place', 'coextensive'),
  ('VA', '51580', '5119728', 'place', 'coextensive'),
  ('VA', '51590', '5121344', 'place', 'coextensive'),
  ('VA', '51595', '5125808', 'place', 'coextensive'),
  ('VA', '51600', '5126496', 'place', 'coextensive'),
  ('VA', '51610', '5127200', 'place', 'coextensive'),
  ('VA', '51620', '5129600', 'place', 'coextensive'),
  ('VA', '51630', '5129744', 'place', 'coextensive'),
  ('VA', '51640', '5130208', 'place', 'coextensive'),
  ('VA', '51650', '5135000', 'place', 'coextensive'),
  ('VA', '51660', '5135624', 'place', 'coextensive'),
  ('VA', '51670', '5138424', 'place', 'coextensive'),
  ('VA', '51678', '5145512', 'place', 'coextensive'),
  ('VA', '51680', '5147672', 'place', 'coextensive'),
  ('VA', '51683', '5148952', 'place', 'coextensive'),
  ('VA', '51685', '5148968', 'place', 'coextensive'),
  ('VA', '51690', '5149784', 'place', 'coextensive'),
  ('VA', '51700', '5156000', 'place', 'coextensive'),
  ('VA', '51710', '5157000', 'place', 'coextensive'),
  ('VA', '51720', '5157688', 'place', 'coextensive'),
  ('VA', '51730', '5161832', 'place', 'coextensive'),
  ('VA', '51735', '5163768', 'place', 'coextensive'),
  ('VA', '51740', '5164000', 'place', 'coextensive'),
  ('VA', '51750', '5165392', 'place', 'coextensive'),
  ('VA', '51760', '5167000', 'place', 'coextensive'),
  ('VA', '51770', '5168000', 'place', 'coextensive'),
  ('VA', '51775', '5170000', 'place', 'coextensive'),
  ('VA', '51790', '5175216', 'place', 'coextensive'),
  ('VA', '51800', '5176432', 'place', 'coextensive'),
  ('VA', '51810', '5182000', 'place', 'coextensive'),
  ('VA', '51820', '5183680', 'place', 'coextensive'),
  ('VA', '51830', '5186160', 'place', 'coextensive'),
  ('VA', '51840', '5186720', 'place', 'coextensive');
UPDATE public.districts AS alias
SET canonical_district_id = owner.id
FROM tmp_district_canonical_pairs AS pair
JOIN public.districts AS owner
  ON owner.district_type = pair.winner
 AND owner.geoid_compact = CASE WHEN pair.winner = 'county' THEN pair.county_geoid ELSE pair.place_geoid END
WHERE pair.winner <> 'unresolved'
  AND alias.district_type = CASE WHEN pair.winner = 'county' THEN 'place' ELSE 'county' END
  AND alias.geoid_compact = CASE WHEN pair.winner = 'county' THEN pair.place_geoid ELSE pair.county_geoid END
  AND alias.canonical_district_id IS DISTINCT FROM owner.id;

-- An alias must never itself be the target of another alias: district
-- selection resolves exactly one hop, so a chain would leave a suppressed row
-- reachable.
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

-- Contests already written under a row this migration just suppressed would
-- become unreachable. The seed rule picks the populated side precisely so this
-- cannot happen; fail loudly rather than hide a ballot.
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

-- Retire queued research for suppressed rows. Only open requests are touched;
-- finished history stays intact.
UPDATE public.manual_district_research_requests AS r
SET status = 'skipped',
    finished_at = now(),
    summary = 'duplicate Census row; research belongs to the canonical district',
    updated_at = now()
FROM public.districts AS d
WHERE d.id = r.district_id
  AND d.canonical_district_id IS NOT NULL
  AND r.status IN ('queued', 'claimed', 'running');
