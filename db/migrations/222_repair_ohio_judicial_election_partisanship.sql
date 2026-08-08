-- Repair Ohio judicial contests that a policy defect forced to partisan.
--
-- `elections.is_partisan` is ballot-facing: it records whether the party is
-- printed next to the name in November. Ohio ran partisan primaries into
-- nonpartisan generals for over 160 years, and S.B. 80 / H.B. 149 (134th G.A.,
-- effective 2021-09-30) changed that for TWO courts only. ORC 3505.03 now
-- prints "the name of the political party by which the candidate was
-- nominated" under candidates for chief justice, justice of the supreme court,
-- and judge of a court of appeals. ORC 3505.04 leaves "judges of a municipal
-- court, county court, or court of common pleas" on the nonpartisan ballot,
-- which may carry no party designation at all — and the probate, juvenile, and
-- domestic relations courts are divisions of the court of common pleas
-- (ORC 2101.01), so they are nonpartisan too.
--
-- The old policy forced every Ohio judicial contest partisan. Rows written
-- under it are wrong on disk and will stay wrong: the policy fix ships
-- alongside this migration but only governs contests that get rewritten, and a
-- district holding an upcoming election classifies as `not_due`
-- (classifyDistrictElectionSearchEligibility), so ordinary discovery never
-- revisits it. Without this migration those contests reach election day
-- claiming a party the ballot does not print.
--
-- Scoped to elections on or after the statute's effective date. Ohio supreme
-- court contests BEFORE 2021-09-30 were genuinely nonpartisan on the ballot, so
-- rewriting them by today's rule would corrupt correct history.
--
-- Idempotent: each statement skips rows that already hold the computed value,
-- so a replay updates nothing.

BEGIN;

-- Clerks, prosecutors, and other officers name a court in their own title
-- without being judgeships; their partisanship follows the ordinary county
-- rule, so they are excluded here exactly as isJudicialOfficeTitle excludes
-- them. Ohio elects no judge by retention, but the exclusion mirrors policy.
WITH ohio_judicial AS (
  SELECT
    e.id,
    (e.official_ballot_title ~* '\y(supreme court|court of appeals?)\y') AS should_be_partisan
  FROM public.elections e
  JOIN public.districts d ON d.id = e.district_id
  WHERE d.state = 'OH'
    AND e.race_type = 'office'
    AND e.election_date >= DATE '2021-09-30'
    AND e.official_ballot_title ~* '\y(judge|justice|judicial|magistrate|supreme court|court of appeals?)\y'
    AND e.official_ballot_title !~* '\yclerks?\y'
    AND e.official_ballot_title !~* '\y(retention|retained)\y'
)
UPDATE public.elections e
SET is_partisan = ohio_judicial.should_be_partisan,
    updated_at = now()
FROM ohio_judicial
WHERE e.id = ohio_judicial.id
  AND e.is_partisan IS DISTINCT FROM ohio_judicial.should_be_partisan;

COMMIT;
