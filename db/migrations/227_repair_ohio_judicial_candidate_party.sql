-- Repair Ohio judicial candidates still holding a primary-nomination party.
--
-- Companion to 222_repair_ohio_judicial_election_partisanship.sql. That
-- migration corrected the CONTESTS: Ohio prints a party only for the supreme
-- court and the courts of appeals (ORC 3505.03), while judges of a municipal
-- court, county court, or court of common pleas — and the probate, juvenile,
-- and domestic relations divisions of that court (ORC 2101.01) — appear on the
-- nonpartisan ballot, which "[n]o name or designation of any political party"
-- may reach (ORC 3505.04).
--
-- It did not correct the CANDIDATES. Those rows were written while the old
-- policy forced every Ohio judicial contest partisan, so they still carry the
-- party that nominated the candidate in the May primary, and the reader shows
-- a party the November ballot does not print: the election page does not
-- consult is_partisan when deciding to render the party filter — it keys on
-- how many party buckets the roster spans — so a stale label is user-visible,
-- not merely stored.
--
-- Scope note, so the next reader does not over-trust this migration: it fixes
-- the STORED and DISPLAYED party only. It does NOT unblock re-running the
-- roster fan-out for these contests. assertCandidatePartyWillNotBeDiscarded
-- inspects the incoming payload (rosterParty / profile.party), not
-- candidates.party, and fanoutManualCandidateRoster sources rosterParty from
-- the staging row, whose stored roster payload still carries D/R. That hard
-- stop is deliberate and is left standing: the remedy the error names is to
-- re-stage the roster without party, which is a research action, not
-- something a data migration should forge on the researcher's behalf.
--
-- 'Nonpartisan' is not an invented sentinel. resolveStoredCandidateParty is
-- the single choke point every candidates.party write flows through, and it
-- stores exactly canonicalizeParty('Nonpartisan') = 'Nonpartisan' whenever
-- includeParty is false. This migration therefore writes what the pipeline
-- itself would write on the next rewrite — it removes drift rather than
-- introducing a value the code could not produce.
--
-- The nomination party is not lost, and that is deliberate. The original
-- roster payloads survive in staging_items under ingest_key
-- 'candidate_roster:<election_id>' (61 such rows for these contests when this
-- was written), so the D/R that the primary assigned stays recoverable for
-- anyone who later wants to model nomination separately from ballot
-- presentation. Scrubbing those payloads too would make the fan-out replay
-- cleanly at the cost of destroying the only record of how these judges were
-- nominated, which is a worse trade than leaving the replay blocked.
--
-- candidates.party is NOT NULL, and party lives on the CANDIDATE rather than
-- the candidacy — one row serves every election a person appears in. So the
-- second guard below is load-bearing rather than defensive bookkeeping: a
-- candidate who also appears in any contest that is partisan (or whose
-- partisanship is still unknown, hence IS DISTINCT FROM false) keeps the
-- stored party untouched, because overwriting it would corrupt the other
-- contest. No candidate matched that exclusion locally, but the backfill
-- campaign is actively writing Ohio rosters, so the guard has to hold for rows
-- that land after this is written.
--
-- Scoped to Ohio and to judicial titles, mirroring 222's predicate exactly
-- (clerks name a court without being judges; Ohio elects no judge by
-- retention, but the exclusion mirrors policy). Idempotent: rows already
-- reading 'Nonpartisan' are skipped, so a replay updates nothing.

BEGIN;

WITH ohio_nonpartisan_judicial_candidates AS (
  SELECT c.id
  FROM public.candidates c
  WHERE EXISTS (
      SELECT 1
      FROM public.candidate_elections ce
      JOIN public.elections e ON e.id = ce.election_id
      JOIN public.districts d ON d.id = e.district_id
      WHERE ce.candidate_id = c.id
        AND d.state = 'OH'
        AND e.race_type = 'office'
        AND e.is_partisan = false
        AND e.official_ballot_title ~* '\y(judge|justice|judicial|magistrate|supreme court|court of appeals?)\y'
        AND e.official_ballot_title !~* '\yclerks?\y'
        AND e.official_ballot_title !~* '\y(retention|retained)\y'
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.candidate_elections ce2
      JOIN public.elections e2 ON e2.id = ce2.election_id
      WHERE ce2.candidate_id = c.id
        AND e2.is_partisan IS DISTINCT FROM false
    )
)
UPDATE public.candidates c
SET party = 'Nonpartisan'
FROM ohio_nonpartisan_judicial_candidates t
WHERE c.id = t.id
  AND c.party <> 'Nonpartisan';

COMMIT;
