# Plan: party canonicalization + candidate/election filters

Status: phase 0 shipped (PR #468) + local backfill applied; phase 1
implemented; phases 2–3 not started. Each phase is
one PR; later phases depend on earlier ones only where noted. Verified facts
below come from the live local DB and the current code — re-verify counts
before the backfill run.

Phase 0 findings from the dry runs (local, 2026-08-01): 63 distinct
variants, 15 change (188 rows). Exact-match table only — generic affix rules
were rejected because they collapse real parties ("Tea Party" → "Tea",
"Independent Party" → "Independent"). Review then removed five more mappings
the first draft had: Alaska's "Registered Republican/Democrat/Libertarian"
labels (83 rows, all AK — a top-four-ballot registration disclosure, not an
affiliation claim) and the minor-party full names "Constitution Party" (ID)
and "Independent American Party" (NV, distinct from UT's "Independent
American" rows) — for minors the "Party" suffix is part of the name. Four
parenthesis-mangled variants (5 rows) are flagged for manual repair, not
auto-changed.

## Background / verified facts

- `candidates.party` is a free string with 20+ spelling variants in the DB:
  "Democratic" (4,036), "Republican" (4,135), "Democrat" (42), "DEM" (24),
  "REP" (26), "Registered Democrat" (25), "Registered Republican" (57),
  "Democratic Party" (20), "Republican Party" (19), "DEMOCRATIC" (12),
  "Democratic-Farmer-Labor" (32), "Nonpartisan" (1,771), "Independent" (158),
  "Libertarian" (52) / "Libertarian Party" (24) / "LIB" (9),
  "Unaffiliated" (49), "Green" (14), "Unenrolled" (11),
  "No Party Affiliation" (6), plus a long tail.
- `elections.is_partisan` is tri-state: 5,633 true / 1,146 false / 30 null.
- `researchAreaScoring.ts` (`@voteapp/api-client`) already scores candidates
  against saved research areas for the "My issues first" sort
  (`scoreCandidateRecords`: weighted matched areas, record volume tiebreak).
  Used by `ElectionPage.tsx` and `CandidatePage.tsx`.
- Ballot `ElectionSummary` already carries `research_areas` client-side, and
  `ElectionCard` already receives `savedAreaWeights` — an elections filter
  needs no API change.
- `my_areas` ballot sort exists server-side (`ballotElectionOrdering.ts`) and
  is the default for signed-in users with saved areas; the public endpoint
  excludes it (`PUBLIC_BALLOT_SORTS`).

Consequence: phases 1–3 are all client-side; only phase 0 touches the
backend.

## Phase 0 — party canonicalization at write time (backend + backfill)

Goal: one spelling per party in storage. The set of parties stays open —
canonicalize spelling, never reject an unknown party, never call AI.

1. `canonicalizeParty(raw: string): string` — deterministic, pure
   (implemented in
   `backend/src/pipeline/candidates/candidatePartyCanonicalization.ts`):
   - trim, collapse internal whitespace
   - EXACT-MATCH lookup table only (case-insensitive), seeded from the full
     observed variant list: DEM/Democrat/Democratic Party → `Democratic`;
     REP/Republican Party → `Republican`; LIB/Libertarian Party →
     `Libertarian`; GRE → `Green`; IND → `Independent`; plus canonical
     self-entries so casing variants ("DEMOCRATIC") land on canonical casing
   - NO generic affix rules ("Registered X" → X, "X Party" → X): they
     collapse real distinctions — "Tea Party" ≠ "Tea", "Independent Party"
     (a registered party in OR/CT) ≠ "Independent", Alaska's "Registered
     Republican" is a registration disclosure not an affiliation, and minor
     parties' "Party" suffix is part of their official name. The major-party
     suffix mappings above are safe only because the adjective form is the
     universal convention for those specific parties
   - unknown values pass through trimmed and whitespace-collapsed but
     otherwise untouched — no title-casing, no rejection, the party set
     stays open
   - `Democratic-Farmer-Labor` is NOT collapsed — a real, distinct legal
     party name; display keeps it, the filter bucket (phase 1) maps it
   - Wired inside `resolveStoredCandidateParty`
     (`backend/src/pipeline/candidates/candidateProfileIdentity.ts`) — the
     single choke point every `candidates.party` write flows through
     (insert and merge alike), so previews stay consistent with storage.
     Verified: the backend has no equality comparisons against
     `candidates.party`, so no read path changes behavior.
2. Unit tests: every known variant maps; the affix traps and Alaska
   registration labels stay untouched; unknown passes through; idempotent
   (`canonicalizeParty(canonicalizeParty(x)) === canonicalizeParty(x)`).
3. Backfill script (`backend/src/scripts/`): repair existing rows with the
   same function; print a before/after variant count table; dry-run flag.
   Run locally after merge, on prod after deploy (same pattern as the
   records-routing repair).
4. Skill docs (done): `voteapp-manual-research` → `rosters.md` stage
   checklist now says to emit canonical party names and to keep distinct
   real parties (and AK registration labels) distinct; the write choke
   point canonicalizes known variants regardless.

## Phase 1 — party filter on the candidates list (ElectionPage + mobile)

1. `partyBucket(party: string | null): "democratic" | "republican" | "other"`
   in `@voteapp/api-client` (shared web + mobile). Built on canonical names;
   affiliates and preserved storage labels map explicitly
   (`Democratic-Farmer-Labor` → democratic, `Registered Democrat` →
   democratic, `Registered Republican` → republican — bucketing is filter
   relevance, so registration labels bucket with their party even though
   storage keeps them distinct).
   Nonpartisan/Independent/Unaffiliated/Unenrolled/unknown → other.
2. Filter UI on `ElectionPage.tsx` candidates list: All / Democrats /
   Republicans / Other, each chip with a count. "All" default. Pure
   client-side filter over the already-loaded roster.
3. Visibility rule is data-driven, not flag-driven: render the filter only
   when the roster spans ≥ 2 buckets. This handles non-partisan races
   (single bucket → no filter), `is_partisan = null` races, and one-party
   rosters for free. `is_partisan` is not consulted.
4. Mobile port (`mobile/`): same component logic on the election screen.
5. Tests: bucket fn unit tests; ElectionPage tests — filter hidden on
   single-bucket roster, counts correct, filtering works, All restores.

## Phase 2 — "Has a record on my issues" filter (ElectionPage + mobile)

Signed-in users with saved research areas only (anonymous users have no
saved areas; the control simply doesn't render).

1. Filter = `scoreCandidateRecords(...).matchedWeight > 0` — reuses the
   exact scoring the "My issues first" sort already uses. No new scoring.
2. Label: "Has a record on my issues" — direction-neutral (relevance, not
   agreement), consistent with the sort's framing.
3. Mitigation for the coverage caveat (no records ≠ no stances; rosters are
   unevenly researched): always show "N candidates hidden" with a one-tap
   clear. The filter must never look like the full roster.
4. Composes with phase 1 (party filter AND records filter).
5. Tests: hidden-count correctness, interaction with party filter, control
   absent when signed out / no saved areas.

## Phase 3 — "Only my issues" toggle on the ballot page (web + mobile)

1. Client-side toggle on `BallotPage` (signed-in ballot; public page never
   shows it — no saved areas). Keep = elections whose `research_areas`
   intersect the user's saved area ids (weight map already on the client).
2. Same mitigation: "N races hidden" count always visible while active;
   filtered-out races still elect real officials, so the toggle is opt-in
   and off by default. State in the URL query (like `sort`) so it survives
   navigation.
3. Interaction with ordering: filter composes with any sort; with `my_areas`
   sort it truncates the zero-score tail the sort already sank.
4. Tests: intersection logic, hidden count, off-by-default, URL round-trip.

## Sequencing / PR slicing

- PR A: phase 0 (canonicalizer + contract + tests + backfill script)
- local repair run, then prod repair after deploy
- PR B: phase 1 (bucket fn + web UI + mobile)
- PR C: phase 2
- PR D: phase 3

Phase 1 technically works without phase 0 (bucket fn absorbs spelling mess)
but display stays ugly ("DEM" next to "Democratic"), so phase 0 ships first.

## Open questions

- Party affiliate → bucket list: DFL → Democratic is confirmed; any others
  worth seeding now (e.g. Working Families, Conservative Party of NY fusion
  labels) or add as encountered?
- Phase 2/3 hidden-count copy: exact wording TBD at build time.
- Prod backfill timing: needs a deploy first; production DB currently
  scheduled for deletion Aug 9 unless upgraded — confirm DB survival before
  scheduling the prod repair.
