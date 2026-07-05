# Research-Area Personalization Plan

Goal: the seven ranked "issues you care about" stop being write-only. They
drive election ordering, visible per-race relevance, per-candidate stance
summaries, record sorting, and (later) issue-based email — without changing
how areas are researched or stored.

## What already exists (do not rebuild)

| Piece | Where | State |
|---|---|---|
| Area catalog + `is_user_selectable` | `research_areas` | done |
| Office ↔ area links | `office_research_areas` | done, already surfaced as `ElectionSummary.research_areas` and rendered as chips on `ElectionCard` |
| Ballot-measure ↔ area + stance | `ballot_measure_research_area_tags` (`for`/`against`) | done in DB + measure detail payload; **not** in election summaries |
| Record ↔ area + stance | `candidate_record_area_tags` (`for`/`against`/`NULL` = general) | done, already in candidate detail payload (`records[].research_area_tags`) |
| User prefs with rank | `user_research_area_preferences.rank` 1–7, unique per user | done end-to-end: PUT validates ranks; only the UI never sets them |
| Sort machinery | `ballotElectionOrdering.ts` decorator (`vote_power`, `soonest`, `district_size[_smallest]`, `followed_first`) + per-user saved sort in `user_ballot_preferences` | done; designed for adding modes |

Consequences: "highlight my areas on each race" and "sort records by my
areas" are pure frontend; the rank editor is pure frontend; only the ballot
sort mode, candidate stance aggregates, and email need backend work.

## Phase A — backend: `my_areas` ballot sort + measure areas in summaries

1. Shared scoring module `userResearchAreaScoring.ts` (pipeline/users):
   loads a user's saved areas and exposes the weight formula
   `weight = 8 - rank` (rank 1 → 7 … rank 7 → 1), unranked-but-selected
   area → 1. One module because three sorts (ballot server-side, candidate
   and record client-side) must agree; the frontend mirrors the same
   constants in a small lib (documented mirror, like BALLOT_SORTS).
2. Include ballot-measure area tags in ballot election summaries (new
   `research_areas` entries or a parallel `measure_research_areas` field —
   decide during implementation; office races keep office areas). Without
   this, measure-heavy ballots can't match user areas.
3. New sort `my_areas` in `BALLOT_SUMMARY_SORTS` + the ordering decorator:
   - Score per election = sum of weights of the user's saved areas that
     intersect the election's areas (office + measure). Non-matching
     elections last.
   - Tiebreaks inside equal scores: best single matched rank, then
     `vote_power` (the current default sort).
   - Anonymous callers / users with no saved areas: `my_areas` degrades to
     `vote_power` (documented, not an error).
   - No per-election match arrays in the payload: the client already holds
     both the prefs and each election's areas, so highlighting is a
     client-side intersection. The server's only personalized job is order.
4. Default behavior: authenticated `/api/me/ballot` with ≥1 saved area and
   no explicitly saved sort preference defaults to `my_areas`; everyone else
   keeps `vote_power`. Explicit user choice (saved in
   `user_ballot_preferences`) always wins.
5. Tests mirror the existing ordering decorator tests.

## Phase B — backend: per-candidate stance aggregates in election detail

Extend election-detail `candidates[]` with:

```text
record_area_stances: [
  { research_area_id, slug, name, for_count, against_count }
]
```

- Aggregated from `candidate_record_area_tags` where `stance IS NOT NULL`
  (stance-bearing only, per the product intent; general/NULL tags excluded).
- One grouped query per election detail request, not per candidate.
- Deliberately user-independent: the detail endpoint stays cacheable across
  users and never needs a `userId`. Candidate sorting by "my areas" happens
  client-side (candidate lists are small; the aggregates + the user's prefs
  are both on the client), using the mirrored weight formula from Phase A.

## Phase C — frontend: consume it all

1. Ballot page: add "My issues" to the sort dropdown (default per Phase A
   rule); highlight chips for areas the user saved (e.g. filled accent chip
   vs. today's muted chip) on `ElectionCard` and election detail; measures'
   area chips get the same treatment.
2. Election detail candidate list: per-candidate area chips with stance
   counts (e.g. "Housing Affordability · 2 for" / "1 against"), saved areas
   highlighted; a client-side sort control with direction — "For my issues
   first" and "Against my issues first" (weighted score per Phase A formula;
   unique matched areas before record counts as tiebreak).
3. Candidate page records: sort control — "Newest first" (current/default)
   and "My issues first" (records tagged with the user's areas first, ranked
   order, then date).
4. All of it degrades cleanly for anonymous users (no highlight, no
   my-issues sort offered).

## Phase D — frontend: rank editor

Settings "Issues you care about" becomes a two-part control:
- Available areas (unselected): tap to add (existing toggle behavior).
- Selected areas (max 7): a reorderable list — drag-and-drop via
  `@dnd-kit/core` + `@dnd-kit/sortable` (the one new dependency; keyboard
  accessible) with up/down buttons as the no-drag fallback. Order = rank
  1..N; saved through the existing PUT (already validates ranks).

## Phase E — backend + frontend: issue email (design gate)

"Email users materials about their interested areas" needs a product
decision before code. Options, cheapest first:
1. Extend the existing candidate-follow digest with an "on your issues"
   section: new stance-bearing records in the user's areas from candidates
   on the user's ballot.
2. A separate weekly "your issues" email (digest-pattern sibling: sender +
   dedupe + scheduler + unsubscribe scope, like election reminders).
3. Curated/editorial content per area — out of scope until such content
   exists anywhere in the system.

Recommendation: option 1 first (reuses delivery, dedupe, and unsubscribe
infra; no new cadence), option 2 only if engagement warrants its own email.
Decide at phase start; do not build ahead.

## Order and rationale

A → B are backend (independently mergeable, both prerequisite-free), C
consumes both, D is standalone frontend (can land any time), E is gated on a
product decision. Per-PR: one phase, one branch, live E2E before push, same
as the reminder feature.
