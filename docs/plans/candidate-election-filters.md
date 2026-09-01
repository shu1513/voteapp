# Plan: party canonicalization + candidate/election filters

Status: phase 0 shipped (PR #468) + local backfill applied; phase 1 shipped
(PR #473); phase 2 shipped (PR #474); phase 3 shipped (PR #476); phase 4
implemented. Each phase is
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

> **Removed from the web ElectionPage 2026-09-01.** Three chip rows stacked
> under "Candidates" read as clutter, and "Auto-pick by my issues" already
> answers "who matches my issues". The auto-pick button now sits below the
> party chips. Mobile still has the chip; drop it there for parity.

Signed-in users with saved research areas only (anonymous users have no
saved areas; the control simply doesn't render).

1. Filter = `scoreStanceRelevance(aggregateRecordAreaStances(records),
   weights).score > 0` — reuses the exact scoring the "My issues first" sort
   already uses. No new scoring, no api-client change.
2. Label: "Has a record on my issues" — direction-neutral (relevance, not
   agreement), consistent with the sort's framing.
3. Mitigation for the coverage caveat (no records ≠ no stances; rosters are
   unevenly researched): always show "N candidates hidden" with a one-tap
   "Show all" while active. The filter must never look like the full roster.
4. Composes with phase 1: applied after the party filter, and the hidden
   count is relative to the party-filtered view (the party chips already
   account for their own hiding). The toggle is a per-race choice — keyed to
   the election id like the party pick, so it never travels to another race,
   but it persists across party switches within the race.
5. Visibility is data-driven like the party filter while the toggle is OFF:
   it renders only when the viewer has saved areas AND the party-filtered
   set splits into matched + unmatched (all-match would be a no-op;
   none-match would empty the list). While ON it stays visible and keeps
   applying — even when a party switch leaves zero matches and the view
   goes empty ("N hidden · Show all" explains it) — because an active
   filter that silently stops applying would show a full roster the viewer
   believes is filtered. Only a viewer with no saved areas gets the pick
   ignored, same as the sort.
6. Tests: hidden-count correctness, interaction with party filter (including
   a switch into a no-match party), reset on navigation, control absent when
   signed out / no saved areas / no split.

## Phase 3 — "Only my issues" toggle on the ballot page (web + mobile)

1. Client-side toggle on all four ballot surfaces — web `BallotPage` +
   `SavedBallotPage`, mobile `ballot.tsx` + `my-ballot.tsx`. Keep =
   elections whose `research_areas` intersect the viewer's saved area ids
   (`savedAreaIds` already on the client). The gate is `hasSaved`, not the
   page: anonymous viewers never see it, and a verified visitor's one-off
   public search is filterable too — same rule as the saved-area chip
   personalization those pages already share.
2. Same mitigation: "N elections hidden · Show all" always visible while
   the filter hides any race; filtered-out races still elect real
   officials, so the toggle is opt-in and off by default. Web state is the
   URL query (`issues=mine`, like `sort`) so it survives navigating into an
   election and back — deliberately NOT an account preference, so hiding
   races never silently persists across visits. Mobile state is local
   (screens stay mounted under a stack push, so it survives back-nav the
   same way).
3. Visibility mirrors phase 2: while OFF, render only when the viewer has
   saved areas AND the list splits into matched + unmatched; while ON it
   stays visible and keeps applying even when that empties the view. A
   viewer with no saved areas gets the request ignored (covers a shared
   `?issues=mine` link opened anonymously). While the saved areas are still
   LOADING on a `?issues=mine` web load, the pages withhold the list
   (useMyResearchAreas().isLoading) instead of flashing the full ballot
   before the filter engages; the flag settles on fetch failure too, falling
   open to the full list — a ballot app errs toward showing races, and no
   on-page element claims filtering in that state.
4. Interaction with ordering: filter composes with any sort; with `my_areas`
   sort it truncates the zero-score tail the sort already sank.
5. Tests: hidden count + Show all restore, URL round-trip, arrives filtered
   from a shared URL, active filter honestly empties the view, control
   absent when signed out / no saved areas / no split.

## Phase 4 — vote-impact filter + unified "Filters" control (ballot pages)

Two changes in one PR because the second exists to absorb the first: a new
vote-impact threshold filter, and a single "Filters" disclosure that groups it
with "Affects my issues" and (on the saved page) "Followed candidates first" so
the controls row stops accreting loose chips.

1. **Impact filter.** A minimum-label threshold, two options: "High or
   above" (`high` + `very_high`) and "Normal or above" (`medium` +
   `high` + `very_high`) — checkbox labels reuse the card vocabulary from
   `formatVotePowerLabel` ("Normal", never "medium"; the wire/URL word
   stays `medium`). The thresholds nest, so exactly one can be engaged:
   checking one swaps the other off, unchecking means any impact (no
   explicit "Any" option — unchecked already says it). Labels only, never
   raw `score` — the label thresholds are backend-authored (`votePower.ts`)
   and already the published grading. `unknown` matches neither threshold
   but is included in the hidden count: the filter claims a minimum impact,
   unknown is not known to meet it, and the count line explains the
   disappearance. Client-side only; `ElectionSummary.vote_power` is already
   on every surface, anonymous included — so unlike phase 3 there is no
   auth gate, no loading withhold, and no fail-open branch.
2. **Offer gate = long ballots.** While OFF, an impact option renders only
   when the ballot has MORE THAN 7 elections (`LONG_BALLOT_THRESHOLD = 7`
   in the shared derivation) AND that option's threshold actually splits
   the list (all-match is a no-op; none-match would empty the ballot
   unexplained). "Normal or above" is additionally withheld when it would
   keep the same set as "High or above" (a ballot with no Normal races) —
   never two checkboxes doing the same thing. The gate covers the *offer*
   only: once engaged — e.g. arriving via a shared `?impact=high` URL onto
   a short ballot — the engaged option stays visible and keeps applying
   regardless, per the phase-2/3 rule that an engaged control never
   vanishes. Unlike `issues=mine` with no saved areas (data missing →
   request ignored), a short ballot never invalidates the impact request —
   the data is present, so it applies.
3. **Unified "Filters" control.** One chip-styled disclosure button on all
   four ballot surfaces, `aria-expanded` semantics, active-filter count
   badge ("Filters · 2" — counts filters only, never ordering). Contents in
   two labeled sections, because the two halves persist differently and the
   grouping must not blur that:
   - **Show**: "Affects my issues" (phase-3 gates unchanged; the label
     reuses the cards' "Affects:" verb — the original "Only my issues"
     read as filtering issues, not elections) and the "Vote impact"
     threshold checkboxes. URL/local state, session-scoped, never
     persisted. All checkboxes, matching the Order section — no chips.
   - **Order** (saved page only): the "Followed candidates first" checkbox
     moves in from `BallotPreferenceControls`, keeping its persisted
     full-object PUT semantics, optimistic overlay, and cross-mount saving
     lock exactly as they are.
   The sort select stays OUTSIDE the disclosure on both pages: it is the
   most-used control and it is ordering, not filtering. The chip renders
   only when it has something to offer (any filter offerable-or-engaged, or
   the Order section present) — an anonymous visitor with a short ballot
   sees no new UI.
4. **Composition.** Both filters AND together in one shared derivation —
   `deriveBallotFilters` in `@voteapp/api-client` replaces
   `deriveOnlyMyIssues` (same file, same four consumers) and returns the
   visible list, per-filter availability/engagement, and one combined
   hidden count. One "N elections hidden · Show all" line sits OUTSIDE the
   disclosure (visible without opening it), keeps the phase-3 `aria-live`
   container placement, and Show all clears BOTH filters. Web URL state:
   `issues=mine&impact=high|medium` (extend `useIssuesFilterParam` into a
   shared two-param hook; unknown impact values read as off); mobile stays
   local `useState`; the disclosure is an inline expandable section on
   mobile, not a portal.
5. **Tests.** Derivation unit tests (threshold boundary at exactly 7 and 8,
   unknown-label counting, AND composition, engaged-overrides-offer-gate);
   page tests per web surface — combined hidden count, Show all clears
   both params, `?impact=high` URL round-trip + arrives filtered, chip
   hidden on short/no-split ballots, badge count, followed-first still
   saves from inside the disclosure (saved page), phase-3 regression suite
   stays green under the new control.

## Sequencing / PR slicing

- PR A: phase 0 (canonicalizer + contract + tests + backfill script)
- local repair run, then prod repair after deploy
- PR B: phase 1 (bucket fn + web UI + mobile)
- PR C: phase 2
- PR D: phase 3
- PR E: phase 4 (impact filter + unified Filters control, one PR — the
  control exists to house the new filter)

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
