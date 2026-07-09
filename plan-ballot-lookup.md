# ballotLookup.ts Decomposition Plan

Written 2026-07-09 after a deep read of the file, its callers, its tests, and
the per-state finance families. Goal: shrink the 11,942-line
`backend/src/pipeline/address/ballotLookup.ts` to its actual job (assembling
ballot/election payloads) by finishing a migration the codebase already
started, without changing a single output byte.

## What the audit found

1. **The file is 12k lines, but ~9k of them are 22 inlined per-state
   campaign-finance loaders plus the federal FEC loader** — each a
   self-contained ~150–500-line block (`loadTexas…`, `loadWashington…`,
   `loadMassachusetts…`, …) with the same four-step shape: feature-flag
   gate → build requests → early-return if empty → a bounded sequence of
   SQL queries (summary, then direct breakdowns, outside groups, and
   industry-label classification as needed; the FEC loader runs five,
   Virginia two, each short-circuiting when the previous returns no rows)
   → map rows to `BallotLookupFinanceSummary`.
2. **The target pattern already exists in the repo — three generations
   coexist:**
   - *Externalized, static import* (the right shape): Kentucky —
     `kentuckyFinance/kentuckyBallotLookupFinanceLoader.ts`.
   - *Externalized, optional dynamic import*: Alaska, Arizona, Florida,
     Louisiana, Pennsylvania, Utah, Vermont — thin wrappers in
     ballotLookup.ts that `await import(...)` the family-folder loader and
     tolerate a missing module (`isMissingOptionalCampaignFinanceModule`).
   - *Fully inlined* (the problem): the other 22 states + FEC.
3. **~20 `build<State>FinanceSummaryRequests` functions are literally
   identical** except the two-letter state code and a type alias
   (verified Washington vs Texas: same body, `"WA"` ↔ `"TX"`).
4. **The aggregator (`loadCandidateFinanceSummariesByCandidateElection`,
   ~150 lines) is 30 sequential awaits followed by 30 copy-pasted merge
   loops.** Precedence is implicit in loop order; only "FEC wins last" is
   documented. Two inline helpers that belong to state families also live
   here (`isMarylandFinanceEligibleOffice`, `isMaineFinanceEligibleOffice`).
5. **Blast-radius correction:** finance loading runs only on the
   election-detail path (`lookupElectionDetailById` →
   `loadFullElectionDetails`). The ballot-summaries endpoint computes
   vote-power/competitiveness but never touches the finance tangle. Still
   the public detail page for every election, but not "every request."
6. **Test-coverage correction — the math is already pinned.**
   `votePower.test.ts` (340 lines) covers the tercile buckets, the
   uncontested cap, the ballot-measure bonus, unknown-input fallbacks;
   `competitivenessLabels.test.ts` covers the label thresholds. The
   original "no test pins these numbers" concern was wrong — no new math
   tests are needed.
7. **`ballotLookup.test.ts` is an 8,065-line, 47-test characterization net
   that already exercises every state's loader path** (per-state
   `vi.stubEnv("<STATE>_CAMPAIGN_FINANCE_ENABLED", "true")` + ordered
   `db.query` mocks; counts are the pre-Phase-0 baseline). It is coupled to the exact query *sequence*, which
   makes it brittle for reordering work but a perfectly good pin for
   verbatim code moves: if behavior and query order don't change, it
   passes unchanged.
8. All state finance flags default **off** (`readBooleanEnv(..., false)`);
   disabled states early-return before any SQL. Serial awaits are
   therefore cheap in practice — an election belongs to one state, so all
   other states' request builders produce zero requests and skip their
   query.

## Defect found while writing Phase 0 (blocks the precedence test)

Writing the FEC-precedence test surfaced a real bug, not just a missing test:

- `candidate_finance_summaries` has **no `election_id` column**; it is keyed
  `(fec_candidate_id, election_year)`, and the FEC loader joins on exactly
  those two columns.
- Every **state** loader joins its links table on `candidate_id` **and**
  `election_id`, so state finance is scoped to the specific election.
- `buildFinanceSummaryRequests` applies **no federal-office gate** — it emits
  an FEC request for *every* election of any candidate holding an `fec_id`.
- `fec_ids` are stored **additively** (`mergeIdentifierLists`), so a candidate
  keeps FEC ids from earlier federal races forever.
- The FEC sync itself gates correctly (`candidateFinanceBatchSync.ts`:
  Senate = `statewide` + `United States Senator` + `S…` id; House = `us_house`
  + `United States Representative` + `H…` id; presidential via cycles).

Consequence: a candidate running for a **state** office in the same year they
have a federal summary row (dual candidacy, or a retained id plus a same-year
federal race) gets federal money rendered on the state race — and because FEC
merges **last**, it *overrides* the correct state finance. Wrong dollar figures
on a real contest, failing silently.

Two further findings that shape the fix:

- **No state finance system covers federal offices** (all 30
  `*EligibleOffices.ts` lists are `statewide`/`state_upper`/`state_lower`/
  `place` state offices; zero reference `United States Senator`,
  `United States Representative`, or `presidential`). So once FEC is gated to
  federal offices, FEC and state **can never both** produce a summary for one
  `(candidate, election)` — the "FEC wins last" rule becomes unreachable by
  construction, and there is nothing to pin.
- A naive gate on `office_canonical_name` is **not** safe: elections can carry
  `office_canonical_name: null` (the existing FEC test's U.S. Senate fixture
  does) and still legitimately load FEC finance today. Requiring office
  metadata would silently drop federal finance wherever `office_id` is unset.

Therefore **Phase 0 is not "pin FEC-wins"** — that pins the bug. The real
contract to freeze is *FEC finance never reaches a non-federal election*, and
the test for it fails today, so it must land **with** the fix:

1. Gate `buildFinanceSummaryRequests` to federal offices, mirroring the sync's
   office+id-prefix rules. First resolve how to classify a federal election
   when office metadata is null (`discovery_contest_family = 'federal'`,
   backfilling `office_id`, or matching on id prefix) — this is the one real
   judgment call.
2. Regression test: a state office (Governor) held by a candidate with a
   retained `fec_id` keeps state finance and issues **no** FEC query.
3. Keep the existing federal-office FEC test as the positive case.

Phases 1–3 below are unaffected and can proceed once this lands.

## Verdict

The file needs the decomposition, not a rewrite. The math-pinning half of
the original pitch is already done by existing tests. What remains is
mechanical-but-wide: finish migrating 22 inlined states + FEC to the
family-folder pattern 7 states already use, dedupe the 20 identical
builders, and replace the hand-written aggregator with a registry loop.
Every step is behavior-preserving and verifiable against the existing
8k-line test net — that net is what makes this safe to do now and unsafe
to postpone (every new state grows the file and deepens the coupling).

## Phase 0 — pin the one unpinned behavior: merge precedence

Four behaviors make up the merge contract. Reading the suite showed
three are already pinned — do not duplicate them:

- *A state summary appears when its flag is enabled* — covered by the ~20
  per-state "includes locally synced … finance summaries" tests.
- *A disabled state issues no query* — covered for Oregon, Utah, Texas,
  plus the all-finance-disabled test.
- *An ineligible office loads no state summary* — covered for Alaska,
  Virginia, Massachusetts, Vermont, Wisconsin.

The one real gap: **FEC wins over a state summary for the same
candidate/election**. Every state test sets
`CANDIDATE_FINANCE_ENABLED=false` and the FEC test enables no state, so
the two sources never overlap anywhere in the suite — the precedence rule
lived only in a comment, and the Phase 3 registry rewrite is exactly the
change that could silently flip it. Phase 0 therefore adds a single test
(verified by mutation: merging FEC first fails only this test; the other
47 pass the flipped rule). No formula work, no assembly rewrite.

## Phase 1 — shared finance types/helpers module (reverse the dependency)

The blocker the moves would otherwise hit: the 7 already-external loaders
all `import type { BallotLookupFinanceSummary, ... } from
"../address/ballotLookup.js"` — the monster file is the type authority,
and moved loaders would also need *value* helpers (`candidateElectionKey`,
`parseFinanceAmount`, `parseFinanceCount`, `firstNonEmptySourceUrl`,
`mapFinanceBreakdown`, the industry display/explanation helpers), which
would create runtime import cycles back into the file that imports them.

- Extract to `pipeline/address/ballotLookupFinanceShared.ts` (one module;
  split only if it gets unwieldy): the finance summary/breakdown/evidence
  types, the helpers above, and one generic
  `buildStateFinanceSummaryRequests(stateCode, candidateRows, electionRows)`
  replacing the ~20 byte-identical `build<State>FinanceSummaryRequests`
  copies (all their request types collapse to one
  `{candidate_id, election_id}` type). States whose builder embeds real
  office logic (e.g. New Jersey's eligible-office filter) keep a custom
  builder — do not force them into the generic.
- ballotLookup.ts re-exports the moved types so the 7 existing external
  loaders keep compiling; they switch to the shared module as they're
  touched.
- Doing the builder dedupe *here*, before the moves, stops 20 identical
  copies from fanning out into 22 state folders and being deduped again
  later.
- Gate: `ballotLookup.test.ts` passes untouched (query order unchanged).

## Phase 2 — move the 22 inlined loaders + FEC out, verbatim (batched)

- Each inlined `load<State>CandidateFinanceSummariesByCandidateElection`
  body moves unchanged to its existing family folder as
  `<state>Finance/<state>BallotLookupFinanceLoader.ts`, exactly like
  Kentucky — importing its types/helpers from the Phase 1 shared module,
  never from ballotLookup.ts. Its row types, SQL, and mapping go with it.
  FEC goes to `pipeline/finance/fecBallotLookupFinanceLoader.ts` (the
  folder already holds the FEC clients; no new directory).
- `isMarylandFinanceEligibleOffice` / `isMaineFinanceEligibleOffice` move
  to their family folders beside the other states' eligible-office files.
- Static imports (Kentucky style). The optional-dynamic-import states
  (AK/AZ/FL/LA/PA/UT/VT) keep their current wrappers in this phase — no
  behavior change of any kind.
- Batch into ~4 PRs of 5–6 states so each diff is reviewable; the moves
  are verbatim, so review is "same bytes, new file."
- Gate per PR: full backend suite green with zero test edits, plus
  `git diff --stat` sanity — ballotLookup.ts only shrinks.

## Phase 3 — registry aggregator + import unification

- A typed list:

  ```ts
  type StateFinanceLookupAdapter = {
    state: string; // "WA"
    isEnabled: () => boolean;
    load: (db, candidateRows, electionRows) => Promise<Map<string, BallotLookupFinanceSummary>>;
  };
  const STATE_FINANCE_LOOKUP_ADAPTERS: readonly StateFinanceLookupAdapter[] = [...];
  ```

  The aggregator becomes: loop the registry in its current order, merge
  each result, merge FEC last with the existing "federal wins" comment —
  now guarded by the Phase 0 precedence tests, not just the comment.
  Keep the loop **sequential** — parallelizing changes query interleaving,
  breaks the ordered mocks in the 8k-line test file for zero practical
  latency win (only one state is non-empty per election).
- Decide the optional-import question once: the dynamic-import +
  missing-module tolerance exists for 7 states while 23 use static
  imports of code in the same repo. Unless a real build/packaging
  constraint surfaces (investigate the original motivation first), unify
  on static imports and delete `isMissingOptionalCampaignFinanceModule`,
  `missingModuleSpecifier`, and the per-state wrapper boilerplate. If a
  constraint does surface, keep the optional pattern but implement it once
  as a generic `optionalLoader(path, stateName)` helper instead of 7
  copies.
- End state: ballotLookup.ts ≈ 2.5–3k lines — payload assembly,
  vote-power/competitiveness wiring, the registry. Adding state #31 =
  write the family-folder loader + one registry entry; the monster file is
  no longer edited.
- Gate: full suite green; a grep gate that ballotLookup.ts contains no
  `_CAMPAIGN_FINANCE_ENABLED` string except via the registry imports.

## Explicitly not doing

- Parallelizing the loader awaits (behavior/test churn, ~no real latency
  win; revisit only if detail-page latency data says otherwise).
- A plugin framework, DI container, or config-driven loader discovery —
  the registry is a typed array literal, nothing more.
- Rewriting `ballotLookup.test.ts` — its order-coupled mocks are ugly but
  they are the safety net; restructuring tests belongs to a later,
  separate decision after the moves are done.
- Touching vote-power/competitiveness math or their tests (already
  pinned, out of scope).
- Decomposing the non-finance parts of the file (election/candidate
  payload assembly) — cohesive and load-bearing; the finance extraction
  already removes ~75% of the file.
- New golden/DB-integration test infrastructure — the existing mock net
  plus verbatim moves make it unnecessary for this refactor.

## Order and rationale

Phase 0 first because the merge contract is the one behavior the later
phases could silently change and the only one the existing net doesn't
pin. Phase 1 next because it is small, independently shippable, proves the
"suite passes with zero test edits" workflow, and removes the import-cycle
trap before any loader moves. Phase 2 is the bulk and is deliberately dumb
(verbatim moves) so it can be reviewed at volume. Phase 3 is the only
phase with judgment calls (registry shape, precedence documentation, the
optional-import decision) — by then the code is already out of the monster
file and each decision is isolated. Nothing here blocks launch or the SSR
work; it can proceed in parallel and pause cleanly between any two PRs.

*(Cross-checked against an independent Codex plan that reached the same
structure; its two genuine additions — the merge-precedence test gap and
the shared-module-before-moves dependency reversal — were verified against
the code and folded in above. Its suggestions not taken: a new
`federalFinance/` directory (the existing `pipeline/finance/` already
holds the FEC clients) and deduping the request builders after the moves
(that would fan 20 identical copies into 22 folders only to dedupe them
again).)*
