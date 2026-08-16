# 50-state ballot order rules — research plan (v1)

## Goal

Replace the single national approximation in
`backend/src/pipeline/address/ballotContestRank.ts` with **per-state contest-order
rules**, each backed by a primary legal citation and corroborated against a real
sample ballot. The sort keeps its name (`state_baseline`) and stays request-only;
it just becomes correct per state instead of correct-on-average.

This is the cheap, durable slice of ballot-layout research: contest order is set by
**state** statute or state election-authority rule, is stable across cycles, and is
~51 records — versus per-county layout (thousands of ballot styles, republished
every cycle), which stays in ballot-facsimile Phases 2–4.

## Scope

- 50 states + DC (51 jurisdictions).
- **General elections only** (November). If a statute orders primaries differently,
  note it in one line and move on.
- **Contest order only**: the sequence of contests down the ballot. Explicitly NOT:
  physical layout, columns, color, paper format (Phase 2); per-county placement data
  (Phase 3); candidate order *within* a contest (rotation is precinct-dependent and
  we never hold precinct data — record the rule as informational only, never
  implement it).

## Research schema — every state answers the same 7 questions

1. **Authority** — the statute / administrative rule / secretary-of-state directive
   that governs general-election contest order. Exact cite, URL, access date. If
   order is set by an SOS manual rather than statute, cite the manual + edition.
2. **Office order** — federal → state → local sequence as the state actually
   prescribes it. Specifically capture: where Governor sits relative to US House
   (Maine-style deviations), and the internal order of statewide executives
   (Governor, Lt. Gov, SOS, AG, Treasurer, Auditor, …).
3. **Judicial placement** — within-level (TX/NC style), late nonpartisan block
   (MN/OH/MI style), or separate ballot/section; where retention questions print.
4. **Measures placement** — after all offices (majority), before offices (WA),
   interleaved per jurisdiction (MN), and whether statewide constitutional
   amendments are ordered differently from local propositions.
5. **County/local discretion** — does state law delegate order (fully or partly) to
   county officials? "No statewide rule" is itself a finding: record it with the
   citation showing the delegation, and the state keeps the generic baseline.
6. **School / special districts** — where school board and special-district offices
   fall (they're often in the delegated/local tail).
7. **Corroboration** — one real sample ballot from a populous county in that state
   (2024 general preferred; 2022 acceptable, labeled). County name + URL + what it
   confirmed. Statute vs. sample conflict → record BOTH, then investigate; county
   discretion usually explains it. Never silently pick one.

## Confidence grades

- **A** — explicit primary authority AND a sample ballot that matches it. Only
  grade A earns a code override.
- **B** — authority found but no sample corroboration yet (or vice versa: clear
  consistent samples, no statute located). No code change; listed for follow-up.
- **C** — no statewide rule found / genuine county option / conflicting evidence.
  Generic baseline stays, entry documents why so nobody re-researches it.

## Sourcing rules

- Primary sources only for the Authority field: state election code, state admin
  code, official SOS ballot-preparation manuals or certification orders.
- Ballotpedia / vendor pages / news are **pointers only** — every fact chased to
  the primary source before it enters the doc.
- Every entry records URLs + access dates. Statute quotes kept to the shortest
  decisive phrase.
- Already-verified seeds (from the #719 research round) go in on day one, marked
  as **partial** — the judicial/measures aspect is verified, the full 7-question
  schema is not: MN Rule 8250.1810 subp 5 (judicial last); Ohio RC 3505.04; MI
  nonpartisan section; TX Elec. Code §52.092 (within-level); NC GS §163-165.6
  (within-level); WA RCW 29A.36.161(3) (measures first); ME 21-A §601 (Governor
  before US House — re-verify cite).

## Known wrinkles to handle deliberately (not silently)

- **Nebraska**: unicameral, nonpartisan Legislature — chamber mapping to our
  `state_upper`/`state_lower` scopes needs a decision, not an assumption.
- **DC**: no state tier at all (Delegate, Mayor, Council, ANC) — needs its own
  mini-order.
- **Louisiana**: November all-comers primary structure — order rules still apply,
  but verify which November ballot the statute is describing.
- **Straight-ticket / party-column states**: a party-column format can make
  "contest order" mean column order — record what the statute actually orders.
- States where the deciding document is an SOS *manual* (not statute) may
  republish per cycle — the entry must carry the edition/cycle.

## Data destinations

1. **Reference doc** — `docs/research/state-ballot-order.md`. One file, one
   section per jurisdiction using the fixed template (skeleton ships with this
   plan, seeds pre-filled, the rest PENDING). Findings land here immediately
   during research, never held in session context.
2. **Code** — `backend/src/pipeline/address/stateBallotOrderRules.ts` (new): a
   per-state override consumed by `stateBaselineContestRank`, keyed by
   `district.state_fips` (already on every ballot election summary —
   `ballotLookup.ts` carries `state_fips`; the rank function's header comment
   already reserves this exact extension point). Only **deviations** from the
   majority baseline get entries; a state matching the baseline gets NO code, the
   baseline is its rule. Overrides are grade-A-only.
3. **Tests** — `ballotContestRank.test.ts` grows a case per override, citation in
   a comment, pinning the deviation (e.g. WA: measures before offices).

## Execution mechanics

- **Batches of ~10 states, population-descending** (value lands where users are):
  1. CA TX FL NY PA IL OH GA NC MI
  2. NJ VA WA AZ TN MA IN MO MD WI
  3. CO MN SC AL LA KY OR OK CT UT
  4. IA NV AR KS MS NM NE ID WV HI
  5. NH ME MT RI DE SD ND AK VT WY, then DC
- Per batch: fan states out to research subagents (per-state prompt = the
  7-question schema + sourcing rules); each result appended to the reference doc
  before the next batch starts.
- **Checkpoint file** `scratch/state-ballot-order-research-state.md`: cursor
  (which states done), open conflicts, grade tally. Fresh session per 1–2 batches;
  resume from the checkpoint, per the token-discipline rules.
- Docs-only PR per completed batch (cheap review, nothing blocks on code).
- Code PR(s) with overrides + tests once a batch's grade-A set is stable —
  overrides can ship incrementally; the baseline keeps covering everyone else.

## Acceptance criteria

- 51/51 jurisdictions have a graded entry; zero PENDING.
- Every A/B entry has a working primary-source URL + access date.
- Every code override maps to a grade-A entry; every grade-A deviation from the
  baseline either has an override or a one-line reason it doesn't need one
  (e.g. deviation invisible at our tier granularity).
- Conflicts (statute vs. sample) are recorded, none silently resolved.
- Backend suite green; `state_baseline` still rejected by
  `SAVEABLE_BALLOT_PREFERENCE_SORTS` (request-only, DB CHECK migration 152).

## Guardrails

- Candidate rotation: research-note only. Never encoded, never implied in UI.
- No addresses, no precinct data — everything stays keyed on districts/state_fips.
- No AI-provider calls anywhere in this campaign (web research only).
- Prior-cycle sample ballots always carry their cycle label in the entry.
- This plan orders contests only — any temptation to also capture layout/color
  during research is Phase 2's job; don't blend the datasets.
