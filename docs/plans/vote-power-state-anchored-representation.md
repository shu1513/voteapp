# Vote power: state-anchored representation scale

Status: IMPLEMENTED (2026-08-24) — code + tests done in worktree; local DB
rescored (55,156 rows). Prod recompute pending deploy.

## Problem

The representation power score compares a district only against districts of the
same type (`recomputeRepresentationPowerScores` in
`backend/src/pipeline/loaders/districtsLoader.ts`). Los Angeles is the largest
California place, so it scores 0/100 and its city races show "below average"
vote power — while a statewide proposition shows "average" (via the
ballot-measure label bump). That inverts real per-vote leverage: a 3.86M-person
city electorate gives each vote ~10× the weight of a 39M-person statewide vote.
Users compare labels side-by-side on one ballot, so within-type-only scoring
misleads, and widespread "below average" labels discourage voting.

## Decisions (user-confirmed)

1. **Re-anchor representation to the voter's own state.** The score answers:
   "how much stronger is your vote here than one statewide vote?" Statewide
   races are the baseline at exactly 50 ("average"). Smaller districts score
   higher. The representation axis can never fall below 50, so representation
   alone never produces "below average".
2. **Fixed ruler (Option B).** The top of the scale is a hard-coded constant
   `K`, not the smallest district currently in the DB (that would make every
   score drift whenever tiny districts are imported later).
3. **Remove the ballot-measure +1 label bump** (`bumpLabel` in
   `calculateVotePower`). The new scale already encodes leverage; keeping the
   bump would push statewide props from "average" to "high" and outrank city
   races again. Keep the `direct_vote_on_policy` factor value for API
   compatibility, but it no longer changes the label or score.
4. **Remove the "Ballot measure" explanation row** (`ballotMeasurePart` in
   `votePower.ts`). Users know what a measure is; no boost to explain.
5. **No softening/encouragement copy lines.** Do not add "close races elsewhere
   need you more" or similar. Remaining "below average" labels come only from
   decisiveness (safe or uncontested races) and stay as-is.

## New formula

```text
ratio = state_population / district_population      -- statewide row: ratio = 1
score = 50 + 50 * ln(ratio) / ln(K),  clamped to [50, 100], rounded to 2 dp
K = 50000
```

- Clamp floor 50 also absorbs data errors where a district's population exceeds
  its state's.
- Grade cutoffs unchanged: 66+ high, 33+ medium, else low. Effective outcomes:
  score 50–65.99 → medium ("average"), 66+ → high. "low" becomes unreachable
  for scored districts (keep the code path for safety).
- The old statewide/us_house national-scope special case dies: every type
  anchors to its own state. A WY at-large US House seat = ratio 1 = 50, which
  is correct (it is effectively a statewide race).
- `NULL`/0 population still yields `NULL` score → "unknown" level (unchanged).

## Why K = 50,000 (data-derived, local DB 2026-08-24)

Median state-pop/district-pop ratios by type: us_house 13.7, state_upper 40.4,
state_lower 112.7, county 220.8, school_unified 819.5, place 6,824.9. LA place
ratio ≈ 10.2.

"High" requires 50·ln(ratio)/ln(K) ≥ 16, i.e. ratio ≥ K^0.32. Constraints:

- Median us_house (13.7, ~770k people) must stay "average" → K > ~3,600.
- Median state_upper (40.4, ~128k people) must reach "high" → K < ~105,000.

K=50,000 sits inside the band. Resulting scores: statewide/props 50; LA city
61; median us_house 62 (all "average"); median state_upper 67, state_lower 72,
county 75, school_unified 81, place 91 (all "high"); ratio ≥ 50,000 caps at
100 (roughly the top ~15% of places, mostly tiny Census places).

## Change list

1. `backend/src/pipeline/loaders/districtsLoader.ts` —
   `recomputeRepresentationPowerScores`: replace the scoped min/max SQL with a
   join from each district to its state's `statewide` row population and the
   fixed-K formula above. Export the constant (e.g.
   `REPRESENTATION_RULER_K = 50000`) so copy/tests share it. Update the model
   doc comment.
2. `backend/src/pipeline/address/votePower.ts` —
   - `calculateVotePower`: delete the `bumpLabel` call and
     `ballotMeasureWithRepresentation`-driven bump logic (keep the
     missing-axis cap). Re-check `calculateScore`: drop the +12 ballot-measure
     score bonus for consistency with the label change.
   - `explainVotePower`: delete `ballotMeasurePart`, `boostApplied`
     re-derivation, and the boost piece in `explanationResultFor`.
   - `representationFormula` + `REPRESENTATION_GRADE_SCALE` + `howCalculated`
     + `representationPart` detail copy: rewrite to describe the new formula
     ("score = 50 + 50 × ln(state population ÷ district population) ÷
     ln(50,000)…") with the district's real numbers; the "large/small for its
     type" language becomes state-relative ("vs one statewide vote"). The
     recompute-and-verify degradation rule stays (symbolic form on drift).
3. `backend/src/pipeline/address/ballotLookup.ts` — the
   `scope_max_population`/`scope_min_population` subselects and
   `representationScopeDescription` (~line 2060–2141) change to supply the
   state population (and state name) instead of type-scope extremes; adjust
   the `representationScope` shape passed to `explainVotePower` accordingly.
4. Tests: `backend/tests/pipeline/address/votePower.test.ts` (labels, no bump,
   explanation rows/copy), loader recompute tests if any, and api-client tests
   that assert factor/label behavior (`packages/api-client/src/format.test.ts`,
   `railSort.test.ts` — labels/values unchanged, so expect no changes, verify
   only).
5. Numeric `score` (45/55 blend) keeps its formula; representation norm now
   spans 0.5–1.0 instead of 0–1.0. Sorting stays monotonic — no client change.
   Frontend/mobile use the label + explanation payload only; no client edits
   expected beyond snapshot updates.

## Rollout

1. Merge code. 2. Run the districts loader recompute locally; spot-check LA
City Attorney (average, score ~61), a CA prop (average, 50), a state senate and
a small-city race (high). 3. `npm run typecheck` + `npm test` in `backend/`.
4. Prod: deploy, then run the recompute against prod per the standard prod
checklist (scores are derived data; no migration needed — pure UPDATE).

## Out of scope

- Decisiveness axis: unchanged (safe/uncontested races still grade low/none).
- No suppression of "below average" labels; no added encouragement copy.
