# Plan: UI polish, election detail enrichment, content wording

Source: user feedback session 2026-07-18. Grouped into independent work items,
ordered A → B → D → C → E (small fast wins first, content audit last).
Check off as PRs ship.

## Group A — Visual polish (frontend only, one PR)

- [x] **"+N more areas" chip color** — `frontend/src/components/ElectionCard.tsx`
  (~line 205): overflow chip uses gray `bg-surface`; switch to the same light
  green as area chips (`AREA_CHIP_CLASS`: `border-green-600/40 bg-green-600/10
  text-green-900`).
- [x] **"Ballot measure" → "Ballot Measure", Democratic blue** — two spots:
  - `ElectionCard.tsx` ~line 153 (list card label)
  - `frontend/src/pages/ElectionPage.tsx` ~line 141 (detail section `<h2>`)
  Color the TEXT only (no background), Democratic party blue (#0015BC).
  Add a Tailwind color token if needed (e.g. `dem-blue`).
- [x] **Vote power color coding by level** — badges in `ElectionCard.tsx`
  (~line 147) and `ElectionPage.tsx` (~line 95) currently always
  `bg-rausch/10 text-rausch-dark`. New mapping (text+bg tint per level):
  - very_high → red
  - high → orange
  - medium (displays "Medium" until Group B renames it "Average") → amber/yellow
  - low (displays "Below average") → slate/blue-gray
  - very_low → gray
  Keep `unknown` hidden as today. Extract a shared `votePowerBadgeClass(label)`
  helper (frontend lib or api-client format.ts) so card + detail page stay in
  sync.
- [x] **Ballot measure research-area stance colors** — `ElectionPage.tsx`
  ~lines 142–157: measure tags already receive `stance: "for" | "against" |
  null` from the API (`ballot_measure_research_area_tags`, migration 088;
  mapped in `backend/src/pipeline/address/ballotLookup.ts` mapResearchAreaTag).
  Render: stance `for` → green chip, `against` → red chip, null → current
  saved/gray behavior. Stance color wins over saved-area green; keep sr-only
  "(saved)" cue if applicable.
- [x] **Candidate stance chips format + color** — `ElectionPage.tsx`
  ~lines 285–306. Replace "Civil Rights · 1 for" style with:
  - only `for` records: `Civil Rights +1` — green chip
  - only `against`: `Housing Affordability -2` — red chip
  - mixed: `Housing Affordability +1 -2` — yellow/amber chip
  Green no longer means "saved" on these chips; saved-ness keeps sr-only cue.
  Update `ElectionPage.test.tsx` + `ElectionCard.test.tsx` accordingly.

## Group B — "Medium" → "Average" wording (frontend + backend copy)

Wire enum value `medium` stays unchanged (no API break); display copy only.

- [ ] `packages/api-client/src/format.ts` ~line 56: `medium: "Medium"` →
  `"Average"`. Scale reads: Very low / Below average / Average / High /
  Very high.
- [ ] `backend/src/pipeline/address/votePower.ts` explanation copy: grades use
  `capitalize(level)` → "Medium"; formula/scale strings say "33+ medium" etc.
  (~lines 362, 454); any result/detail sentences mentioning "medium".
  Change user-visible words to "average" while internal level keys stay
  `medium`. Update votePower tests + format.test.ts.

## Group D — Candidate records ordering (small, frontend)

- [ ] `frontend/src/pages/CandidatePage.tsx` `groupRecords` (~line 58):
  non-saved group fallback sort is alphabetical. Change to public-salience
  order from `frontend/src/lib/researchAreaPriority.ts` (env → wealth gap →
  anti-corruption → gov efficiency → healthcare → …); "Other records" stays
  last. Saved-areas-first-by-user-rank behavior (orderGroupsByPreference)
  already correct — keep.
- [ ] Groups need `slug` to use the priority comparator — confirm record tags
  carry slug (they do: `research_area_tags[].slug`) and thread it through
  `RecordGroup`.

## Group C — Election detail page enrichment (backend + frontend)

- [ ] **Backend: add `office` + `research_areas` to detail payload.**
  `BallotLookupElection` (backend/src/pipeline/address/ballotLookup.ts ~line
  222) lacks both; the summary type has them. Extend
  `lookupElectionDetailById` / `loadFullElectionDetails` to include
  `office: OfficeSummary | null` (with `summary` description) and
  `research_areas: ResearchAreaSummary[]`. Mirror in
  `packages/api-client/src/types.ts` `ElectionDetail`.
- [ ] **Frontend: render office description, then affected areas** on
  `ElectionPage.tsx`, above candidates. Affected-areas chips reuse
  ElectionCard styling (saved-first ordering, green chips, no cap or a
  higher cap — detail page carries the full set per ElectionCard comment).
- [ ] **Sort dropdown fixes** — `ElectionPage.tsx` ~lines 232–245:
  - Verify what order the API actually returns candidates in (true ballot
    order vs alphabetical) before renaming "Ballot order". If alphabetical,
    label it "Alphabetical".
  - Default sort → "My issues first" when the viewer has saved research areas
    (label text: "My issues first", not "For my issues first").
  - Remove "Against my issues first" option (drop `against_mine` branch of
    `sortCandidatesByStance` / `CandidateSort`).

## Group E — Content quality audit (data workflow, minimal code)

- [ ] **Ballot measure wording rewrite.** Example of the problem:
  "Proposition 5: Recall of State Officers" — summary and what_yes_means are
  legalese ("addressing the election of a successor and vacancy handling").
  Target: an average reader understands what the measure does and what a
  yes/no vote changes, concretely. Work:
  - Audit existing `ballot_measures` rows (summary, what_yes_means,
    what_no_means); rewrite unclear ones via manual research workflow
    (voteapp-manual-research skill, no AI API calls).
  - Tighten plain-language rules in `backend/src/ai/enrichBallotMeasure.ts`
    prompt so future rows arrive readable. Honor prompt-simplicity rule:
    few single-idea rules, merge before adding.
- [ ] **Office description audit.** Once Group C exposes office summaries on
  the detail page, review each `offices.summary` row for accuracy and
  readability; rewrite where needed (same manual research workflow).

## Notes / decisions

- Stance data for ballot measures already flows end to end; frontend just
  ignores it today. No backend work needed for stance coloring.
- Vote power palette above is a proposal — user delegated non-high colors.
- "Democratic blue" = #0015BC (party brand blue), text color only.
