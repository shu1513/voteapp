# Plan: candidate stance summary (supports / opposes / mixed by issue)

Status: implemented (PR #709). Frontend-only; no backend, API, route, or
migration changes. One PR.

## Goal

The ballot-measure detail view explains a measure with a green "A YES vote
means" box and a red "A NO vote means" box. Give the candidate detail page the
same at-a-glance treatment: after the candidate's summary and before the
Track record list, a green box lists the research areas the candidate's
records support, a red box the areas their records oppose — and a third amber
box lists areas where the record is **mixed** (both for and against records).

Mixed is shown, never collapsed: this is a voter-information product, and
majority-voting a 3-for/2-against record into a plain "Supports" chip would
assert a position the evidence doesn't hold. Flip-flops and genuine nuance are
exactly what a reader wants surfaced; the amber box shows the split and lets
them open the matching Track record group to judge.

## Background / verified facts

- **Stance data model.** `CandidateRecordTag.stance` is `"for" | "against" |
  null` (`packages/api-client/src/types.ts:518`). The DB enforces the same:
  `chk_candidate_record_area_tags_stance CHECK (stance IS NULL OR stance IN
  ('for','against'))` (`db/migrations/090_remove_neutral_candidate_record_area_stance.sql`).
  Historical `mixed` / `unknown` / `neutral` values were removed by
  migrations 058, 059, and 090 — they cannot appear in payloads.
- **Stance is required except on non-stance areas.** The label contract
  rejects a missing stance for every area except `general` and
  `integrity_and_ethics`, where a stance is *forbidden*
  (`backend/src/contracts/candidateRecordAreaLabelPayloadContract.ts:71`,
  `backend/src/pipeline/candidates/candidateRecordResearchAreaPolicy.ts:4`).
  So a null-stance tag only ever marks relevance, never a position.
- **The aggregation already exists.** `aggregateRecordAreaStances(records)`
  (`packages/api-client/src/researchAreaScoring.ts:55`) rolls a candidate's
  record tags into per-area `{for_count, against_count}`, dropping null-stance
  tags — which means `general` / `integrity_and_ethics` can never surface in
  its output. Every returned area has `for_count + against_count >= 1`.
- **The three-way color semantics already exist.** Election-page roster rows
  color each area name green (all-for), red (all-against), or amber (mixed)
  from those same counts, with `+N`/`-N` visible text and spelled-out sr-only
  counts (`frontend/src/pages/ElectionPage.tsx:703-751`). This plan reuses
  that classification rule verbatim, in box form.
- **Measure box styling to mirror.** `rounded border border-green-200
  bg-green-50 p-3` + `text-green-900` headings, red equivalents
  (`frontend/src/pages/ElectionPage.tsx:482-491`); the mobile election screen
  has the same boxes. Tailwind v4 default palette is in use (`amber-900`
  already appears in ElectionPage), so `border-amber-200 bg-amber-50
  text-amber-900` need no theme work.
- **Evaluative areas.** For `legal_competence` and `impartiality`, for/against
  grades the *evidence* (favorable/unfavorable), not advocacy —
  `EVALUATIVE_AREA_SLUGS` + `stanceLabel` in
  `frontend/src/pages/CandidatePage.tsx:222-234`. "Supports Legal Competence"
  would state an intent the data never claimed, so these two areas are
  excluded from the summary boxes; their favorable/unfavorable tallies remain
  on the Track record group headers (`groupStanceCounts`,
  `CandidatePage.tsx:204`).
- **Ordering.** `compareByResearchAreaPriority`
  (`packages/api-client/src/researchAreaPriority.ts`) orders areas by public
  salience with unranked slugs sinking alphabetically — the same order the
  Track record groups use, so the boxes and the groups below list areas in a
  consistent sequence. (`aggregateRecordAreaStances` returns slug order;
  re-sort its output.)
- **Data flow.** The page's route loader serves the full record history
  (newest-first, uncapped) from `GET /api/candidates/:id`
  (`backend/src/pipeline/candidates/candidateDetailReader.ts:255`), so the
  section derives client-side from loader data and renders in SSR HTML —
  crawlers and no-JS readers see it. No new fetches.
- **Insertion point.** `CandidatePage.tsx:564` renders the summary paragraph;
  pick rows follow at 566, finance at 586, Track record at 594. The measure
  page puts its yes/no boxes directly after the summary and *before* the
  choice buttons — mirroring that puts this section immediately after the
  summary paragraph, before the pick rows.
- **Mobile.** The mobile candidate screen groups records but renders no
  stance chips at all today (`mobile/src/app/candidates/[candidateId].tsx`) —
  a pre-existing gap. Mobile parity for this section is a follow-up, not part
  of this PR.
- **Tests.** `CandidatePage.test.tsx` renders via `renderCandidate(() =>
  candidateDetail({records: [...]}))` with `stubApiRoutes` fixtures
  (`frontend/src/test/fixtures.ts:155`); new tests follow that pattern.

## Classification rule

From `aggregateRecordAreaStances(candidate.records)`, minus evaluative areas,
sorted by `compareByResearchAreaPriority`:

- `against_count === 0` → **Supports** (green)
- `for_count === 0` → **Opposes** (red)
- both > 0 → **Mixed record** (amber)

Null-stance tags never reach the aggregator's output, so `general` and
`integrity_and_ethics` are excluded by construction, and untagged records
contribute nothing. No thresholds, no majority rule: one against-record among
five for-records makes the area Mixed, on purpose.

## UX spec

- Renders only when at least one area classifies; otherwise nothing (no empty
  boxes, no heading). Judicial candidates whose only stance-bearing areas are
  evaluative therefore show nothing — correct, not a bug.
- Position: directly after the summary paragraph, before the pick rows.
- Layout mirrors the measure boxes: `grid gap-3 sm:grid-cols-2`, green box
  "Supports" and red box "Opposes". When Mixed areas exist, a full-width
  amber box "Mixed record" sits below the pair. A side with no areas renders
  no box (the other spans naturally; never an empty placeholder).
- Box content: a comma-separated list of area names (matching the roster
  rows' text treatment — boxed chips read as buttons), each with its count:
  - Supports/Opposes: `Environment (3 records)` — singular/plural handled.
  - Mixed: `Gun Control (2 support · 1 oppose)` — same phrasing as the Track
    record group headers (`CandidatePage.tsx:669-675`), and for evaluative
    consistency it can't collide since evaluative areas are excluded.
- Headings inside the boxes: "Supports" / "Opposes" / "Mixed record", with a
  shared lead-in line above the grid (small, `text-ink-soft`): "Where they
  stand, based on their records:" — the boxes must not read as
  self-descriptions or endorsements; the phrasing pins the derivation to
  records, matching the "Records:" label precedent on roster rows.
- Accessibility: an `sr-only` `h2` ("Where {name} stands, based on their
  records") above the grid so the section lands in heading navigation; box
  titles are `h3` like the measure boxes. Counts are plain spelled-out text —
  no `+N`/`-N` compression needed here, so no sr-only duplication.
- Not links in v1: area names are static text. Jumping to (and opening) the
  matching Track record `<details>` group is a follow-up.

## Implementation

1. Derivation helper in `CandidatePage.tsx` (module scope, not exported —
   tests exercise it through page rendering, and exporting it would trip the
   fast-refresh lint rule):
   `classifyStanceSummary(records: CandidateRecord[]): {supports:
   AreaStance[]; opposes: AreaStance[]; mixed: AreaStance[]}` wrapping
   `aggregateRecordAreaStances` + evaluative filter + salience sort. Reuses
   the existing `EVALUATIVE_AREA_SLUGS` const already in the file.
2. `StanceSummary` presentational component in the same file (the page
   already keeps `RecordItem`, `StanceChip` etc. local); render it between
   the summary paragraph and the pick rows.
3. Tests (`CandidatePage.test.tsx`):
   - All-for area lands in the green box, all-against in red, split in amber
     with "2 support · 1 oppose" phrasing.
   - Areas ordered by salience within a box (use real ranked slugs, e.g.
     `healthcare_affordability` before `gun_control`).
   - Evaluative areas (`legal_competence`, `impartiality`) never appear even
     when stance-bearing; null-stance (`integrity_and_ethics`) never appears.
   - No stance-bearing records → section absent entirely.
   - One-sided profile renders only the one box, no empty counterpart.
   - Record with tags on two areas counts once per area (per-area tallies,
     documented double-count).
4. `npm run typecheck`, `npm test`, `npm run lint` in `frontend/`.

## Explicitly out of scope (v1)

- **Majority/threshold collapsing of mixed areas** — rejected as misleading
  (see Goal).
- **Linking box entries to their Track record groups** (needs
  `<details>`-open state coupling or anchor ids; follow-up if asked).
- **Mobile app parity** — the mobile candidate screen has no stance rendering
  at all yet; porting this section (and the missing chips) is its own task.
- **Backend/API changes** — none; the shared aggregator already serves both
  web and mobile election screens.
- **Weighting by recency or record volume** — counts are shown raw; any
  scoring beyond for/against presence is a different feature.

## Risks / notes

- **Prolific incumbents** can carry stances on 15+ areas; comma-separated
  text wraps fine (roster rows already handle this), but the boxes may get
  tall. Accepted for v1 — the section is still far shorter than the record
  wall it summarizes.
- **A single record tagged on several areas** counts toward each area's
  tally; the per-area counts are honest per-area but their sum exceeds the
  record count. Same semantics as the existing group headers — not new.
- **Sparse research** can make one candidate in a race show a rich summary
  and an opponent nothing; the Track record section's
  researched/not-researched messaging (`CandidatePage.tsx:703-707`) already
  carries that caveat, and the section's absence-when-empty avoids implying
  "no positions".
