# Plan: Plain-language content wording

## Problem

User-facing AI-researched text is written at a newspaper/legal register and repeats
context the UI already displays. Observed on a real ballot (Baldwin Park, CA,
2026-11-03):

1. **Reading level too high.** Measure summaries say "Authorizes $11.25 billion in
   California general obligation bonds"; record descriptions say "Repeatedly
   rebuffed subpoenas". Target audience is every voter — text must be readable at
   a 6th-grade level.
2. **Candidate summaries restate the contest.** "…running for Superior Court
   Judge, Office No. 87" appears directly under an election card that already
   shows the office, date, and runoff badge.
3. **Candidate summaries carry horse-race narrative.** "He led the June primary
   with 42.04% and faces attorney David DeJute in the November runoff", "after
   defeating incumbent Alex Villanueva". Campaign-status content is not the
   candidate's record and duplicates what the ballot view already conveys.

Root cause for all three: the research prompts never define an audience or forbid
restating contest context. `ballotMeasuresPrompt.ts` says only "plain-language";
`candidateProfilePrompt.ts` says "short neutral bio summary";
`candidateRecordDiscoveryPrompt.ts` says "neutral and factual". The model defaults
to wire-service register and dutifully weaves in the election context the prompt
itself provides.

A fourth observation is **not** a wording problem: the same record renders under
multiple research-area groups (by-design grouping in `CandidatePage.tsx`,
`groupRecords`). Kept as an optional display phase.

## Affected text surfaces (verified — the only AI prose users see)

| Surface | Column | Prompt | Rows today |
|---|---|---|---|
| Candidate summary (ballot card + candidate page) | `candidates.summary` | `candidateProfilePrompt.ts` | 922 |
| Measure summary / what_yes_means / what_no_means | `ballot_measures.*` | `ballotMeasuresPrompt.ts` | 34 |
| Record description | `candidate_records.description` | `candidateRecordDiscoveryPrompt.ts` | 1,651 |

A fourth prompt writes to the same record `description` column:
`candidateRecordSourceRepairPrompt.ts` ("You may fix description…"), whose
repaired rows the enricher persists verbatim. It carries the same style rules so
repairs cannot reintroduce the old register.

`electionsPrompt.ts` emits no user-facing prose, and the presidential roster
prompt's `description` field is internal roster evidence, never displayed.
Presidential profiles reuse the candidate profile prompt, so they inherit the
fix. The manual-research skill reuses these prompts verbatim, so it inherits the
fix too.

## Non-goals

- No re-research of facts; existing sources, dates, and claims stay as researched.
- No new review/validator machinery for style (the reviewer loop already exists;
  style is enforced at generation time).
- No UI copy changes (legal text, labels, disclaimers are out of scope).

## Phase 1 — prompt style rules (code)

New module `backend/src/ai/providers/promptWritingStyle.ts` exporting a shared
rule block (array of prompt lines, matching the existing line-array idiom):

- Write for a 6th-grade reader: short sentences, everyday words.
- Prefer the plain phrase over the technical one ("borrow money" over "issue
  general obligation bonds", "refused" over "rebuffed").
- When a technical term is unavoidable, define it in plain words in the same
  sentence ("bonds — money the state borrows and pays back over time").
- Keep numbers concrete; do not round away meaning.

Splice the block into the four prompts (trailing position, after every
content rule, before "return JSON only"). Add profile-summary-specific rules to
`candidateProfilePrompt.ts`:

- Do not name the office, election, election date, or stage the candidate is
  running for — the app always shows the summary next to that context.
- No campaign-status or horse-race content: no vote percentages, no primary
  results, no "running for…", "seeking re-election", "faces X in the runoff".
- The summary is who the person is and what they have done: current role,
  career, qualifications.

Gates:
- Existing prompt tests updated; new assertions pin the style lines and the
  profile no-contest-context rules.
- Full backend suite green.

## Phase 2 — backfill existing rows (script)

One-off rewrite pass (no web research): a script reads each row, asks the AI to
rewrite to the Phase 1 style **changing no facts**, and updates the text column.

- Input text is authoritative: the model must not add, drop, or reorder claims.
- Candidate summaries additionally strip contest context: the script passes the
  candidate's known office/election so the model can remove those clauses.
- Batched with resume support (processed-ids table or file) so a crash or rate
  limit does not restart from zero. 2,607 rows total.
- Local `DATABASE_URL` only (same guard as other manual scripts).

Fact preservation is enforced in three layers — mechanical checks alone cannot
catch a flipped stance, lost negation, or changed amount, and for civic data a
silent fact change is worse than ugly wording:

1. **Mechanical pre-filter**: no URLs introduced or lost, no dates changed,
   length within a band of the original, non-empty. Cheap rejection of obvious
   breakage before spending a verification call.
2. **Independent fact-consistency verification**: a second AI pass (separate
   call, verifier role — never the rewriter judging itself) compares original
   and rewrite claim by claim and answers only: same facts, same direction,
   same quantities, nothing added or dropped? Any mismatch → the rewrite is
   discarded, the original row is kept unchanged, and the row id lands in a
   flagged list for manual review. Flagged rows are never auto-accepted or
   auto-retried into the database.
3. **Original text retention**: every rewritten row's prior text is stored
   (audit table keyed by table/row/column) before UPDATE, so any later report
   can be diffed against what research originally produced and reverted
   one row at a time.

Gates:
- Dry-run mode prints before/after pairs for a sample; human spot-check before
  the write run.
- Verification-pass discard/flag rate reported at the end of the run; a high
  flag rate (>5%) halts the run for prompt tuning instead of grinding through.
- Spot-check the Baldwin Park ballot (Prop 1, Office No. 87, Sheriff) reads at
  target level after the run.

## Phase 3 (optional) — record display dedupe

`CandidatePage.tsx` renders a record once per research-area tag. Alternative:
render each record once with all its area names as chips. Pure frontend change;
decide after Phases 1–2 land.

## Order

Phase 1 first (stops new bad text), Phase 2 second (fixes the 2,607 existing
rows), Phase 3 whenever.
