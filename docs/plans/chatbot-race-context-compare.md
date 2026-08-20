# Chatbot: race-collective questions on a viewed page — plan

Status: IMPLEMENTED on branch (2026-08-19), pre-merge. Parent:
`docs/plans/chatbot-rag.md` (Phases 1–2 built, LIVE IN PROD); builds on the
PR 4 race-members branch (`docs/plans/chatbot-improvements-2026-08.md`,
merged `5ec86846`).

Implementation deltas from the plan below (found during the deep read):

- `RACE_COLLECTIVE_RE` lives in `retrieval.ts` (exported; askService imports
  it) so one matcher arms both the context gate and the members branch.
  Final form adds `this/that/their race` and drops `the field`
  (false-positive risk, no demonstrated need).
- Two correctness fixes the plan missed, both required or a compare only
  ever sees the viewed candidate:
  - the contenders filter treats context-driven members as deliberate
    evidence (they have `contextRank` Infinity, no entity match, low cosine
    — the anti-filler filter would have dropped every opponent);
  - context-driven members take rank precedence over `contextRank` in the
    top-K sort (context-first would fill the cap with the viewed
    candidate's own chunk pile; the members set contains those chunks too,
    so nothing is lost).
- Clarify surfaces via `contextRaceAmbiguousTitles` (the tied races'
  listing titles) on `RetrievalResult`, checked in askService before the
  answerability gate.
- Eval gained `pageContext` golden-case support (entity name → source_id
  in the active generation). Keyword-only run 2026-08-19: all 4 new cases
  pass; the hybrid (embeddings-up) gate run remains a pre-ship step.
- Review round (post-plan): `RACE_OTHERS_RE` — a collective question that
  ALSO names a candidate ("compare Jon Ossoff with the other candidates")
  keeps race-member precedence; the entity match alone used to switch back
  to entity-first ranking, filling the top-K with the named candidate's
  chunks and dropping the opponents the members branch fetched. `each
  other` is excluded (a fully-named comparison stays entity-first). The
  eval's recall rerun also inherits the page context's state and skips the
  previous-turn carry-over when context is set, mirroring askService.

## Problem

On a candidate detail page (e.g. Nithya Raman, reached from the LA mayor
election in the split view), "compare the candidates for me" returns the
`refuse_no_data` copy. Two stacked gaps:

1. **Page context is only applied to deictic questions.** `askService.ts`
   applies the widget's page context when the question contains a pronoun
   (`DEICTIC_RE`: this/that/their/she/…) or names a page candidate. "compare
   the candidates for me" has neither, so the context is ignored and the bare
   text — no names, no race title — fails the answerability gate.
2. **Candidate context never pulls opponents' chunks.** Even when context IS
   applied, the context branch retrieves the candidate's own chunks plus the
   election *listing* chunk only (by design — enough for "who is she running
   against", not enough to compare records/finance across the field). The
   race-members branch that pulls a whole roster fires only on a
   high-confidence race *title* match ("the Los Angeles mayor race"), which a
   phrasing like "compare the candidates" never produces.

## Design decisions (agreed 2026-08-19)

- **No frontend or API-contract change.** The split view's rail knows the
  arrival election via nav state, but the server already knows every
  candidate's race from the candidate id (the context branch's own-races
  subquery). Resolving server-side covers ALL arrival paths — from the
  election, from search, from followed candidates — identically. Plumbing
  `electionId` through the widget was considered and rejected as redundant
  state to keep in sync.
- **Do not widen `DEICTIC_RE`.** Adding collective nouns to the pronoun regex
  would let every question containing "the candidates" ride page chunks
  through the gate from any page. Race-collective detection is a separate,
  narrower test applied only where context exists.
- **Reuse, don't rebuild.** `classifyRaceQuestion()` (money/records/neutral)
  and the race-members SQL + `RACE_MEMBER_TYPE_ORDER` + round-robin
  interleave already exist. The change routes an already-resolved election id
  into that branch instead of requiring a title match.
- **Refusal stays the default elsewhere.** The same question with no page
  context anywhere (asked from /ballot, no candidate/election viewed this
  session) still refuses — there is no race to compare.

## Changes (backend only, one PR)

### 1. `askService.ts` — race-collective questions count as pointing at the page

New narrow matcher next to `DEICTIC_RE`:

```text
RACE_COLLECTIVE_RE: compare | difference(s) between | the candidates |
who is running | the race | the field | each other
```

Context applies when the question is deictic (unchanged), names a page
candidate (unchanged), **or matches `RACE_COLLECTIVE_RE`** (new). The matcher
stays literal and short; grow it only on demonstrated misses (same policy as
`classifyRaceQuestion`).

### 2. `retrieval.ts` — members branch fires from context, not only title

- Resolve the context's election id: election context → itself; candidate
  context → the distinct `election_id`s of the candidate's own chunks (the
  existing own-races subquery, hoisted so its result is reusable).
- Exactly one election id → run the existing race-members query with it
  (same `RACE_MEMBER_TYPE_ORDER[classifyRaceQuestion(question)]` ordering,
  same `BRANCH_LIMIT`, same score-0 / rank-only contribution). Title-match
  activation stays as-is for questions that name a race with no page context;
  context activation wins when both apply (it is the stronger signal).
- More than one election id (candidate in two covered contests) → set a new
  `contextRaceAmbiguous: true` on the retrieval result and pull no members.
- Gate math unchanged: members contribute RRF rank only; the question passes
  the gate via `contextMatched`, which the context branch already sets.

### 3. `askService.ts` — clarify on the ambiguous case

When the question matched `RACE_COLLECTIVE_RE`, context resolved, and
retrieval reports `contextRaceAmbiguous` → return the existing `clarify`
outcome ("They're in more than one race we cover — which one do you mean?"
listing the contests), never an arbitrary pick (BEHAVIOR.md rule 7).

### 4. Golden set + gates

New cases:

- candidate context + "compare the candidates for me" → `retrieval`, expected
  entities = the race's filers (member profiles retrieved).
- same question, no context → `refuse_no_data` (unchanged behavior pinned).
- candidate context + off-topic question ("what will the weather be?") →
  still `refuse_no_data` (the collective matcher must not leak).
- candidate context + "who has raised more money?" → `retrieval` with
  finance-first member ordering (money classification exercised).
- multi-race candidate + collective question → `clarify`.

Re-run `npm run chatbot:eval` recall + template/refusal gates before ship
(BEHAVIOR.md release-gate rule). Evidence fed to the LLM changes for
contexted questions, so this ships under a prompt-version bump — it rides
the `p3` bump already staged on this branch (reading-level rewrite); no
separate bump needed if shipped together, otherwise bump to `p4`.

## Non-goals / rejected

- Frontend nav-state `electionId` plumbing (redundant; see decisions).
- Widening `DEICTIC_RE` (gate-leak regression risk).
- LLM question rewriting or multi-turn scope inference (v1 carry-over rule
  already appends the previous question; unchanged).
- Any answer that ranks or recommends candidates — comparison output stays
  field-by-field under BEHAVIOR.md rules 1–3 (the existing system prompt
  already enforces this; no prompt change).

## Rollout

Backend deploy only. No migration, no flag, no new spend path (same LLM call
count; member chunks ride inside the existing chunk budget). Revert = revert
the PR.
