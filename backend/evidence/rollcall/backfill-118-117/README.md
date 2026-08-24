# 118th/117th backfill — phase 2

Plan §5 step 2: the federal expansion back from the 119th Congress, for
candidates whose service overlaps. Local database only. Migrations 251
and 252 are still local-only, so nothing here is promotable yet.

This is a **data run**. The pipeline needed no code changes, verified
before starting rather than assumed: the URL builder is congress-agnostic
(`1787 + 2N + session − 1` for the House year), all eight feed endpoints
answer, the classifier produced all four kept classes on a 162-roll
sample of 118-2 and 117-1 with no unrecognized final-passage spelling,
and the one resolver outcome new to these congresses, `no_fec_id`, is
already skipped by `collectVoters`.

## Survey

Every roll call in the four sessions was fetched: **3,879 rolls, 942
kept** by the classifier. No `judgmentCleared`, no `approved_conflict`,
no session mismatches. Twenty House parse errors, all the benign
Speaker-election shape (no `<totals-by-vote>`): 117-1 roll 2, the fifteen
ballots of January 2023, and the four of October 2023.

| Session | Rolls | Kept |
| --- | --- | --- |
| House 118-1 | 724 | 140 |
| House 118-2 | 517 | 223 |
| House 117-1 | 449 | 208 |
| House 117-2 | 549 | 285 |
| Senate 118-1 | 352 | 29 |
| Senate 118-2 | 339 | 18 |
| Senate 117-1 | 528 | 18 |
| Senate 117-2 | 421 | 21 |

`survey/` holds the eight per-run fetch reports — the full per-roll
ledger with the classifier's verdict and reason for every roll — and
`survey/divided-worklist.tsv`, the worklist described below. The raw
survey XMLs are not committed: each is re-fetchable by URL and pinned by
the `source_sha256` stored on its `legislative_votes` row. The XMLs for
judged rolls are committed in their batch directories, where the importer
checks them against that hash before writing.

## The judging gate

Phase 1 covered the current Congress, where every kept vote is live
context for a 2026 voter. This is backfill: the same candidates, older
votes, four times the volume. So a roll is judged only when the vote was
**divided** — the losing side at least a quarter of the winning side —
**and** a research area fits it without inventing a direction. That
leaves **376 candidate rolls**; the other 566 kept rolls are
near-unanimous and stay pending.

Carried over from phase 1 unchanged: trivia is dropped per plan §1 step
3; appropriations and continuing resolutions are never judged, because no
research area maps onto a vote to fund the government; votes whose
direction reasonable people read both ways are left pending; and no
judgment is written without a congress.gov CRS summary to ground it.

Two rules this run added:

- **One judged roll per chamber per measure — the decisive one.** Twenty
  measures were voted twice in a single chamber. Where the pair is the
  same bill (a failed suspension then a successful passage, or passage
  then concurrence), only the decisive vote is judged.
- **But check that the pair really is the same bill.** Congress reuses
  bill numbers as vehicles, and congress.gov carries only the enacted
  summary. H.R. 5376 was Build Back Better in November 2021 and the
  Inflation Reduction Act in August 2022; H.R. 6833 was the Affordable
  Insulin Now Act in March 2022 and a continuing resolution in September;
  S. 2938 was unrelated before it became the Bipartisan Safer Communities
  Act. In each case the vote that matches the summary on file is judged
  and the other is left pending, because there is no official text to
  write the other judgment from.

## Resuming in a fresh session

The scratch directory does not survive a session, but everything needed
is committed here.

1. `survey/divided-worklist.tsv` is the worklist: one row per divided
   roll in date order, numbered, with a `status` column naming the batch
   that judged it or `unjudged`.
2. Re-fetch the XMLs for the rolls a new batch will judge with
   `rollcall:fetch --chamber … --congress … --session … --rolls …` into a
   scratch dir (the rows are already on `legislative_votes`, so the fetch
   reports them `unchanged`), and copy the judged ones into the batch
   directory.
3. Pull the congress.gov title and CRS summary for each measure and write
   the judgment sentences from those, never from memory.
4. `rollcall:judge` → `rollcall:resolve` → `rollcall:import --dry-run` →
   read the dry-run report → `rollcall:import`, then commit the batch.

All commands need `DATABASE_URL=postgresql://localhost:5432/voteapp`
inline; the worktree has no `backend/.env`.
