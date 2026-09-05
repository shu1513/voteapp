# Alaska batch-03 — how these votes were judged

The sources, the version-check discipline, the rule on measures that pull both ways, and the
wording standard are all the same as batch-02. `../batch-02/JUDGING.md` states them in full
and is not repeated here. What follows is what is specific to this batch.

## Saying plainly that nothing happened

Every description ends by naming the chamber that never voted, so the reader is told the bill
died rather than left to infer it:

> The Alaska House passed it 26-14. The Alaska Senate did not vote on it, so it did not
> become law.

The body of every description is conditional throughout. Nothing here says a rule exists.

## The version each chamber actually voted

Alaska dates a committee substitute when the committee offers it, not when the chamber adopts
it, which makes the latest text on a bill page a poor guide to what was voted. Each roll was
matched to its own version:

| roll | version voted | how it was confirmed |
| --- | --- | --- |
| HB 20, House, 2026-03-04 | `CSHB 20(JUD)` | history line "JUD CS ADOPTED UC" the same day; the document is dated 2026-02-06 |
| HB 58, House, 2025-04-30 | the introduced bill | no committee substitute appears anywhere in the history |
| SB 111, Senate, 2026-05-11 | `CSSB 111(L&C) am` | journal: "and so, CS FOR SENATE BILL NO. 111(L&C) am passed the Senate" |
| SB 250, Senate, 2026-05-16 | `CSSB 250(CRA)` | history line "CRA CS ADOPTED UC" on 2026-05-15 |

SB 111 is the one that would have gone wrong without this step. The latest text LegiScan
lists is the **House** committee substitute of 2026-05-15, produced after the Senate had
already voted. The description is written from the Senate's version.

## SB 250 and the reconsideration vote

LegiScan carries two Senate rolls for SB 250 on 2026-05-16: a 13-5 final passage and a 14-5
passage on reconsideration. The bill history shows the order plainly — passed 13-5, Senator
Myers gave notice of reconsideration, the Senate voted to reconsider, and then
`PASSED ON RECONSIDERATION Y14 N5`. The 14-5 roll is the vote that stands, and the 13-5 roll
is listed in `acknowledge_later_rolls` so the superseded-stage gate can be satisfied without
suppressing it.

## Checks

The lint, reading-level, spelling, tally-oracle and journal checks were run across both
batches together and are reported in `../batch-02/JUDGING.md`. For the four rolls here:
0 lint warnings, Flesch-Kincaid grades 6.9 to 7.8, longest sentence 25 words, all four
tallies confirmed against the bill history, and no member on the wrong side in the journal.

## Reconciliation

Predicted independently from the crosswalk before touching the database: **12 records and 10
area tags**. SB 250 accounts for the gap between them, because its single yea-voting senator
earns two area tags.

| source | records | tags |
| --- | --- | --- |
| independent prediction | 12 | 10 |
| importer dry run | 12 insert | — |
| importer real run | 12 insert, 0 errors | — |
| database, this run's stamp | 12 | 10 |

The dry run's stamp `2026-09-04T20:28:53.301Z` matches zero rows. The real run's stamp is
`2026-09-04T20:29:38.702Z`, and the convergence re-run reported all 12 unchanged.

No roll in this batch flagged a related hand-researched record.
