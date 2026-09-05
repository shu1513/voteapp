# Nevada batch-03 — how these votes were judged

Sources, the version-check discipline, the rule on measures that pull both ways, and the
wording standard are the same as batch-02 and are stated in full in `../batch-02/JUDGING.md`.
What follows is specific to this batch.

## No version questions

All seven rolls are the only divided roll on their measure, and the survey worklist records
`amend_after_roll = none` for every one. Each chamber voted the text that became law, so no
description here has to name a version.

## Saying what these acts do not do

More than in either earlier batch, the measures here are narrower than their titles suggest,
and every description says so in its own words:

- **AB 420** requires the numbers to be published. The description says it does not limit when
  force may be used and sets no penalty for failing to publish.
- **AB 460** does not hand anyone custody. The description says the person named still has to
  ask a court to appoint them.
- **SB 39** creates a loan fund with no money in it. The description says the act puts no money
  in the account, that it fills only if federal grants arrive and a later legislature adds a
  match, and that only local and tribal governments may borrow.
- **SB 157** does not say what laboratories must test for. The description says the bill sets
  no pass or fail limits and leaves those to the Board.
- **SB 424** is a charge and a refund at once. The description says a company with few Medicaid
  patients can still pay more than it gets back.
- **AB 540** names the $133 million and the match requirement, and also names the two things
  that cut the other way — the widening to 150 percent of area median income and the dropped
  rule that had aimed local fee relief at lower-income projects.

## Checks run before importing

| check | result |
| --- | --- |
| Repository plain-language lint, 45-word sentence cap | 14 descriptions, **0 warnings** |
| `nv_check.py` | **0 problems** |
| Flesch-Kincaid grade | median **7.0**, worst **8.5** |
| Banned areas | 0 used |
| Every stated tally against the stored vote row | **7 of 7 match** on chamber, measure, date and tally |

## Reconciliation

Predicted independently from the crosswalk before touching the database: **94 records and 69
area tags**.

| source | records | tags |
| --- | --- | --- |
| independent prediction | 94 | 69 |
| importer dry run | 94 insert | — |
| importer real run | 94 insert, 0 errors, 0 notified | — |
| database, this run's stamp | 94 | 69 |

The dry run's stamp `2026-09-05T03:52:16.664Z` matched zero rows. The real run's stamp is
`2026-09-05T03:52:18.296Z`, and the re-run reported all 94 unchanged.

Nevada's three batches now hold **746 records and 532 area tags across 39 rolls**, and the
per-batch stamps reconcile exactly: 387 / 274, 265 / 189, and 94 / 69.

**A caution on verifying this.** A `LIKE '%<timestamp>%'` filter over `origin_run_id` returns
the wrong count, because the run id also carries the roll number and the pattern matches more
rows than intended. Group by
`regexp_replace(origin_run_id, '^rollcall:NV:[a-z]+:2144:[0-9]+:', '')` instead. That is how
the numbers above were confirmed.
