# Nevada batch-07 — judging and import

**Result on local `voteapp`, 2026-09-06: 17 roll calls approved, 336 records inserted
across 41 candidates, 0 errors, 0 notified. Production untouched.**

Nevada now holds **2,086 live roll-call records**.

## Sources

Each measure was judged from the text the chamber actually voted, downloaded through the
LegiScan `getBillText` API with both the byte length and the MD5 checksum verified against
the dataset. All documents passed both checks.

Version comparisons used the corrected method from batch-06: no filtering by fragment
length, only true page furniture removed, and every fragment containing must, may, shall,
not, solely or only flagged for reading. That method is now a script,
`ne_diff.py`, so the batch-06 error cannot recur by hand.

## ⚠ The import report trap, hit again and now understood

The review of PR #1193 found that batch-05 and batch-06 had been committed with batch-04's
import reports. The cause is now clear, and it is worth writing down.

**The importer keeps the FIRST run's report as `import-report.json` and overwrites a single
`import-rerun-report.json` on every later run.** So after batch-04, `import-report.json`
stopped changing, and each new batch's real ledger landed in the rerun file — where the next
batch's run promptly overwrote it. Copying `import-report.json` after a later batch copies
batch-04's numbers.

This batch's files were checked before committing: `import-report.json` here is the rerun
file, its `startedAt` is `2026-09-06T22:50:12.012Z`, which equals this batch's run stamp,
and its action counts are 336 inserts against 1,750 unchanged. The dry-run file is the
matching `import-dry-run-rerun-report.json` at `22:49:46.553Z`.

**The rule for every future batch: copy the ledger out immediately after the run, and assert
that its `startedAt` equals the run stamp before committing it.**

## Superseded-stage gate

The gate did not fire on any roll in this batch.

## Labels

Nine measures, nine research-area labels, all with `"nay": null`. The area choices for
`immigration` and `election_integrity` were checked against existing campaign usage rather
than picked fresh; the reasoning is in `PLAN.md`.

## Counterweights considered and not treated as opposite directions

- **SB 85** removes the local-government cost reimbursement path, so counties carry the cost
  of the data collection themselves. That is a cost shift, not a change of policy direction,
  and the description says the bill left local agencies to carry the cost.
- **SB 141** says nothing in it requires a jail "to construct additional buildings or
  facilities or to implement any additional training". That limits how far the new duty
  reaches; it removes nothing that existed.
- **AB 589** adds exceptions to existing genetic privacy statutes for one criminal justice
  use, while broadening those same statutes from genetic information to DNA samples and
  raising the penalty from a misdemeanor to a felony. The net movement is plainly toward
  privacy.
- **AB 597** lets a clerk direct these voters into the nonpartisan voting method in very
  small precincts, and withholds local cost reimbursement. Neither reverses the opening of
  the primary.

## Wording checks, all run before the import

- The real `candidateRecordPlainLanguageLint` over all 34 descriptions: **0 warnings**.
  It flagged SB 63's Senate description at 46 words on the first pass; that sentence was
  split, which pushed the description to five sentences, so two sentences were then merged
  to bring it back inside the 2-to-4 range. Both fixes were made before anything was
  imported.
- Every description is 2 to 4 sentences.
- British spellings scanned for. Three were found and fixed before importing: "licence" in
  AB 140, "programme" in AB 589, and "adverts" in SB 63.
- Every description cites its own roll call's tally, checked against the stored row.
- Measure, date and chamber checked against the stored row for all 17 judgments.

## Reconciliation — three ways

| check | result |
| --- | --- |
| import report | 336 inserts, 1,750 unchanged, 0 errors |
| run-stamp predicate `rollcall:NV:%:2026-09-06T22:50:12.012Z` | 336 records, 41 candidates |
| table delta | 2,086 − 1,750 = 336 |

Per-roll fan-out: 29 or 30 candidates on Assembly rolls, 10 or 11 on Senate rolls, none at
zero.

## Duplicate sweep

Swept with `origin_run_id NOT LIKE 'rollcall:%'`. Within a candidate there are **0 duplicate
record identity keys and 0 duplicate source URLs** across all Nevada roll-call records.

Six hand-written rows matched a measure number in this batch and all six were kept. Three
are Kansas Senate Bill 63, a different bill that shares a number. One is an Ohio
ranked-choice-voting law. One is a member of the public testifying against AB 597, which is
not a legislator's vote. One is a legislator presenting AB 140 to a committee, which is a
different act from voting on it.
