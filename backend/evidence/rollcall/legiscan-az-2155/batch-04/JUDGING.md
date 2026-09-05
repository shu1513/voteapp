# Arizona batch-04 — judging and import

## Sources

Same as batch-03: the staff analysis stamped `Vetoed` or `As Vetoed`, and the last engrossed
print as the text the Governor was sent. Neither carries a sponsor statement of intent.

## Vote and version checks

All 50 rolls were confirmed to be on the text sent to the Governor, using batch-01's version
rule. A single query over all 50 found **no later or same-day kept peer**, so no judgment in
this batch needed `acknowledge_later_rolls`.

The constitutional-majority defect described in batch-03's notes — 25 rolls across the session
where LegiScan records `passed = 1` on a vote Arizona's own history calls FAILED — touches
nothing here either.

## Duplicate sweep, and two records retired

The importer reported 0 related flags. The same strict sweep used in batch-03 — same candidate,
same event date, text asserting a vote, same bill number — found two, and both were checked
against the roll evidence before retirement:

| member | measure | chamber | stated | roll |
| --- | --- | --- | --- | --- |
| Analise Ortiz | SB 1089 | Senate | against | Nay |
| John Kavanagh | SB 1612 | Senate | for | Yea |

Retired with reasons naming the replacing record ids; `duplicate-retirements.json` is kept for a
production run.

## Labels

Ten area-and-direction pairs across seven areas. `anti_corruption` is new to Arizona. Both
directions appear in `environment_and_public_health`, `public_safety_and_crime_control` and
`corporate_accountability`.

Every nay is stated and every nay is `null`. On the groundwater measures the objection to a
`no` vote is usually farm economics or property rights, which is a different axis from the
area's own goal; on the welfare measures it is cost.

## Writing checks

- Plain-language lint: **100 descriptions, 0 warnings.**
- Reading level: **Flesch-Kincaid median 9.4, best 6.4, worst 10.7**, mean sentence 17.1 words,
  longest 41. A first draft measured median 9.6 and worst 12.0; three bodies were rewritten
  before import.
- Bodies are conditional, asserted by the builder.

**⚠ Two British spellings reached the database and had to be fixed after import.** The
builder's spelling check had been silently inert since an earlier edit double-escaped its regex
through a shell heredoc, so it required a literal backslash and matched nothing. `programme`
(HB 2449) and `favour` (HB 2527) got through. The regex is now a raw literal, verified against a
known-bad string before use; batch-03 re-checked clean and batch-04's two were fixed by
re-judging and re-importing, which rewrote **127 records in place**. Ledger:
`import-spelling-fix-report.json`. The original insert ledger is untouched.

The lesson is general: **a checker that never fires is indistinguishable from a checker that
passes.** Any assertion helper should itself be tested against a string it must reject.

## Import

| run | stamp | result |
| --- | --- | --- |
| dry run | `2026-09-05T04:16:50.100Z` | 1,294 planned inserts, 0 errors |
| real run | `2026-09-05T04:16:59.126Z` | **1,294 inserts**, 0 errors, 0 notified |
| spelling fix | `2026-09-05T04:20:01.869Z` | 127 rewrites, 1,167 unchanged |
| convergence re-run | — | all 1,294 `unchanged` |

Reconciled three ways: Arizona's live roll-call records moved 2,553 to 3,847, a delta of 1,294;
the run-stamp predicate returns 1,294 records across 54 candidates; the dry run's stamp matches
zero rows.

## Arizona, closed

**3,847 live records across 54 candidates, 2,373 tags, 149 approved roll calls**, over four
batches:

| batch | scope | measures | rolls | records |
| --- | --- | --- | --- | --- |
| batch-01 | signed | 11 | 15 | 408 |
| batch-02 | signed | 21 | 26 | 682 |
| batch-03 | vetoed | 32 | 58 | 1,463 |
| batch-04 | vetoed | 32 | 50 | 1,294 |

Tags reconcile by side arithmetic: 2,373 tags on the 2,373 yes-side records, none on the no-side,
because every nay is `null`. Fourteen research areas are covered, with both directions present
in seven of them.

**Production holds no Arizona roll-call records.**
