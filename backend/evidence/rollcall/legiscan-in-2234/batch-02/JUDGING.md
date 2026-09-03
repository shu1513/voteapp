# Indiana 2026 batch-02 — judging

Every measure was judged from its enrolled act read in full. No AI provider was called.

## Version checks

Each roll was compared word for word against the enrolled act, using the dated version stack
in the LegiScan bill JSON.

| Roll | Text voted on | Result |
| --- | --- | --- |
| SB 78 Senate 36-12 | the concurrence itself | is the enacted text |
| HB 1355 House 73-22 | the concurrence itself | is the enacted text |
| HB 1424 Senate 38-10 | Senate committee text, 2026-02-19 | no operative difference |
| HB 1273 House 72-20 | the concurrence itself | is the enacted text |
| SB 76 House 61-28 | House engrossed text, 2026-02-12 | identical, zero differences |
| SB 76 Senate 37-11 | the concurrence itself | is the enacted text |
| HB 1150 House 72-19 | the concurrence itself | is the enacted text |
| HB 1150 Senate 37-10 | Senate engrossed text, 2026-02-09 | no operative difference |
| HB 1368 House 57-40 | the concurrence itself | is the enacted text |
| HB 1368 Senate 29-19 | Senate engrossed text, 2026-02-24 | one full stop in a section number |
| SB 270 Senate 34-15 | House engrossed text | identical across 9,992 tokens |

Five rolls that did show a real difference were dropped and are written up in PLAN.md.

The comparison tool was corrected during this batch. It now strips committee reports and floor
motions appended after the bill body, which the printer includes in every engrossed version.
Before that fix a bill whose committee report happened to be long looked like a large policy
change. HB 1150 and HB 1424 were both re-checked after the fix, and both cleared.

## Member lists

**All eleven rolls were verified name by name against Indiana's own roll-call PDF.** Every one
matched the journal exactly, on the count and on the names on each side.

| Roll | LegiScan | Official | |
| --- | --- | --- | --- |
| SB 78 Senate, journal 291 | 36-12 | 36-12 | match |
| HB 1355 House, journal 299 | 73-22 | 73-22 | match |
| HB 1424 Senate, journal 263 | 38-10 | 38-10 | match |
| HB 1273 House, journal 368 | 72-20 | 72-20 | match |
| SB 76 House, journal 240 | 61-28 | 61-28 | match |
| SB 76 Senate, journal 271 | 37-11 | 37-11 | match |
| HB 1150 House, journal 275 | 72-19 | 72-19 | match |
| HB 1150 Senate, journal 158 | 37-10 | 37-10 | match |
| HB 1368 House, journal 420 | 57-40 | 57-40 | match |
| HB 1368 Senate, journal 259 | 29-19 | 29-19 | match |
| SB 270 Senate, journal 301 | 34-15 | 34-15 | match |

Separately, the five rolls the worklist had flagged were checked and **all five failed**. That
result is in PLAN.md and in the campaign README.

## Writing checks

- `listPlainLanguageWarnings`: **0 warnings over 22 descriptions**.
- Reading level measured separately: mean sentence 12.3 words, longest 25,
  **Flesch-Kincaid grade 6.8**. Per measure the range is 5.9 to 7.6.
- Each roll's own tally appears in both its yes and its no sentence.
- Body and closing sentence joined with a period; `", The "` appears nowhere.
- Every label carries `nay: null`.
- Two rolls carry two labels each, pointing opposite ways, because HB 1150 does two unrelated
  things. That is the multi-label-per-strand rule, not a hedge.

**A date error the judge caught.** Two judgments carried the roll's dataset date rather than
the stored vote date, on HB 1273 and SB 270. `rollcall:judge` refused the file and named both.
Every judgment's date was then reconciled against the stored rows before re-running. This is
the second time the judge's own cross-check has caught an authoring slip that reading alone
did not.

## Import ledger

| | |
| --- | --- |
| Files | 11, all `imported`, 0 errors |
| Planned inserts (dry run) | 493 |
| Actual inserts | 493 |
| Candidates | 104 |
| Area tags written | 430 |
| Notifications | 0 |

Dry run, real run and the database all agree on 493 rows.

**One convergence run reported a transient error.** A first re-run returned 10 files imported,
one error and 410 unchanged. A repeat immediately afterwards returned all 11 imported and all
493 unchanged, and the error text was not preserved, because the importer overwrites its
re-run report. Rather than assume, every roll's row count was compared against the insert
ledger directly in the database: **all nineteen rolls across batches 01 and 02 match their
ledger exactly**, and the 83 rows the failed run did not reach belong to SB 76's House roll,
which holds exactly 83. Nothing is missing. The likely cause is a transient database failure
inside that roll's transaction, which is the unit the importer commits in.

Indiana now holds **1,861 live roll-call records across 104 candidates with 1,474 area tags**,
over twenty-three measures and twenty-seven rolls across both sessions. Production still holds
no Indiana records.
