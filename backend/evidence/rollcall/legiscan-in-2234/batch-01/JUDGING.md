# Indiana 2026 batch-01 — judging

Every measure was judged from its enrolled act read in full. No AI provider was called.

## Version checks

Each roll was compared word for word against the enrolled act, using the version stack in the
LegiScan bill JSON, which carries a dated link for every printed version. That makes the
roll-to-version mapping exact rather than guessed.

| Roll | Text voted on | Result |
| --- | --- | --- |
| SB 284 House 70-25 | House committee text, 2026-02-19 | no operative difference |
| SB 112 House 72-24 | House committee text, 2026-02-05 | no operative difference |
| HB 1274 House 69-25 | House committee text, 2026-01-20 | no operative difference |
| SB 258 House 64-28 | House committee text, 2026-02-03 | no operative difference |
| SB 258 Senate 36-9 | Senate committee text, 2026-01-15 | no operative difference |
| SB 285 House 53-44 | House engrossed text, 2026-02-23 | no operative difference |
| SB 285 Senate 28-22 | the concurrence itself | is the enacted text |
| HB 1193 House 66-29 | the concurrence itself | is the enacted text |

In every case the only differences the comparison found were the cover-page digest, the
running footer, and committee reports appended to the printed version. Two rolls that did
show a real difference were dropped, and are written up in PLAN.md.

HB 1274 is worth singling out. Its House vote was on 2026-01-28 and the Senate printed a
later version on 2026-02-12, which is exactly the shape of a version trap. The comparison
found the Senate's version operatively identical, so the House roll stands.

## Member lists

**All eight rolls were verified name by name against Indiana's own roll-call PDF**, the step
this state's LegiScan defect requires. Every one matched the journal exactly, on both the
count and the names on each side.

| Roll | LegiScan | Official | |
| --- | --- | --- | --- |
| SB 284 House, journal 358 | 70-25 | 70-25 | match |
| SB 112 House, journal 234 | 72-24 | 72-24 | match |
| HB 1274 House, journal 129 | 69-25 | 69-25 | match |
| SB 258 House, journal 211 | 64-28 | 64-28 | match |
| SB 258 Senate, journal 61 | 36-9 | 36-9 | match |
| SB 285 House, journal 359 | 53-44 | 53-44 | match |
| SB 285 Senate, journal 303 | 28-22 | 28-22 | match |
| HB 1193 House, journal 395 | 66-29 | 66-29 | match |

## Writing checks

- `listPlainLanguageWarnings`: **0 warnings over 16 descriptions**.
- Reading level measured separately: mean sentence 13.5 words, longest 23,
  **Flesch-Kincaid grade 6.9**. Per measure the range is 6.2 to 7.7. This clears the
  seventh-grade target, and it was reached by splitting sentences and choosing plainer words,
  not by dropping any statutory limit or counter-strand.
- Each roll's own tally appears in both its yes and its no sentence.
- Body and closing sentence joined with a period; `", The "` appears nowhere.
- Every label carries `nay: null`, so no voters carry no tag. The reasoning is in PLAN.md.

## Import ledger

| | |
| --- | --- |
| Files | 8, all `imported`, 0 errors |
| Planned inserts (dry run) | 548 |
| Actual inserts | 548 |
| Candidates | 104 |
| Area tags written | 378 |
| Notifications | 0 |

Dry run, real run and the database all agree on 548 rows across 104 candidates. The
convergence run afterwards reported all 548 `unchanged`, so the import is idempotent and the
original ledger is untouched. The 378 tags against 548 records is the expected shape: every
label sets `nay: null`, so only the yes side of each roll carries an area tag.

Indiana now holds **1,368 live roll-call records across 104 candidates with 1,044 area tags**,
over fifteen measures and twenty-four rolls across both sessions. Production still holds no
Indiana records.
