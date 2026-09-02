# Indiana 2025 batch-05 — judging

Every measure was judged from its enrolled act read in full. No AI provider was called.

## Version checks

| Roll | Text voted on | Result |
| --- | --- | --- |
| SB 140 Senate 39-10 | the conference report itself | is the enacted text |
| HB 1634 Senate 38-11 | Senate committee text, 2025-04-03 | no operative difference |
| SB 409 Senate 30-19 | Senate engrossed text, 2025-02-19 | no operative difference |
| HB 1037 Senate 31-18 | Senate committee text, 2025-04-08 | no operative difference |
| HB 1037 House 68-20 | the concurrence itself | is the enacted text |

SB 409 is the one worth naming. Its only roll is from February and the House printed two later
versions, which is the shape of a version trap. The comparison found the differences were the
cover page and the running footer only, so the roll stands. HB 1460 was the mirror case and its
measure was lost; that is written up in PLAN.md.

## Member lists

**All five rolls were verified name by name against Indiana's own roll-call PDF**, and every one
matched the journal exactly.

| Roll | LegiScan | Official | |
| --- | --- | --- | --- |
| SB 140 Senate, journal 523 | 39-10 | 39-10 | match |
| HB 1634 Senate, journal 367 | 38-11 | 38-11 | match |
| SB 409 Senate, journal 191 | 30-19 | 30-19 | match |
| HB 1037 Senate, journal 425 | 31-18 | 31-18 | match |
| HB 1037 House, journal 479 | 68-20 | 68-20 | match |

HB 1037's own House third reading of 2025-02-04 was already flagged in the worklist and is not
used; the concurrence carries the measure instead.

## Writing checks

- `listPlainLanguageWarnings`: **0 warnings over 10 descriptions**.
- Reading level: mean sentence 13.5 words, longest 33, **Flesch-Kincaid grade 6.5**. Per measure
  4.7 to 7.8.
- Each roll's own tally appears in both its yes and its no sentence.
- Body and closing sentence joined with a period; `", The "` appears nowhere.
- Every label carries `nay: null`.

## Import ledger

| | |
| --- | --- |
| Files | 5, all `imported`, 0 errors |
| Planned inserts (dry run) | 127 |
| Actual inserts | 127 |
| Notifications | 0 |

Dry run, real run and the database agree on 127 rows, and the convergence run reported all 127
`unchanged`.

The 2025 session now holds **947 records**, and Indiana across both sessions holds **2,274
records over 104 candidates with 1,833 area tags**. Production still holds no Indiana records.
