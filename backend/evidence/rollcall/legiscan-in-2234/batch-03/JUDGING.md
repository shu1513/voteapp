# Indiana 2026 batch-03 — judging

Every measure was judged from its enrolled act read in full. No AI provider was called.

## Version checks

| Roll | Text voted on | Result |
| --- | --- | --- |
| HB 1001 House 72-21 | the concurrence itself | is the enacted text |
| HB 1001 Senate 35-13 | Senate engrossed text, 2026-02-23 | no operative difference |
| HB 1003 House 69-28 | the conference report itself | is the enacted text |
| SB 277 House 53-45 | House engrossed text, 2026-02-23 | no operative difference |
| SB 277 Senate 26-21 | the concurrence itself | is the enacted text |

SB 277 is worth a note on method. Its comparison flagged 299 differing runs, which on a
299-section act looks alarming. Grouping the runs by content showed 136 of them were the single
running footer `ES 277 - LS 6849/DI 150` becoming `SEA 277 - Concur`, and the rest were the cover
page and a handful of stray page numbers. A raw run count is not evidence of a policy change;
the runs have to be read.

## Member lists

**All five rolls were verified name by name against Indiana's own roll-call PDF**, and every one
matched the journal exactly.

| Roll | LegiScan | Official | |
| --- | --- | --- | --- |
| HB 1001 House, journal 372 | 72-21 | 72-21 | match |
| HB 1001 Senate, journal 223 | 35-13 | 35-13 | match |
| HB 1003 House, journal 422 | 69-28 | 69-28 | match |
| SB 277 House, journal 356 | 53-45 | 53-45 | match |
| SB 277 Senate, journal 296 | 26-21 | 26-21 | match |

## Writing checks

- `listPlainLanguageWarnings`: **0 warnings over 10 descriptions**.
- Reading level: mean sentence 11.9 words, longest 21, **Flesch-Kincaid grade 6.7**. Per measure
  6.2 to 7.4.
- Each roll's own tally appears in both its yes and its no sentence.
- Body and closing sentence joined with a period; `", The "` appears nowhere.
- Every label carries `nay: null`. HB 1001 carries two labels pointing opposite ways.

## Import ledger

| | |
| --- | --- |
| Files | 5, all `imported`, 0 errors |
| Planned inserts (dry run) | 286 |
| Actual inserts | 286 |
| Notifications | 0 |

Dry run, real run and the database agree on 286 rows, and the convergence run reported all 286
`unchanged`.

Indiana's 2026 session now holds **1,327 records across 104 candidates with 1,078 area tags**,
over seventeen measures and twenty-four rolls. Across both sessions Indiana holds **2,147
records**. Production still holds no Indiana records.
