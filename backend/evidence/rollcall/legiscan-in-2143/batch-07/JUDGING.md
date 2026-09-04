# Indiana 2025 batch-07 — judging

Both measures were judged from their enrolled acts read in full. No AI provider was called.

## Version checks

Both enrolled acts came out of a conference committee, so each chamber's final vote was on the
conference report, and the conference report is the enacted text by definition.

| Roll | Text voted on | Result |
| --- | --- | --- |
| HB 1003 House 67-25 | the conference report itself | is the enacted text |
| HB 1003 Senate 30-20 | the conference report itself | is the enacted text |
| HB 1004 Senate 37-13 | the conference report itself | is the enacted text |

The earlier readings were checked and rejected. HB 1003's House third reading of 2025-02-18
sits at 0.70 similarity to the enrolled act across 339 differing runs. HB 1004's Senate third
reading of 2025-04-15 sits at 0.89 across 355 runs. Neither is used.

## Member lists

The three kept rolls were verified name by name against Indiana's own roll-call PDF and all
three matched.

| Roll | LegiScan | Official | |
| --- | --- | --- | --- |
| HB 1003 House, journal 544 | 67-25 | 67-25 | match |
| HB 1003 Senate, journal 513 | 30-20 | 30-20 | match |
| HB 1004 Senate, journal 528 | 37-13 | 37-13 | match |

A fourth roll was checked and rejected. HB 1004's House conference report, journal 566, reports
67-23 in LegiScan against 68-23 in the journal, and the missing member is Representative Behning
on the yea side. That roll is not used, which costs HB 1004 its House fan-out.

## Writing checks

- `listPlainLanguageWarnings`: **0 warnings over 6 descriptions**.
- Reading level: mean sentence 12.5 words, longest 23, **Flesch-Kincaid grade 6.8**.
- Each roll's own tally appears in both its yes and its no sentence.
- Body and closing sentence joined with a period; `", The "` appears nowhere.
- Every label carries `nay: null`.

## Import ledger

| | |
| --- | --- |
| Files | 3, all `imported`, 0 errors |
| Planned inserts (dry run) | 107 |
| Actual inserts | 107 |
| Notifications | 0 |

Dry run, real run and the database agree on 107 rows, and the convergence run reported all 107
`unchanged`.

The 2025 session now holds **1,212 records**, and Indiana across both sessions holds **2,539
records over 104 candidates with 2,027 area tags**. Production still holds no Indiana records.
