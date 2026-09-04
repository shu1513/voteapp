# Indiana 2025 batch-06 — judging

Both measures were judged from their enrolled acts read in full. No AI provider was called.

## Version checks

| Roll | Text voted on | Result |
| --- | --- | --- |
| SB 365 House 66-24 | House engrossed text, 2025-04-01 | no operative difference |
| HB 1079 House 64-21 | the concurrence itself | is the enacted text |

Two measures failed this check and are written up in PLAN.md. HB 1144's Senate roll predates a
conference committee that removed two whole provisions. SB 331's roll passed the version check
but failed a different one.

## The judge's decisive-vote guard

`rollcall:judge` refused the first version of this batch, naming SB 331's House conference
committee report of 2025-04-22 as a later kept floor vote on the same measure in the same
chamber. That is a check no reading of the bill text would have caught, and it was correct: the
tallies differ, 72-22 on the third reading and 73-17 on the conference report, so five members
moved. The judgment was withdrawn rather than acknowledged past the guard. **This is the third
time in the campaign that a tool's own cross-check has caught something the reading did not.**

## Member lists

Both rolls were verified name by name against Indiana's own roll-call PDF, and both matched.

| Roll | LegiScan | Official | |
| --- | --- | --- | --- |
| SB 365 House, journal 358 | 66-24 | 66-24 | match |
| HB 1079 House, journal 462 | 64-21 | 64-21 | match |

SB 331's roll was also verified and matched, before the guard removed it for a different reason.

## Writing checks

- `listPlainLanguageWarnings`: **0 warnings**.
- Reading level over the batch as authored, including the SB 331 descriptions that were then
  withdrawn: mean sentence 14.0 words, longest 23, **Flesch-Kincaid grade 7.0**.
- Each roll's own tally appears in both its yes and its no sentence.
- Body and closing sentence joined with a period; `", The "` appears nowhere.
- Both labels carry `nay: null`.

## Import ledger

| | |
| --- | --- |
| Files | 2, all `imported`, 0 errors |
| Planned inserts (dry run) | 158 |
| Actual inserts | 158 |
| Notifications | 0 |

Dry run, real run and the database agree on 158 rows, and the convergence run reported all 158
`unchanged`.

An earlier import attempt in this batch reported `not_approved` on all three files, which is the
importer behaving correctly: the judge had refused the file, so no row was approved and nothing
was written. The database was never touched by that attempt.

The 2025 session now holds **1,105 records**, and Indiana across both sessions holds **2,432
records over 104 candidates with 1,951 area tags**. Production still holds no Indiana records.
