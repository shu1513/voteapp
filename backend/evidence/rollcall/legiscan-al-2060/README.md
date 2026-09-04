# Alabama roll-call votes — LegiScan session 2060 (2023 Second Special Session)

Alabama's July 2023 special session, called to redraw the congressional map after the Supreme Court
decided *Allen v. Milligan*. Registered as `AL-2060`, sharing the 2023 vocabulary definition with the
2023 regular session.

The dataset holds 39 bills, 26 roll calls and 138 people. The dataset and the 21 stored roll-call
evidence files live outside this repository at `/Users/shu/legiscan-data/al-2060/` and
`/Users/shu/legiscan-data/al-2060-evidence/`.

## Feed health

Clean: no repeated roll call ids, no summary-only rolls, no tally mismatches, no committee votes, no
parse errors, and no roll call id collides with any other Alabama session. Fetch stored 21 rows
(26 minus 2 on excluded instrument types and 3 identity duplicates), with nothing left unclassified.
All 14 surveyed description families are already covered by the 2023 definitions, which is why the
registry entry adds no rules of its own.

## Pool

10 kept floor votes, 4 divided, and 2 of those on the one measure that became law.

| Measure | Roll | Vote | Question |
|---|---|---|---|
| SB 5 congressional reapportionment | Senate 1350863, 2023-07-19 | 24-8 | passage |
| SB 5 | Senate 1350905, 2023-07-21 | 24-6 | concurrence in the House changes |
| HB 5 congressional reapportionment (died) | House 1350857, 2023-07-19 | 74-27 | passage |
| HB 5 | Senate 1350910, 2023-07-21 | 23-7 | passage as amended |

SB 5 became Act 2023-563 — the map the 2026 special session's HB 1 later refers to by name. HB 5 was
the House's competing map and died.

## Identity

The crosswalk holds 138 entries: 134 carried over unchanged from the modern Alabama crosswalks and 4
explicit nulls (David Cole, John Rogers, Greg Reed and Clay Scofield, none of them on the November
2026 ballot). Resolution over all 21 stored rolls: 931 matched, no member without a crosswalk entry,
none out of scope, no file errors. Fan-out is a median of 86 mapped candidates per House roll and 24
per Senate roll.
