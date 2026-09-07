# Missouri batch-04 — the rest of the divided votes that did not become law

58 measures read, **11 kept: 11 rolls / 1,107 records.** With batch-03's five,
every roll in `../survey/divided-not-enacted-worklist.tsv` now carries a
disposition. Session 2169 now holds **2,573 roll-call records** and Missouri as a whole **2,802** across
both sessions, local `voteapp` only.

All eleven passed the House and died in the Senate — the Pennsylvania class.

| measure | roll | area | yes vote |
|---|---|---|---|
| HB 126 ends no-excuse absentee voting | H 85-64 | `civil_rights` | against |
| HB 1175 Second Amendment Preservation Act | H 100-51 | `gun_control` | against |
| HB 1264 thirty-day permit shot clock | H 96-37 | `housing_affordability` | for |
| HB 195 Born-Alive Abortion Survivors Protection Act | H 109-32 | `womens_reproductive_rights` | against |
| HB 269 child care tax credits | H 120-34 | `cost_of_living_reduction` | for |
| HB 344 state preemption of local tobacco rules | H 108-29 | `environment_and_public_health` | against |
| HB 437 settlement demand limits | H 96-52 | `corporate_accountability` | against |
| HB 493 sales tax exemption for used property | H 104-41 | `cost_of_living_reduction` | for |
| HB 794 bars foreign money in ballot-measure campaigns | H 99-48 | `election_integrity` | for |
| HB 875 belief-based student groups on campus | H 108-47 | `civil_rights` | for |
| HB 952 collateral source evidence | H 37-107 | `corporate_accountability` | against |

**HB 952 is a defeat, not a passage** — the House rejected it 37-107, and the
description says so.

## Every roll was audited against the history's own passage tally, and one failed

Missouri's bill history prints the tally in the action line
(`Third Read and Passed (H) - AYES: 100 NOES: 51`), which is what picks the right
roll when a chamber perfects a bill and then reads it a third time on the same
day. Running that check over all sixteen candidates caught one:

**HB 416 (school safety) is not importable.** Its divided roll is 101-47, but the
history records third reading and passage at **112-20**. The 101-47 roll is an
earlier vote that day, so it is not the chamber's vote on the measure. Marked
`screened:not-the-passage-roll`. Without the audit it would have been imported as
though the House split 101-47 on a bill it passed 112-20.

## Four Senate rolls could not be verified, so they were not imported

HB 343, HB 546, HB 758 and HB 958 each carry a Senate roll in the pool, but each
bill's own history ends in House committee and records **no floor passage at
all**. What those Senate votes were taken on cannot be confirmed against the
state record, so they are `screened:unverifiable-question` rather than guessed
at. Three of them (HB 546, HB 758, HB 958) are the minimum-wage and paid-sick-
leave rollbacks, so this is a real loss and worth revisiting with the Senate
journal.

## Reconciliation

| step | records |
|---|---|
| dry run | 1,107 `insert` |
| real run | 1,107 `insert` |
| re-run | 1,107 `unchanged` |
| rows in the local database, stamp `2026-09-07T03:28:31.126Z` | 1,107 |

Median Flesch-Kincaid grade **6.4**, worst **9.1**, no sentence over 45 words,
no British spellings.
