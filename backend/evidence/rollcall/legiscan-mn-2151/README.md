# Minnesota roll-call import (LegiScan phase 4)

Source: LegiScan bulk datasets **2151** (94th Legislature, 2025-2026 regular session) and
**2217** (2025 First Special Session, June 9 2025). Both were downloaded from the LegiScan
API on 2026-09-03 and surveyed before anything was written to the registry.

One evidence directory covers both sessions because they share a crosswalk: LegiScan
`people_id` values are stable across sessions, so `crosswalk.json` here serves 2217 as well.
Session 2217's roll evidence lives in this directory's batch folders alongside 2151's, and
the two are told apart by the session number in each evidence file name.

## What the feed holds, and what it does not

| | 2151 (regular) | 2217 (special) |
|---|---|---|
| Bills | 10,590 | 48 |
| Roll calls | 277 | 50 |
| Stored rows | 260 | 49 |
| Kept floor votes | 181 (186 before the seven holds below) | 31 |
| Divided and enacted | 26 rolls / 16 measures | 25 rolls / 14 measures |
| House share of that pool | 5 rolls | 11 rolls |

**⚠ The 2026 session's floor votes are missing from LegiScan.** The 2151 dataset was cut
2026-08-16 and its bill histories carry 11,889 actions from 2026, 292 of which record a bill
passing a chamber. It holds no roll call dated after 2025-05-19. Nothing in this pipeline can
recover those votes; only the 2025 regular session and the June 2025 special session are
importable from this feed. Re-check a later dataset before assuming Minnesota is finished.

Feed health is otherwise the cleanest tier: no committee votes at all (every tally is a
whole-chamber tally, 107-134 in the House and 54-67 in the Senate), no summary-only rolls, no
tally mismatches, no parse errors, and no file errors. 2151 collapsed 4 identity-duplicate
rolls, 2217 none.

## ⚠ The Minnesota House often prints the bill number where the question belongs

43 House rolls read exactly `House: H.F. NO. 2115` or `House: S.F. NO. 2370`. That one caption
covers at least six different questions:

- repassage after the Senate amended the bill,
- adoption of a conference committee report,
- passage after a motion to take the bill from the table,
- a motion to suspend the rules (which needs two thirds, so it divides often),
- a motion to place a bill on the calendar,
- a **failed** motion to adopt a conference report.

HF 2115 is the clean example. Roll 1573202 and roll 1574164 wear the identical caption; the
first is the failed motion to adopt the conference report (67-67) and the second is the
repassage that made it law (124-10). Only that bill's own history, read for that date and that
chamber, separates them. **Never write a description of one of these rolls from its caption.**

Every divided bill-number roll in the regular session was matched to its bill history on
revisor.mn.gov (15 rolls over 9 bills, 2026-09-04). Eight are the chamber's vote on the measure.
Seven are procedural motions, and those seven are pinned in the config's `heldRollCallIds`, so
the fetch stores them with `is_floor_vote` null and nothing can queue or approve them:

| roll | bill | date | tally | what the history says |
|---|---|---|---|---|
| 1545590 | HF 20 | 2025-04-10 | 67-67 | motion to take from table, did not prevail |
| 1571834 | HF 3023 | 2025-05-14 | 70-63 | motion to suspend rules, did not prevail (90 needed); **LegiScan prints `passed: 1`** |
| 1573116 | HF 3023 | 2025-05-18 | 67-67 | motion to take from the table the motion to place on the calendar, did not prevail |
| 1573117 | HF 3023 | 2025-05-18 | 67-67 | same motion, second attempt |
| 1573118 | HF 3023 | 2025-05-18 | 67-67 | same motion, third attempt |
| 1574181 | SF 856 | 2025-05-19 | 65-68 | motion to lay on the table, did not prevail |
| 1574182 | SF 856 | 2025-05-19 | 70-63 | motion to suspend rules, did not prevail (two thirds needed); **LegiScan prints `passed: 1`** |

Five of the seven are stored rows; the fetch had already collapsed 1573117 and 1573118 as
identity duplicates of 1573116 (same tally, same member list), so they were never stored. The
re-fetch that applied the holds is run `rollcall-legiscan-fetch-mn-20260905T013455Z` (5 rows
updated, 255 unchanged; 181 kept floor votes, 6 surfaced: the five holds and the blank
description below).

The unanimous bill-number rolls were not matched because they can never be selected. A later
dataset that adds bill-number rolls needs the same match before any of them is judged.

One roll is deliberately left unmatched: roll 1556487 on HF 2431 has the description `House:`
and nothing more. The history says the House passed the bill 132-0. No pattern can recover a
question from an empty string, and a 132-0 vote is not divided, so it surfaces for review and
costs nothing.

## Crosswalk

`crosswalk.json` holds all 206 people: **164 mapped, 42 explicit nulls.** Validation over every
stored roll returned `no_crosswalk` 0 and 0 file errors in both sessions.

**Refreshed 2026-09-05, after the full November 2026 roster campaign.** The roster now covers all
134 House districts (253 candidates) and all 67 Senate districts (134 candidates), so the resolver
was re-run and the crosswalk went from 48 mapped to 164. The 116 additions break down as:

- **161 proposals, every one reviewed by hand and accepted.** 154 agree on the seat outright.
  The other seven are sitting representatives moving up to the Senate district their own House
  district nests inside, which is mechanical corroboration in Minnesota: Mike Wiener (HD-005B to
  SD-005), Bernie Perryman (HD-014A to SD-014), Steven Jacob (HD-020B to SD-020), Tom Dippel
  (HD-041B to SD-041), Mike Freiberg (HD-043B to SD-043), Liz Reyer (HD-052A to SD-052) and
  Ben Bakeberg (HD-054B to SD-054). Four of the 161 matched on a weaker first-name prefix but on
  the same seat and were accepted: Ronald/Ron Latz, Joshua/Josh Heintzeman, Cal/Calvin Bahr and
  Pete/Peter Johnson.
- **2 more hand-added members of the nickname class**, alongside Scott Dibble below. LegiScan
  carries the legal first name and the ballot carries the working name, and the proposer reads
  neither: **Alicia Kozlowski, House District 8B**, who appears on the 2026 ballot as Liish
  Kozlowski, and **Andrew Smith, House District 25B**, who appears as Andy Smith. Both are on
  their own seat.
- **6 surname collisions rejected on the seat**: Carla Nelson, Tou Xiong, Nicole Mitchell,
  Patti Anderson, Bruce Anderson and James Carlson each share a surname with a 2026 candidate on
  a different seat.
- **The 42 remaining nulls are sitting members who are not on the November ballot at all.** Each
  was checked by hand against the Secretary of State general-election roster for its own seat and
  against every other 2026 seat, and each carries that reason in the file.

⚠ Writing the November general profiles created a second candidate row for 45 Senate members who
already had a row from the August 11 primary roster, because those primary rows carry no hard
identifier for the writer to match on. All 45 were merged with the earlier primary row as the
survivor before the crosswalk was refreshed, so the ids in this file are the surviving rows and
the records written before this campaign stayed with them.

The proposer was run with `--scope-from 2026-08-01` so that members whose only stored 2026
contest is the August 11 primary were reviewed here rather than left unexamined. Whether they
receive records is the importer's own `--scope-from` decision, which this file does not make.

- **47 proposals, all accepted.** 40 agreed on the seat outright. The other 7 are the seven
  sitting representatives on a 2026 ballot: five are running for the Senate district that
  contains their own House district (Minnesota nests House districts nnA and nnB inside Senate
  district nn, so the corroboration is mechanical), and two are running for their own House
  seat, where the two sources only spell the district differently (`HD-006B` against
  `State House District 6B`).
- **1 hand-added member the proposer cannot reach: Scott Dibble, Senate District 61.** LegiScan
  files him as `David Dibble` — the legal first name in `first_name`, the working name `Scott`
  in `nickname` — and the proposer reads neither `name` nor `nickname`. The seat agrees. This
  is the same class that produced hand-adds in Pennsylvania, Connecticut, North Carolina,
  Kentucky and Indiana.
- **Five surname matches were rejected as different people**, each settled by the seat: Mary
  Carlson (SD-11) is not James Carlson (SD-52); Nat Smith (SD-33) is not Andrew Smith
  (HD-025B); Rick Olson (SD-54) is not Bjorn Olson (HD-022A); Angela Nelson (SD-35) is neither
  Carla Nelson (SD-24) nor Nathan Nelson (HD-011B); Cherie Johnson (SD-26) is neither Pete nor
  Wayne Johnson.
- Jason Rarick is filed by LegiScan with `role` "Rep" and `district` "SD-011". He is a senator.
  The seat logic reads `district` and never `role`, which is why he matched correctly.
- Resolving 2217 reports 5 `crosswalkPeopleNotInSnapshot`. That is expected, not a defect: the
  special session's people file holds 201 people against the regular session's 206, because
  five members were replaced mid-term.

**Fan-out was small, and our roster was the reason. That is now fixed.** Before the roster
campaign, at the pipeline's default November-2026 scope, a Senate roll reached 22 candidates and a
House roll reached 3, because the roster held only 30 of 67 Senate districts and 1 of 134 House
districts. After the campaign the roster holds every district in both chambers, and the same three
rolls reach **113 and 112 candidates in the House and 44 in the Senate**. The re-import added no
duplicates: every record written before the campaign came back `unchanged`.

## Batches

`batch-01/` holds the whole campaign: **2 measures, 3 rolls, 269 records, 157 candidates**, with
`PLAN.md` recording a disposition for every one of the 30 gated measures and `JUDGING.md`
recording the reading. Twenty-eight measures are dropped, most of them biennial budget acts and
seven of them because their 2025 vote was cast on text a 2026 conference committee replaced.

## Re-import after the roster campaign (2026-09-05)

The same `batch-01/` evidence was re-imported once per session against the refreshed crosswalk.
Nothing about the judgments changed; only the set of members the rolls could reach.

| run | plan | live | result |
|---|---|---|---|
| `--state MN` (2151) | 110 inserts, 3 unchanged | 110 inserts, 3 unchanged | 113 records on SF 2200 |
| `--state MN-2217` (2217) | 131 inserts, 25 unchanged | 131 inserts, 25 unchanged | 156 records on HF 1 |

The three counts reconcile: the dry runs planned 241 inserts, the live runs performed 241, and the
database moved from **28 records across 25 candidates to 269 records across 157 candidates**
(28 + 241 = 269) for `origin_run_id like 'rollcall:MN:%'`. Convergence dry runs afterwards reported
`unchanged` for all 113 and all 156 rows and wrote nothing.

Of the 269 records, 236 carry a research-area tag. The 33 without one are the nay side of SF 2200,
whose nay stance the committed judgment sets to null on purpose.

## Layout

- `survey/` — the description histogram both sessions were configured from, and
  `divided-enacted-worklist.tsv`, which carries a disposition and a reason for all 51 gated
  rolls across all 30 measures.
- `batch-01/` — judgments, roll evidence, and the import ledgers. The original ledgers from the
  first import are `import-2151-report.json`, `import-2217-report.json` and their two convergence
  files. The re-import after the roster campaign has its own set, named for that run:
  `import-2151-roster-link-report.json` and `import-2217-roster-link-report.json` for the live runs,
  `import-2217-roster-link-dry-run-report.json` for the plan, and the two
  `*-roster-link-convergence-report.json` files for the confirming dry runs.
- `crosswalk.json`, `legiscan-people-mn-2151.json` — the identity review.
  `crosswalk-proposals-report.json` is the FIRST resolver run (47 proposals, before the roster
  campaign) and is kept for history. `resolve-report.json` is the 2026-09-05 re-run against the
  full roster (161 proposals) and is the one the current crosswalk was built from.

Datasets and full evidence live outside the repository at `/Users/shu/legiscan-data/mn-2151*`
and `/Users/shu/legiscan-data/mn-2217*`.
