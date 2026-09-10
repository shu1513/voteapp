# Utah — findings recorded, not fixed

## 1. Two rolls carry one vote's member list twice (2025)

LegiScan roll calls 1514013 and 1514014 are both HB 550, Senate, 2025-03-07,
23-6, with byte-identical member lists, so the fetcher's identity check kept
the lower id and counted the other as a duplicate. Utah's own record shows two
real votes that evening: 23-6 at 10:03 PM on Substitute 0, then a motion to
reconsider, then a substitution, then 23-6 again at 10:13 PM on Substitute 1,
which is the text that was enrolled.

The same 23 senators voted yes both times, so nothing is misattributed, but any
description of the stored roll must be written as the second vote, on the
enrolled substitute. HB 550 is not in batch-01.

## 2. Four 2026 rolls contradict Utah's own vote sheets

Held by id on the `UT-2214` config entry, so they are stored and surfaced but
can never be approved. Found by the tally audit and confirmed against the
per-roll vote sheet, which names every member.

| roll | measure | LegiScan | Utah | what is wrong |
| --- | --- | --- | --- | --- |
| 1653739 | HB 502 Senate | 21-8 | 20-9 | Karen Kwan on the wrong side |
| 1654810 | HB 270 Senate | 19-1 | 18-2 | Todd Weiler on the wrong side |
| 1656396 | SB 62 Senate | 21-7 | 28-0 | a different vote's member list |
| 1652433 | SB 152 House | 65-7 | 70-1 | a different vote's member list |

The 2025 session has no such roll.

## 3. LegiScan's `passed` flag does not know Utah's two-thirds rule

Passing a bill on second and third reading at once requires suspending the
rules, which takes two thirds of the chamber. LegiScan's flag is a bare
majority check, so HB 212 (2026) is marked passed at 17-12 while Utah's own
history records `Senate/ failed`. Nothing in the pipeline reads `result`, and
selection reads each bill's history instead, so no roll is affected.

## 4. Two enacted bills sit at status 3

Utah records `Became Law w/o Governor Signature` in the bill history and
LegiScan leaves those bills at status 3, so an enacted-status filter misses
them: HB 77 in 2025 and HB 195 in 2026, both divided. Selection reads the
history's governor action rather than `status`, and any description of one must
say the bill became law without the Governor's signature.

## 5. The action timestamp format changes between sessions

Utah's per-bill JSON prints `1/29/2025 11:27 AM` in the 2025 session and
`2026-02-05 15:37:38.270` in the 2026 session, on the same site. Any tool that
reads the action trail must accept both.

## 6. A committee report adoption carries no substitute number

The floor action `comm rpt/ substituted` has an empty `parm1`; the substitute
number sits on the preceding `Comm - Substitute Recommendation`. A floor
`substituted` and a conference substitution do carry `parm1` as the new number
and `parm2` as the one replaced, and `substitute adoption failed` carries the
number that was NOT adopted. Reading only the floor actions puts a vote on the
wrong substitute.

## 7. Utah's official summary can describe a bill the act does not contain

The Office of Legislative Research and General Counsel's highlighted provisions
for SB 86 (2025) say the act "reduces the number of employees a person may
employ before being considered an employer" under the Antidiscrimination Act.
The enrolled text strikes "15" and re-inserts "15": the threshold does not
move. The only change the act makes is to the definition of sexual harassment.
The summary is an index, never the source.
