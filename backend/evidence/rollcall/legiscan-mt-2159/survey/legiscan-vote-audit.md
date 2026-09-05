# Checking LegiScan's Montana roll calls against Montana's own record

Run in batch-07, after one member's vote on SB 542 turned out to be wrong.
**Corrected after review**: the first version of this audit paired votes by
date and nearest tally, which matched the wrong motion on two HB 231 days and
reported disagreements that did not exist. The pairing now uses member
agreement. The numbers below are from the corrected run.

## What was compared

Every stored floor roll on all 335 bills in `divided-enacted-worklist.tsv` —
**1,826 roll calls** — was compared member by member against Montana's own vote
record.

Montana publishes it without a key:

    https://api.legmt.gov/bills/v1/votes/findByBillId?billId=<id>

The `billId` is the `id` field of the bill record already used for chapter
numbers, from
`findBySessionIdAndDraftNumber?sessionId=2&draftNumber=<LC>`. Each vote carries
a `legislatorVotes` array of legislator ids and vote types, which resolve
against the official roster at
`/Users/shu/legiscan-data/mt-legmt-legislators.json`. Vote types seen are `YES`,
`NO`, `ABSENT`, `EXCUSED`, `YES_EXCUSED` and `NO_EXCUSED`; `YES_EXCUSED` and
`NO_EXCUSED` are votes and count as such, `EXCUSED` and `ABSENT` are not.

## How a LegiScan roll is paired with a Montana vote

This is the part that went wrong first time, so it is spelled out.

LegiScan does not name the motion, only the stage ("2nd Reading Motion to
Amend Failed"). Montana names it ("AMD-HB0231.004.008 Dunwell D/PASS"). So the
two cannot be joined on the motion. Nor can they be joined on the tally: on
2025-04-22 the Senate took two votes on HB 231 amendments that both went 24-26.
Nor on the order taken: LegiScan's roll numbers are not always chronological
within a day, and on 2025-04-17 they run the opposite way to Montana's.

The one signal that identifies a vote is the members. A correct pairing agrees
on nearly every member; a wrong one agrees on about half. So for each
(date, chamber) on a bill, every way of pairing LegiScan's rolls with Montana's
votes is tried and the pairing with the fewest member disagreements is kept.
When Montana has more votes on a day than LegiScan (it records a cloture vote on
SB 542 that LegiScan omits), the best-agreeing subset is tried. When the two
sides cannot be paired at all, the rolls are reported as **unpaired**, not
guessed at.

A defective roll still shows its disagreements against its best match. What the
method removes is disagreements that were only ever an artefact of comparing two
different votes.

Three members needed an alias because the two sources name them differently:
Julie Dooling / Darling, Robert / Bob Carter, and Sidney / Chip Fitzpatrick.
Every other name resolved on its own, and **no name failed to resolve**.

The scripts live outside the repository with the other Montana helpers:
`/Users/shu/legiscan-data/mt_verify.py` does the comparison and
`mt_prefetch.py` warms the cache of official records.

## The result that matters

**None of the 81 roll calls this campaign had imported when this audit ran
disagrees with Montana's own record.** Every one was in scope, every one paired,
and every one matches member for member. No Montana candidate record is wrong.

Batches 08 and 09 imported 16 more rolls afterwards. Each was compared the same
way, member by member, before it was imported, and each agrees exactly — so all
**97** imported rolls now stand checked. The 1,826-roll sweep below is the
batch-07 run and was not repeated.

## What disagrees

| | Rolls |
| --- | --- |
| Compared | 1,826 |
| Paired with a Montana vote | 1,780 |
| Unpaired (Montana and LegiScan record different numbers of votes that day) | 46 |
| **A member's vote is flipped** — LegiScan says yes, Montana says no, or the reverse | **20** |
| **An excused or absent member is shown as voting** — nearly always as a no | **23** |
| Tally differs | 42 |

The 46 unpaired rolls are all second-reading amendment votes on HB 2 (the
general appropriations bill, on two days) and HB 291 (one day). None is a roll
this campaign would use.

**Flipped votes.** Twenty rolls. In nineteen of them exactly one member is on
the wrong side; HB 2's House third reading of 2025-04-07 has three. Nine of the
twenty are third readings, and eight of those nine are a chamber's last kept
floor vote — the roll this campaign selects:

| Measure | Roll | Date | LegiScan | Montana | Member |
| --- | --- | --- | --- | --- | --- |
| SB 542 | 1556679 | 2025-04-24 | 73-26 | 72-27 | Amy Regier, shown yes, voted no |
| HB 15 | 1481075 | 2025-01-27 | 78-20 | 77-21 | Randyn Gregg, shown yes, voted no |
| HB 76 | 1558903 | 2025-04-28 | 69-31 | 68-32 | Jodee Etchart, shown yes, voted no |
| HB 284 | 1551835 | 2025-04-17 | 58-41 | 59-40 | Melody Cunningham, shown no, voted yes |
| HB 636 | 1508554 | 2025-03-07 | 84-15 | 85-14 | Melody Cunningham, shown no, voted yes |
| HB 888 | 1558107 | 2025-04-25 | 59-41 | 58-42 | Brian Close, shown yes, voted no |
| SB 243 | 1546282 | 2025-04-11 | 77-22 | 76-23 | Scott Rosenzweig, shown yes, voted no |
| SB 342 | 1546349 | 2025-04-11 | 58-41 | 57-42 | SJ Howell, shown yes, voted no |

Seven of these are marked `held:legiscan-vote-defect` in the worklist. HB 636's
roll never entered the worklist, because 84-15 is not divided.

**Excused members shown as voting.** Twenty-three rolls, all second readings.
In twenty-two of them Montana records a member as `EXCUSED` or `ABSENT` and
LegiScan shows them voting no; in the others LegiScan shows a member absent who
Montana records as voting yes. The first pattern is consistent enough to look
like how LegiScan's Montana feed handles an excused member. It matters for this
campaign because an excused member shown as a no would receive a "voted
against" record for a vote they did not cast. None of the twenty-three is a roll
this campaign uses.

**What was retracted.** The first version of this audit claimed 47
disagreements on HB 231's Senate motion of 2025-04-17 and eight on its motion of
2025-04-22, and said thirteen rolls had matching tallies with members still
differing. All of that was the wrong-motion artefact. Correctly paired, the
2025-04-17 motion differs only in three excused members shown as no votes, and
the 2025-04-22 motion does not differ at all. **No paired roll has a matching
tally with a member on the wrong side.** Every flipped vote moves the tally.

## What happens next

The importer verifies the SHA-256 of each roll call payload against the value
approved at fetch time, and separately checks the evidence file's tally against
the approved row. Those guards make a hand correction impossible, which is
right: they exist to stop unreviewed editing of source data.

So an affected roll can only be held. Importing one needs a supported way to
record that an upstream source is wrong about a named member, which is a code
change and belongs in its own review. Until then the eight rolls above stay
held, and each affected measure is carried on its other chamber alone, or not at
all.

`legiscan-vote-audit.tsv` in this directory lists every disagreeing and every
unpaired roll, with the member and the direction of each disagreement.

## Whether this reaches other states

Unknown, and worth checking. The comparison was only possible because Montana
publishes member-level roll calls in a machine-readable form without a key. Any
state that does the same can be audited the same way, and any state already in
the registry that does should be, before its records are promoted to production.
