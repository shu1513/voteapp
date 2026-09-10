# Wisconsin, LegiScan session 2197 (2025-2026 Regular Session)

Wisconsin files both years of its two-year session in one dataset, so this is the
whole session and there is no second key to register. Dataset cut 2026-06-28,
downloaded and surveyed 2026-09-09.

Everything here was written against the local database only. Production holds no
Wisconsin roll-call records.

## The dataset

2,749 bills, 572 roll calls, 141 people, for 99 Assembly seats and 33 Senate
seats.

The archive was downloaded through the LegiScan bulk API rather than by hand.
**Verify it against the `hash.md5` file inside the archive, not against the ZIP.**
LegiScan rebuilds the ZIP on every request, so the file timestamps differ and the
ZIP's own md5 never matches the published `dataset_hash`. The byte length matches
exactly, and the internal `hash.md5` matches `dataset_hash`
(`5edcb7acfb122db6acfb483715465206`).

## What makes Wisconsin different

**It records very few votes.** 572 recorded roll calls across 2,749 bills over two
years. Most Wisconsin bills pass on a voice vote, with no tally and no member
list, so they can never become a candidate record. This limits the state more than
its divided government does, and no configuration change can lift it.

**There are no committee roll calls at all.** Every roll's total is the whole
chamber. Wisconsin does publish committee votes, but only as prose in the bill
history (`Report passage recommended by Committee on Ways and Means, Ayes 7, Noes
3`). West Virginia is the only other state in this campaign like that.

**The largest procedural class is an appeal of a ruling from the chair**, 62 rolls.
The minority offers a substitute amendment, the chair rules it out of order as not
germane, the minority appeals, and the chamber votes on whether the chair was
right. Those votes divide on party lines and look like rich material, but they are
about whether an amendment may be considered at all, never about the bill.

**Wisconsin pairs votes.** The history line reads `..., Ayes 53, Noes 42, Paired
2`. A pair is two members on opposite sides who agree to both withhold their
votes. They are neither absent nor undecided, and LegiScan files them as not
voting.

**No veto override roll exists in the whole two-year session.** Wisconsin needs two
thirds, and the legislature never reached it against this governor.

## The pool, measured before any batch was promised

A vote is closely divided when the smaller side is at least a quarter of the
larger.

379 of the 572 rolls are passage questions, and 217 of those are closely divided.
Counting only real bills, which are the only measures that can become law:

| outcome | roll calls | measures |
| --- | --- | --- |
| became law | 24 | 23 |
| vetoed by the governor | 126 | 75 |
| died without becoming law | 49 | 47 |

The vetoed group being five times the size of the enacted one is Wisconsin's
divided government showing up in the data. The operator chose to work both scopes.

## Batches

| batch | scope | measures | rolls | records |
| --- | --- | --- | --- | --- |
| batch-01 | the whole enacted pool | 15 kept of 24 | 16 | 706 |
| batch-02 | vetoed pool, education strand | 9 kept of 13 | 15 | 839 |
| batch-03 | vetoed pool, crime and courts | 10 kept of 10 | 18 | 878 |
| batch-04 | vetoed pool, immigration and foreign adversaries | 9 kept of 10 | 14 | 835 |

The vetoed pool holds 74 measures on 122 closely divided rolls. Batches 02, 03
and 04 read 33 of them, leaving 41 measures on 68 rolls. The remaining strands,
for later batches: health, gender and civil rights; labor, unemployment
insurance and taxes; and elections, environment and state government operations.

Nine of the passage rolls are the first chamber accepting the other chamber's
amendment (`Senate: Assembly Substitute Amendment 7 concurred in`). When the
second chamber `concurred in as amended`, the bill goes back and the first chamber
votes on the changed text; that vote, not its earlier passage roll, is its last
word on the bill. SB 622 is the example: Senate 22-11, Assembly substituted, Senate
accepted the substitute 20-13. The first cut of the config excluded these under the
blanket numbered-amendment rule, which also hid them from the superseded-stage gate.

Joint and senate resolutions add 18 more closely divided rolls. An adopted joint
resolution is not law — it goes to a second consecutive legislature and then to the
voters — even though LegiScan marks it status 4. Of the 315 status-4 measures in
this session, 246 were approved by the governor and 69 are resolutions that were
only adopted.

## The audit

Wisconsin prints its own tally in its bill history, so every roll can be checked
without a single network call. `tools/wi_audit.py` does it.

All 572 rolls were audited, not only the closely divided ones, because a wrong
tally can itself decide whether a roll looks divided. Result: **566 exact, 4
wrong, 2 with no matching history line.**

The 4 wrong ones are all the same shape — LegiScan files one member's recorded
vote as not voting, so the stored tally is short by one. All 4 are in
`heldRollCallIds`, along with a fifth roll whose caption names a question
Wisconsin took no recorded vote on. None of the five changes whether its roll is
closely divided, but each would put a wrong number in a candidate's record, and
every description is required to cite its own roll's tally.

Of the 2 with no matching history line, one is a caption variant on the budget
bill, where the history says plainly `Passed, Ayes 19, Noes 14` while the roll
says `Read a third time and passed`. The tallies agree. The other is the held
roll.

## Two bills became law with a partial veto

Wisconsin's governor can veto parts of a bill. The history wording is `Report
approved by the Governor with partial veto on 4-8-2026. 2025 Wisconsin Act 203` —
not "approved in part" and not "vetoed in part", so a check written from another
state's vocabulary misses it. That happened on the first pass here, and
`wi_audit.py` itself carried the wrong vocabulary until review caught it: it
counted all 246 approved bills as approved whole. It now matches Wisconsin's
wording and prints both partial vetoes.

**AB 1034 and AB 650** are the two, and AB 1034 sits inside the closely divided
enacted pool. In both cases the legislature tried to override the partial veto and
failed. So what became law is not the bill the chambers voted on, and any
description has to be written from the published Act and say what the partial veto
removed.

## Still to check before judging

- Wisconsin uses substitute amendments that replace a whole bill. Verify which
  text each chamber voted before writing anything.
- Whether Wisconsin marks deleted and added text in a way `pdftotext` throws away.
  Not yet determined, and nearly every state does.
