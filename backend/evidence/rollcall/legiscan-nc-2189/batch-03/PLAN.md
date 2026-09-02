# Where batch-03 comes from

Batches 01 and 02 worked the divided votes on bills that became law. That pool
is finished: all 165 rolls carry a disposition.

This batch opens the other pool. A member's vote is a real, recorded position
whether or not the bill survived, so a bill that died still belongs in the
record. Pennsylvania batch-02 took the same scope first.

## The pool

82 divided floor votes on measures that did not become law, across 66 measures.
They split two ways:

- **17 rolls on 6 measures the Governor vetoed.** These are batch-03.
- **65 rolls on 60 measures that passed one chamber and died there.** These are
  left for batch-04 and are marked `candidate:batch-04-unbatched` in
  `../survey/divided-not-enacted-worklist.tsv`.

## How batch-03 narrowed to 7 rolls

Of the 17 vetoed-measure rolls:

- 6 are an earlier stage the chamber voted on again later.
- 4 are 2026 House rolls, which cannot be imported because LegiScan understates
  their tallies. See finding 3 in `../CODE-FINDINGS.md`. They sit in
  `held-rolls/`.
- 7 remain. Every one has a complete member list, and every one was checked
  against the official ncleg.gov transcript. Of those 7, 3 failed the stance
  filter (listed below) and 4 were imported.

## Two votes here are not passage votes

Two of North Carolina's vetoed bills were overridden by one chamber only, so
they are still vetoed and still not law.

- **Senate Bill 50** was overridden by the Senate 30-19 on 2025-07-29. The House
  never voted on an override. The imported Senate roll is that override.
- **House Bill 171** was overridden by the House on 2026-06-24. The Senate never
  voted on an override. The House override roll is one of the four held, so the
  roll imported for this bill is the Senate's earlier passage vote.

A description of either has to say the bill did not become law, and why.

## In the batch

| Measure | What it would have done | Area and direction of a yes vote |
|---|---|---|
| H 171 | Bans diversity, equity and inclusion programs in state agencies and local government | civil rights, against |
| S 50 | Permitless concealed carry at 18, plus higher police death benefits and new felon-with-a-gun offenses | gun control, against |
| H 437 | Higher drug penalties near homeless shelters, and bars local governments from allowing public camping | public safety for, social programs against |

Senate Bill 50 appears twice: the House passage vote and the Senate's veto
override.

## Dropped on the fifth filter

- **H 958 Election Law Changes.** Shortens early voting and raises campaign
  money reporting limits, while adding a citizenship answer to registration and
  State Auditor audits of county boards. A fair reader could call a yes vote
  either a restriction or a safeguard. Same reason House Bill 834 was dropped
  from batch-02.
- **H 377 2026 Court Changes.** A vehicle bill. The House passed it 113-0 as an
  estates and trusts package and the Senate replaced the contents a year later.
  Most of the final bill is routine court procedure, and it was a conference
  report, which cannot be amended.
- **H 96 Expedited Removal of Unauthorized Persons.** Despite the title it is
  about squatters, not immigration. It bundles a fast removal process with an
  unrelated ban on local pet shop rules, and no research area fits.
