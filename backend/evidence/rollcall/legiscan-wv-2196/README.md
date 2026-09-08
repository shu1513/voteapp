# West Virginia 2025 Regular Session — LegiScan session 2196

Roll-call vote import, phase 4. See `docs/plans/roll-call-vote-import.md`.

## Source

LegiScan bulk dataset for West Virginia's 2025 Regular Session, downloaded
2026-09-07. The downloaded archive matched the byte size LegiScan declares for
the dataset exactly. The extracted dataset lives outside the repository at
`~/legiscan-data/wv-2196/`, because a whole session is far larger than the
curated evidence this directory keeps.

Dataset contents: 1298 roll calls and 134 people. The 134 people are
exactly 100 members of the House of Delegates and 34 senators.

## Survey

`survey/` holds the report written by:

    npm run rollcall:legiscan:fetch -- --state WV --dataset-dir ~/legiscan-data/wv-2196 \
      --survey --evidence-dir evidence/rollcall/legiscan-wv-2196/survey

The state's configuration entry in
`backend/src/pipeline/rollcall/legiscanStateConfigs.ts` was written from that
report's description histogram and from nothing else.

Classification of this session, confirmed by a dry-run fetch:

- 904 floor votes kept
- 305 roll calls excluded by rule as procedural: the separate effective-date votes, amendments, and motions
- 10 roll calls surfaced and never queued: 2 with an unknown question, plus the 8 rolls held by the audit
- 79 roll calls on excluded measure types (simple and concurrent resolutions)
- 0 committee votes
- 0 unrecorded votes, 0 duplicate roll-call identities, 0 parse errors, 0 file errors

The first four lines add up to the dataset's 1,298 roll calls.

## What is unusual about West Virginia

**It publishes no committee votes at all.** Every roll call's total equals the
full chamber, so the floor-versus-committee inference that other states rely on
has nothing to separate here.

**It votes a bill's effective date separately from the bill.** Those votes need
a two-thirds majority, so the minority can defeat them and they are often
closely divided. They are excluded, because they are not votes on the bill's
substance.

**Divided votes are rare.** 10.2 percent (93 of 912 before the two surfaced rolls) of this session's floor votes are closely
divided. Republicans hold a supermajority in both chambers, so a party-line
bill passes far outside the divided gate rather than near it.

## The pool

29 measures and 55 floor roll calls are both closely divided and on a
bill that became law, over 40 measure-chamber slots.

## Audit against the state's own vote sheets

Every floor roll on an enacted bill in this session was checked against West
Virginia's own vote sheet, which the dataset links from each vote's
`state_link`. The sheet prints the bill number, the question, the tally and both
member lists, and the audit compares all three of bill, tally and question. It
covers every such roll, not only the closely divided ones, because a tally error
can itself decide whether a roll counts as closely divided.

Across both sessions 1,409 of 1,423 rolls match on all three. The 8 that
fail in this session are pinned in `heldRollCallIds` in the state configuration,
which stores and surfaces them but never lets them be approved. The reason for
each is written out there. They are of three kinds: a roll filed under the wrong
bill, a tally short by one member, and a caption naming a different question
from the one the sheet prints.

The question comparison also corrected one pattern. The caption "House
reconsidered effective date and passage" reads like a vote on both, but the
sheets for both such rolls print EFFECT FROM PASSAGE, so it is now excluded with
the other effective-date votes.

Four Senate sheets print no question line at all (their rolls are unanimous or
nearly so), and nine Senate rolls captioned "Passed Senate" carry the question
"Concur and Pass" on the sheet. Those nine are still votes on the measure, so
they stay kept; the caption only misnames which stage.

## Batches

**Batch 01** covered measures that became law. That pool is used up: every
measure-chamber slot in it carries a written disposition in
`survey/dispositions.tsv`.

**Batch 02** covers measures one chamber voted on that did not become law. This
is the one sanctioned exception to the "became law" filter, and it is used only
because the enacted pool is exhausted. Every description in it is written in the
conditional and says what stage the measure reached. See `batch-02/PLAN.md` for
the method and `batch-02/dispositions.tsv` for all 36 slots in this session.
