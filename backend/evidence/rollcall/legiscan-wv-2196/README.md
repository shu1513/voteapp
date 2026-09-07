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

- 909 floor votes kept
- 7 roll calls surfaced and never queued: the unknown questions plus the 5 rolls held by the tally audit
- 79 roll calls on excluded measure types (simple and concurrent resolutions)
- 0 committee votes
- 0 unrecorded votes, 0 duplicate roll-call identities, 0 parse errors, 0 file errors

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

## Tally audit

Every floor roll on an enacted bill in this session was checked against West
Virginia's own vote sheet, which the dataset links from each vote's
`state_link`. The audit covers all such rolls, not only the closely divided
ones, because a tally error can itself decide whether a roll counts as closely
divided.

Across both sessions 1,412 of 1,423 rolls match exactly. The 5 that fail in
this session are pinned in `heldRollCallIds` in the state configuration, which
stores and surfaces them but never lets them be approved. The reason for each
is written out there.
