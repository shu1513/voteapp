# West Virginia 2026 Regular Session — LegiScan session 2254

Roll-call vote import, phase 4. See `docs/plans/roll-call-vote-import.md`.

## Source

LegiScan bulk dataset for West Virginia's 2026 Regular Session, downloaded
2026-09-07. The downloaded archive matched the byte size LegiScan declares for
the dataset exactly. The extracted dataset lives outside the repository at
`~/legiscan-data/wv-2254/`, because a whole session is far larger than the
curated evidence this directory keeps.

Dataset contents: 1432 roll calls and 134 people. The 134 people are
exactly 100 members of the House of Delegates and 34 senators.

## Survey

`survey/` holds the report written by:

    npm run rollcall:legiscan:fetch -- --state WV --dataset-dir ~/legiscan-data/wv-2254 \
      --survey --evidence-dir evidence/rollcall/legiscan-wv-2254/survey

The state's configuration entry in
`backend/src/pipeline/rollcall/legiscanStateConfigs.ts` was written from that
report's description histogram and from nothing else.

Classification of this session, confirmed by a dry-run fetch:

- 1033 floor votes kept
- 0 roll calls surfaced as an unknown question (never queued)
- 22 roll calls on excluded measure types (simple and concurrent resolutions)
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

**Divided votes are rare.** 4.6 percent (47 of 1,033) of this session's floor votes are closely
divided. Republicans hold a supermajority in both chambers, so a party-line
bill passes far outside the divided gate rather than near it.

## The pool

17 measures and 24 floor roll calls are both closely divided and on a
bill that became law, over 21 measure-chamber slots.
