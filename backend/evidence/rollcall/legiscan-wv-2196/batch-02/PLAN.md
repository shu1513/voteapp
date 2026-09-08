# West Virginia 2025 regular session, batch 02

## What this batch is

Batch 01 covered measures that became law. That pool is now used up. Every
measure-chamber slot in it carries a written disposition in
`../survey/dispositions.tsv`, and none is unaccounted for.

Batch 02 uses the one sanctioned exception to the "became law" filter: a measure
one chamber voted on that did not become law. Pennsylvania set this precedent,
and the rule is that it may only be used once the enacted pool is exhausted. It
is exhausted here.

Because none of these measures became law, every description is written in the
conditional. Each one says "would have", and each one says what stage the
measure actually reached, so a reader is never left thinking a bill is in force
when it is not.

## How the pool was built

Start from every roll call the West Virginia classifier keeps as a floor vote on
the substance of a bill. Group them by measure and chamber. Keep that chamber's
last such roll call, ordered by the printed roll number that the chamber itself
assigns. Require that last roll call to be closely divided, meaning the smaller
side is at least a quarter of the larger. Then keep only the measures that did
not become law.

That gives 58 measure-chamber slots across both sessions, 36 of them in this
2025 session.

## Checks run before any judging

**Every measure was confirmed not to be law.** No bill in the pool carries an
approval line in its own action history. Two carry a veto: HB 3111 in this
session and SB 672 in 2026. Both were vetoed after the session had ended and
neither history shows an override vote, so both stand vetoed. West Virginia can
override a veto by simple majority, so this was checked rather than assumed.
Both were dropped on subject grounds in any case.

**Every tally was audited against the state's own vote sheets.** West Virginia
publishes one official vote sheet for each roll call. All 58 slots were
downloaded and compared on bill number, yes count and no count. All 58 matched.
The result is in `tally-audit.json`.

**The text each chamber actually voted was checked one measure at a time.** This
mattered more than usual here, and it changed three records. See `JUDGING.md`.

## What the filters removed

Of the 36 slots in this session, 14 were imported and 22 were dropped. The
reasons are recorded per slot in `dispositions.tsv`. In summary:

- 9 were administrative machinery or licensing detail, such as the value at
  which a city must auction property, or whether drywall work needs a contractor
  license.
- 6 were too narrow for a voter to recognize as a subject, such as a hunting
  provision or a tax credit for compliance paperwork.
- 6 read two honest ways on their own text, so no direction could be defended.
- 1 was a pay raise for judges, which lands on the no-stance general label. That
  follows the same call made on South Carolina S 933.

## Result

14 roll calls imported, 518 candidate records. No errors.

The three counts agree: the import report says 518, the run-stamp query returns
518, and the table grew by 518. The duplicate sweep found no hand-written record
on the same candidate and the same date.
