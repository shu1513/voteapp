# South Dakota, the votes on measures that did not become law

The first five South Dakota batches took only measures that became law. That
gate hides most of what a South Dakota legislator actually votes on, because
the House and the Senate kill each other's bills constantly and a bill that
dies never appears in the enacted pool at all. This scope opens those votes.

It is the same scope Pennsylvania batch-02 opened, and the reasoning is the
same: a recorded floor vote is a position whether or not the bill survived.

## Result: the scope is closed

| | count |
| --- | --- |
| closely divided measures that did not become law | 155 |
| imported, batches 06 to 10 | **54** (61 roll calls, 2,080 records) |
| dropped with the reason written out | **101** |
| open | **0** |

The ledger, one row per measure, is `dispositions-not-enacted.json`. The
judging notes are `JUDGING-not-enacted.md`.

## The pool, measured before any batch size was promised

Built by `sd_notenacted.py`, which is `sd_worklist.py` with filter 2 turned
around. One row per measure and chamber, carrying that chamber's last recorded
floor roll on a kept caption, on a bill whose status is not "enacted" and whose
history carries no enactment line.

| | count |
| --- | --- |
| slots on measures that did not become law | 236 |
| of those, closely divided | **179** |
| distinct measures | **155** |

Of the 179 divided slots, 118 are votes the measure lost and 61 are votes it
won in one chamber before dying in the other. Eight are veto override votes on
four bills the Governor vetoed and the Legislature failed to save.

## The gate was checked, not assumed

Every measure at LegiScan status 3 ("enrolled") or 5 ("vetoed") was read by
hand, because both statuses can sit next to a bill that later became law:

- Four bills were vetoed and the veto sustained: HB 1132 and HB 1169 in 2025,
  HB 1077 and HB 1138 in 2026. All four carry the state's own line "Delivered
  veto sustained to the Secretary of State".
- Three bills reached enrolled status and still died: HB 1135, HB 1209 and
  HB 1323, on a failed concurrence, a rejected conference report or a failed
  reconsideration.

No measure in the pool has a signing line or an act number.

## Filters

The same five as the enacted batches, with filter 2 reversed:

1. Closely divided: the smaller side is at least a quarter of the larger.
2. **The measure did not become law.**
3. The subject is one a voter would recognize as a policy question.
4. One roll per measure per chamber: that chamber's last recorded floor roll.
5. The measure carries a research area with a defensible for-or-against
   direction. No `general`, no no-stance labels.

Filter 5 does most of the work: 101 of 155 measures are dropped.

## Three traps this scope adds, each now checked by a tool

**The vehicle bill.** `sd_vehicle.py` renders, for every roll, the version
printed on or before that roll's date and reports how much text it held. It
found HB 1135, whose whole text when the House passed it was one placeholder
sentence. Dropped.

**The floor amendment that was never reprinted.** A bill that fails is never
reprinted, so an amendment adopted minutes before the losing vote appears in no
version. `sd_lateamend.py` lists every such case: 18. Each was read from the
amendment document. Three changed what the measure meant (HB 1223, SB 51 and
SB 198 in 2026).

**The other chamber's fate.** The sentence saying what the other chamber did is
read from the state's own history lines by `sd_build_ne.py`: its decisive vote
on a kept caption, a floor vote to table or defer with its tally, or a
committee kill named as one. See `JUDGING-not-enacted.md` for the three rules
and the rewrites they caused.

## Wording

Every description is in the conditional, "which would have required", because
none of these bills changed the law. Both South Dakota sessions adjourned
before this work began, the last action in either being the veto day of 30
March 2026, so "did not become law" is a statement about a closed session and
cannot later turn false.
