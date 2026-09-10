# South Dakota, the votes on measures that did not become law

The first five South Dakota batches took only measures that became law. That
gate hides most of what a South Dakota legislator actually votes on, because
the House and the Senate kill each other's bills constantly and a bill that
dies never appears in the enacted pool at all. This scope opens those votes.

It is the same scope Pennsylvania batch-02 opened, and the reasoning is the
same: a recorded floor vote is a position whether or not the bill survived.

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

Of the 179 divided slots, 118 are votes the measure LOST and 61 are votes it
won in one chamber before dying in the other. Eight are veto override votes on
four bills the Governor vetoed and the Legislature failed to save.

## The gate was checked, not assumed

Every measure at LegiScan status 3 ("enrolled") or 5 ("vetoed") was read by
hand, because both statuses can sit next to a bill that later became law:

- Four bills were vetoed and the veto sustained: HB 1132 and HB 1169 in 2025,
  HB 1077 and HB 1138 in 2026. All four carry the state's own line "Delivered
  veto sustained to the Secretary of State".
- Three bills reached enrolled status and still died — HB 1135 and HB 1209 on a
  failed concurrence or reconsideration, HB 1323 the same way.

No measure in the pool has a signing line or an act number.

## Filters

The same five as the enacted batches, with filter 2 reversed:

1. Closely divided: the smaller side is at least a quarter of the larger.
2. **The measure did not become law.**
3. The subject is one a voter would recognize as a policy question.
4. One roll per measure per chamber: that chamber's last recorded floor roll.
5. The measure carries a research area with a defensible for-or-against
   direction. No `general`, no no-stance labels.

Filter 5 does most of the work. Of 155 measures, 96 are dropped, and the reason
is written out for every one of them in `dispositions-not-enacted.json`.

## Two traps this scope adds

**The vehicle bill.** South Dakota introduces placeholder bills whose whole
text is one sentence, then hoghouses the real subject in later. A chamber that
voted before the hoghouse voted on nothing. `sd_vehicle.py` renders, for every
roll in the pool, the version printed on or before that roll's date and reports
how much operative text it held. It found one: **HB 1135**, whose entire text
when the House passed it 53-15 was "The Legislature shall provide opportunities
for South Dakotans". Dropped.

**The floor amendment that was never reprinted.** South Dakota reprints a bill
after a chamber passes it. A bill that FAILS is never reprinted, so an
amendment adopted minutes before the losing vote appears in no version at all.
Reading the newest print then describes text nobody voted on.
`sd_lateamend.py` lists every such case: there are 18. Each was read from the
amendment document, which prints the whole bill as amended.

Two of them changed what the measure meant:

- **HB 1223** was narrowed on the House floor from vaccinations in general to
  genetic-based vaccinations only.
- **SB 51** was changed on the House floor from the Ten Commandments in every
  classroom to a prominent location in each school, so the two chambers voted
  different requirements. It is judged twice, once per chamber.

## Wording

Every description is in the conditional — "which would have required" — because
none of these bills changed the law. The sentence saying what happened to each
bill is derived by `sd_build_ne.py` from the dataset: the chamber's own
Passed or Failed word, what the other chamber did, and whether a veto was
sustained. Nothing about a bill's fate is typed by hand.

Both South Dakota sessions adjourned before this work began, the last action in
either being the veto day of 30 March 2026, so "did not become law" is a
statement about a closed session and cannot later turn false.
