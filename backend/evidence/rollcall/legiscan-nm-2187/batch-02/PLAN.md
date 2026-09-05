# New Mexico batch-02 — selection

**15 measures, 15 House roll calls, 872 records across 63 candidates.**

This batch clears the rest of the pool. Every divided-and-enacted House roll in the 2025 regular
session is now either imported, held for a stated reason, or dropped with a reason recorded in the
committed worklist.

## How the batch was picked

The same five standing filters as batch-01, applied in order.

1. **Divided.** The losing side is at least a quarter of the winning side.
2. **Became law.** LegiScan status 4.
3. **A nameable subject that maps to a research area.**
4. **One roll per measure per chamber.** New Mexico satisfies this for free: no bill has a second
   roll call in the same chamber anywhere in the session.
5. **A defensible direction.** A measure is taken only if a research area carries an honest
   for-or-against reading of a yes vote. Anything that would land on `general` is dropped.

**Only House rolls are eligible at all.** New Mexico senators serve four-year terms and were elected
in 2024, so no Senate seat is on the 2026 ballot. All 38 divided-and-enacted Senate rolls fan out to
nobody. That leaves 40 House rolls, one per measure. Batch-01 took 14, Senate Bill 3 is held, eight
were dropped at filter 3 or 5 during the survey, this batch takes 15, and three more are dropped
below.

## The 15

| Measure | Roll | Vote | Area, and which way a yes vote points |
|---|---|---|---|
| House Bill 8 | 1490733 | 48-20 | public safety and crime control, for; gun control, for |
| House Bill 78 | 1518278 | 36-28 | healthcare affordability, for; corporate accountability, for |
| House Bill 91 | 1499171 | 42-25 | cost of living reduction, for |
| House Bill 493 | 1523893 | 51-13 | anti-corruption, for |
| Senate Bill 1 | 1496721 | 46-19 | social programs and welfare, for |
| Senate Bill 5 | 1516594 | 42-26 | environment and public health, for |
| Senate Bill 7 | 1520401 | 46-18 | public infrastructure, for |
| Senate Bill 23 | 1523849 | 37-31 | corporate accountability, for |
| Senate Bill 37 | 1523940 | 42-20 | environment and public health, for |
| Senate Bill 45 | 1524422 | 55-14 | healthcare affordability, for |
| Senate Bill 48 | 1522144 | 39-26 | environment and public health, for |
| Senate Bill 59 | 1524366 | 34-27 | reduce wealth gap, for |
| Senate Bill 83 | 1523816 | 31-25 | environment and public health, for |
| Senate Bill 88 | 1524409 | 52-13 | healthcare affordability, for |
| Senate Bill 376 | 1524394 | 52-13 | healthcare affordability, for |

Seventeen labels across ten research areas. Six of the ten are new to New Mexico: public safety and
crime control, cost of living reduction, anti-corruption, social programs and welfare, public
infrastructure, and reduce wealth gap. Senate Bill 83 at 31-25 is the narrowest vote in the batch.

Every label states the nay side explicitly, and every one is null. New Mexico had a Democratic
trifecta in 2025, so the divided-and-enacted set is the majority's agenda and every direction reads
`for`. On each measure the realistic objection runs on a different axis from the area being scored,
which is what a null nay records.

## The three dropped at filter 5

These passed the divided and enacted gates but carry no research area with an honest direction.

- **House Bill 66** (36-22, workers' compensation). Raises the cap on claimant attorney fees from
  $22,500 to $30,000 and the employer's discovery cost advance from $3,000 to $3,500, with further
  steps in 2027 and 2029. Fee mechanics. Batch-01 dropped the marriage license fee bill for the same
  reason.
- **House Bill 295** (38-27, Renewable Energy Transmission Authority property tax). Confirms that a
  private lessee's interest in transmission line property owned by the authority stays exempt from
  property tax. Read one way it clears the path for renewable transmission; read the other it is a
  property tax break for a private energy company at the expense of local revenue. Both readings are
  strong, so no direction is defensible.
- **Senate Bill 73** (39-27, bicycle stops). Lets a cyclist treat a stop sign as a yield and a red
  light as a stop-then-proceed. Which way this points on public safety is exactly what the floor
  debate was about, and no research area settles it.

## What is left in New Mexico after this batch

- **Senate Bill 3** stays held. LegiScan's stored tally disagrees with the state's official roll call
  sheet and the approval gate would publish the wrong number. It needs an official-tally override,
  the shape of the existing `official_vote_date` override.
- **37 divided rolls on measures that passed one chamber and died.** A different scope, never opened
  for New Mexico.
- **The 2026 regular session is LegiScan 2251**, complete and unsurveyed. It needs only an `NM-2251`
  registry key if its description vocabulary matches. Special sessions 2227 and 2232 are also
  unsurveyed.
- **Production still holds no New Mexico roll-call records.** Both batches are local only.
