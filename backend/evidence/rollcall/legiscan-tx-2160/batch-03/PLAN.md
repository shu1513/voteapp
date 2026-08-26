# Texas 89R — batch 03

The third slice of the Texas roll-call import. Batches 01 and 02 took the 52
marquee and second-tier votes; this batch takes 12 more from the 716 divided
floor actions they left pending.

`rolls.json` is the selection, machine-readable. `JUDGING.md` records how each
measure was judged.

## The numbers

| | |
|---|---|
| divided floor votes still `pending` after batch 02 | 716 |
| …distinct measures among them | 646 |
| …that became law or were vetoed | 296 |
| …after removing measures already judged in batches 01–02 | 250 |
| selected for batch 03 | **12** (8 house, 4 senate) |
| measures covered | 8 |
| records written | **967** |

Batch 01 wrote 1,620 records from 25 votes and batch 02 wrote 1,741 from 27.
This batch is deliberately smaller. The pool left after two passes is the tail
PLAN 02 predicted — narrow and local bills — and the honest yield is a short
list of measures a voter can actually recognize, not a quota.

## How these 12 were chosen

The same four filters as batches 01–02 (divided; became law; nameable subject;
one roll per measure per chamber, preferring third reading), plus one new
filter this batch:

5. **A defensible stance direction.** Every measure here carries a research-area
   label with a `for`/`against` direction. Anything that would have landed on
   `general` was dropped rather than imported, because a `general` record buys
   the reader far less and the tail is full of them.

That fifth filter did most of the cutting. Measures examined and dropped for it:

| measure | why it was dropped |
|---|---|
| SB 1957 | one sentence barring felons from a municipal police/fire civilian oversight board — reads both as integrity and as weaker community oversight |
| SB 1498 | applies existing civil asset forfeiture to digital assets and adds custody rules; calling that for or against civil rights overstates it |
| HB 2674 | bars TEA and the SBOE from regulating home school programs — outside the public-school-quality area it would have to borrow |
| SB 974 | the caption reads like a school-board bill; the statute is about who may sit on an **appraisal review board**. Vetoed, and narrow |

## The batch

Each of the eight measures fills a research area with **zero** prior Texas
coverage.

| measure | subject | area | yea | house | senate |
|---|---|---|---|---|---|
| HB 223 | competitive bidding for municipal lobbying contracts | `anti_corruption` | for | 92-56 | 19-12 |
| SB 14 | state agency rulemaking and judicial deference | `government_efficiency` | for | 97-51 | — |
| HB 121 | armed officer on each campus, threat assessment teams | `public_safety_and_crime_control` | for | 97-38 (conc.) | — |
| HB 1586 | access to the school immunization exemption affidavit | `environment_and_public_health` | against | 83-54 | — |
| SB 2024 | banned categories of e-cigarette product | `environment_and_public_health` | for | 96-35 (conf.) | 20-11 |
| HB 5033 | contingent end of vehicle emissions inspections | `environment_and_public_health` | against | 101-45 | 21-10 |
| SB 2835 | single-stairway apartment buildings | `housing_affordability` | for | 81-57 | — |
| SB 1036 | licensing and cancellation rights in residential solar sales | `corporate_accountability` | for | 94-37 | 22-8 |

Where a measure shows only one chamber, the other chamber passed it without
division. Exact roll numbers, dates, and the specific question for each are in
`rolls.json`.

## What this batch still leaves out

- **704 divided actions**, still mostly narrow or local: a management
  district's bond authority, one county's composting rules, a specialty
  license plate.
- **350 measures that passed only one chamber.**
- **The vetoed measures with divided votes.** SB 974 was examined here and
  dropped on subject, not on the veto; the class stays deferred.
- **SB 3**, the vetoed hemp/THC ban, which still has no divided votes.
