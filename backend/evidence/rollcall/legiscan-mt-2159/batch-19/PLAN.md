# Montana batch-19 — the last batch

Five measures, eight roll calls, 280 candidate records. All five became law.
A sixth, SB 117, was imported and then withdrawn; see the correction note at
the end of this file.

**This batch finishes the Montana campaign.** Every one of the 633 divided,
enacted roll calls in the survey worklist now carries a disposition, and nothing
is left unworked.

| Measure | Chapter | Area | Yes vote means |
| --- | --- | --- | --- |
| SB 39 rules on what a lawyer may charge the losing side | 542 | anti_corruption | against |
| SB 154 selling a whole human body banned | 361 | environment_and_public_health | **for** |
| SB 193 chief justice alone appoints the bar examiners | 365 | anti_corruption | against |
| SB 342 court administrator serves the chief justice alone | 387 | anti_corruption | against |
| SB 375 no hemp product containing THC sold to consumers | 394 | environment_and_public_health | **for** |

**SB 342 carries its Senate roll alone.** Its House roll, 1546349, is one of the
eight rolls held on the LegiScan vote defect — LegiScan puts SJ Howell on the
wrong side. That was found in the batch-07 audit and the hold still stands.

## Two bills on the same question, judged the same way

SB 193 moves the power to appoint, remove and direct the attorneys' examining
board from the Supreme Court as a body of seven to the chief justice alone.
SB 342 does the same for the state court administrator, and applies retroactively
to the person already in post.

Both are **against** `anti_corruption`, for the same reason SB 97 and HB 365 were
in batch-17: they concentrate a power that was held collectively, and reduce the
number of people who must agree before it is used.

## What was dropped, and why the tail is so thin

Fifty-four measures, SB 117 included, were dispositioned without a record in
this batch. All of
them were read. The reasons are in `../survey/filter-5-drops.md` and fall into
four groups: agency housekeeping, acts carrying several unrelated subjects,
acts that cut both ways at once, and acts too narrow for any research area.

That is the expected shape of the end of a session. The campaign worked the
substantive bills first and the tail last, so the keep rate fell from twelve
measures in batch-11 to six here.

## Every roll was checked against Montana's own vote record

All nine imported rolls, SB 117's included, agree exactly. Two rolls on these measures do not and
neither was selected: SB 154's roll 1540327, and SB 342's House roll, which was
already held.

## Correction — SB 117, withdrawn after import

SB 117 was imported with this batch and has since been withdrawn. Its 74
records are retired (`sb117-retirements.json`), and the measure is marked
`dropped:filter-5` in the worklist.

**Why.** The first reading said both of its changes to 15-10-420 raise what a
local government may collect. That was wrong. Section 2 of the enrolled act
(Chapter 554) makes two changes to the maximum-mill formula, and they pull in
opposite directions:

- The inflation allowance rises from one-half of the three-year average to the
  full average, capped at 4 percent. That raises the permitted levy.
- Before the act, all of the current year's newly taxable value was subtracted
  from the taxable value before the mills were set, and the resulting rate was
  then applied to every property, new ones included, under subsection (2). The
  act subtracts only 75 percent of new class four value, and 40 or 50 percent
  for most other classes. Leaving part of the new value in the base lowers the
  permitted mills, so a local government now keeps only part of the growth it
  used to keep in full. That lowers the permitted levy. The retired records
  described this change as a gain; it is the reverse. Subsection (2) is not
  amended, so newly taxable property was never escaping the levy.

Which effect is larger depends on how fast a jurisdiction grows against
inflation, and nothing on the face of the act settles it. Under the rule
settled in batch 11, an act that raises one limit and lowers another in the
same provision has no defensible single direction, so it is dropped rather than
judged. The HB 924 overlap on 15-10-420 noted in the earlier version of this
file no longer touches any record.
