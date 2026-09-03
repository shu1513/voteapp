# Montana batch-07 — the held measures, and a defect in the vote data

Five measures, seven roll calls, 333 candidate records. All five became law.
Three more were read in full and set aside; the reasons are in
`../survey/filter-5-drops.md` and below.

| Measure | Chapter | Area | Yes vote means | House | Senate |
| --- | --- | --- | --- | --- | --- |
| HB 179 signing a ballot petition does not reactivate a voter | 191 | election_integrity | **for** | 58-42 | — |
| HB 231 the second half of the 2025 property tax law | 674 | cost_of_living_reduction | **for** | 60-39 | 28-22 |
| HB 801 limits on suing gun makers over their advertising | 727 | gun_control | against | 59-40 | 37-13 |
| SB 440 a public ballot-count report for every county | 610 | election_integrity | **for** | 58-41 | — |
| SB 542 the 2025 property tax law | 767 | cost_of_living_reduction | **for** | held | 28-22 |

## Why this batch exists

Batches 4 through 6 set four measures aside for reasons that could not be
settled inside those batches. This batch settles all four.

- **HB 231 and SB 542** were held because HB 231 voids most of itself if
  SB 542 also passes, and SB 542 also passed. Both are judged here, as one
  policy carried by two bills.
- **HB 329** was held because the House adopted a conference committee report
  with no matching Senate roll. That question is answered — the Senate's final
  vote was 41-9, too lopsided to use — but the measure is dropped for a
  different reason, below.
- **HB 801** was held because its private right of action needed its own read.
  It is judged here.

## What was set aside, and why

- **HB 329** (tax exemptions for ammunition manufacturers) is dropped under
  filter 5. No research area in the catalogue maps to it with a direction that
  can be defended. See `../survey/filter-5-drops.md`.
- **HB 423** (voter list maintenance) is dropped under filter 5 as genuinely
  two-sided.
- **SB 25** (deepfakes in election advertising) is dropped under filter 1. Both
  chambers' final votes were lopsided — the House concurred 80-17 and the
  Senate 45-3 — so neither roll separates one candidate from another.

## The vote-data defect

While working out which text the House had voted on for SB 542, its roll call
was checked against Montana's own vote record. LegiScan has one member's vote
wrong. That led to an audit of every roll in the campaign, which is described in
`JUDGING.md` and in `../CODE-FINDINGS.md` §7. **SB 542's House roll is held**
rather than imported, because the pipeline rightly refuses a hand-edited
evidence file.

## Reach

The House rolls carry 75 records each and the Senate rolls carry 11 each,
because all 100 House seats are on the 2026 ballot while only about half the
Senate is.
