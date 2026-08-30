# California batch-04 — selection

**21 roll calls / 11 measures / 806 records.** Imported to local `voteapp` 2026-08-30. Prod untouched.

## How much is actually left

Triaged every remaining measure before starting. After the 31 measures used in batches 01-03 and the
16 dropped under filter 5, **180 measures still had a divided, enacted roll**. But:

| | measures |
| --- | --- |
| divided in BOTH chambers | 67 |
| — of those, budget acts and trailer bills | 27 |
| — of those, single-jurisdiction local measures | 8 |
| — **genuine candidates** | **32** |
| divided in ONE chamber only | 113 |

So the both-chamber tail is **two batches, not five**. This batch reads 11 of the 32; the rest,
plus the 113 one-chamber measures (one roll each, ~68 records in the Assembly or ~11 in the Senate),
are what remains.

## What came through

| measure | area | yea | Assembly | Senate |
| --- | --- | --- | --- | --- |
| SB 497 shield law for protected health care | womens_reproductive_rights | for | 61-17 | 30-10 |
| SB 59 privacy of name and gender change records | data_privacy | for | 60-16 | 29-8 |
| AB 847 oversight boards see police personnel files | public_safety_and_crime_control | for | 50-16 | 23-10 |
| AB 1312 hospitals must screen for charity care | healthcare_affordability | for | 62-16 | 27-8 |
| SB 707 remote public access to local meetings | anti_corruption | for | 53-16 | — |
| AB 1362 registration of farmworker recruiters | corporate_accountability | for | 59-16 | 30-8 |
| AB 1340 rideshare drivers may bargain | corporate_accountability | for | 60-15 | 29-10 |
| AB 454 migratory bird protection | environment_and_public_health | for | 58-17 | 30-10 |
| AB 309 permanent needle access | environment_and_public_health | for | 54-15 | 28-11 |
| SB 262 wider prohousing policy definition | housing_affordability | for | 57-15 | 27-11 |
| AB 727 crisis line on student ID cards | environment_and_public_health | for | 58-15 | 30-10 |

**Two more areas gain their first California coverage: `womens_reproductive_rights` (SB 497) and
`data_privacy` (SB 59).** California now covers 13 of the 27 research areas.

## The two judgment calls in this batch

- **SB 707 contributes only its Assembly roll.** The Senate's single divided vote (24-6, 2025-06-03)
  came *before* two later amendments (09-02 and 09-05); its post-amendment concurrences were 27-6,
  outside the divided gate. Rather than write a "the Senate voted an earlier version" caveat across
  ~11 records, the Senate roll is left out. Filter 4 caps a measure at one roll per chamber; it does
  not require both.
- **SB 59's Senate concurrence has a duplicate-date twin** (`../CODE-FINDINGS.md` §1): rolls 1602138
  (09-12) and 1602889 (09-13), both 29-8 with an identical lineup. The official history records the
  concurrence on **09/13**, so 1602889 is the pick. Every other pick was screened and has no twin.

## Dropped under filter 5 after a full read

- **SB 642 (Employment: payment of wages)** — strengthens equal-pay claims (a broader comparator, a
  three-year limitations period running from the last violation) while **loosening the pay-scale
  disclosure duty**, redefining "pay scale" as a good-faith *estimate* of the expected range. Two
  directions in one bill, the SB 477 / SB 786 pattern.
- **ACA 8 and AB 604 (congressional redistricting)** — the mid-decade redistricting fight. Contested
  direction, not a reading of the statute; held out of every batch so far for the same reason.
- **SB 125, SB 155, SB 170, SB 57, AB 182, SB 158** — a Medi-Cal provider tax, a media-program
  appropriation, codification of a Governor's Reorganization Plan, a report requirement, ballot-
  measure mechanics, and a terse-titled land use bill: budget-adjacent, procedural, or report-only.
- **AJR 29** — a joint resolution urging Congress; no legal effect in California.

## Checks

- **Version check: all 21 votes were cast on the enrolled text.** `rolls.json` records each bill's
  last amendment date. SB 707's Senate roll was the one pick that failed this check, and it was
  dropped rather than caveated.
- **Duplicate-date screen run on every pick**; only SB 59 had a twin, resolved against the history.
- **Every qualification read from the enacted section**, not the digest: SB 707's population
  thresholds and 2030 sunset (Gov. Code 54952.2), AB 1362's H-2A start date of 2027-07-01 (Bus. &
  Prof. Code 9998), AB 454's surviving federal-rule exceptions (Fish & Game Code 3513), SB 59's
  effective dates and the six-month wait before the enforcement right opens.
- **Written in plain English from the start** and linted before any database write: **0 warnings
  across 42 descriptions**, average sentence 22 words, none of the tracked jargon terms present.

## Left for later

**21 both-chamber candidates** from the 32 triaged, plus **113 one-chamber measures**. The autumn
re-download remains a separate, larger seam: 430 divided rolls were still awaiting the governor when
this dataset was cut on 2026-08-23.
