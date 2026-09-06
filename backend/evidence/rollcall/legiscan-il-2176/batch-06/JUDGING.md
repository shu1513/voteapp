# Illinois batch-06 — measures 51 to 75 of the unbatched pool

25 measures read, **10 kept: 13 rolls / 994 records across 132 candidates.**
Illinois now holds **8,385 roll-call records**, local `voteapp` only.

## What was kept

| measure | rolls | area |
|---|---|---|
| SB 1560 school mental health screening | H 72-36 | `public_education_quality` |
| SB 1743 prescribing psychologists and patients over 65 | H 67-31 | `healthcare_affordability` |
| SB 1797 Digital Assets and Consumer Protection Act | H 76-38, S 41-15 | `corporate_accountability` |
| SB 1859 Climate Displacement Task Force | H 71-38, S 40-16 | `environment_and_public_health` |
| SB 1920 AI guidance and sign language material for schools | H 74-34 | `public_education_quality` |
| SB 1947 teacher performance assessment pilot | H 84-31 | `public_education_quality` |
| SB 2280 firearm violence prevention reporting | H 71-36 | `gun_control` |
| SB 2319 Digital Asset Kiosks Act | H 81-36, S 39-18 | `corporate_accountability` |
| SB 2762 insurance for seizure detection devices | H 82-25 | `healthcare_affordability` |
| SB 2972 boat liability insurance | H 80-22 | `public_safety_and_crime_control` |

## Two more sine-die rolls are dated a day early

This extends the README's second hazard and the batch-03 finding. LegiScan
stamps the *legislative* day; the ILGA action trail carries the calendar day.

| measure | roll | LegiScan `vote_date` | ILGA date, used as `official_vote_date` |
|---|---|---|---|
| SB 1797 senate concurrence | 1582866 | 2025-05-31 | **2025-06-01** |
| SB 2319 senate concurrence | 1582888 | 2025-05-31 | **2025-06-01** |

Both were checked against the trail rather than assumed: SB 1859's senate
concurrence carries the same LegiScan date and the ILGA trail agrees with it, so
that roll got no override. All 67 records from the two skewed rolls landed on
2025-06-01.

## SB 2339 is not imported again, and that is deliberate

The pool held SB 2339's Senate concurrence of 2025-10-30, 37-20. **batch-01
already filled this measure's Senate slot** with the April third reading, 35-21,
and that description already tells the reader what happened next: the House
removed the E-Verify limits and the criminal penalty, and the narrower version
became law. Importing the concurrence too would give every senator two records
on one bill. The row is marked `screened:duplicate-chamber`.

## The drops

Two measures were dropped for a contested direction rather than a thin subject:

- **SB 2111** lets a person on a bicycle roll through a stop sign after slowing
  and yielding to pedestrians and traffic. Whether that makes roads safer or
  less safe is the argument itself, not a settled direction.
- **SB 2913** removes the student growth component from a teacher evaluation
  plan when a district and its teachers cannot agree and no contract covers it.
  A reader who cares about school quality could want that either way.

Twelve more found no research area: SB 1594, SB 1667, SB 1793, SB 1799, SB 2019,
SB 2179, SB 2408, SB 2426, SB 2506, SB 2761, SB 2914 and SB 2980. SB 2914, the
notice a school board owes a teacher before dismissal charges, is **the labor
gap for the third batch running.**

## Reconciliation

| step | records |
|---|---|
| predicted from `crosswalk.json` | 994 |
| dry run | 994 `insert` |
| real run | 994 `insert` |
| re-run | 994 `unchanged` |
| rows in the local database | 994 |

Median Flesch-Kincaid grade **7.9**, worst **8.7**, no sentence over 45 words,
no British spellings.

## What is left

93 rolls of the original 204 still carry `candidate:unbatched`.
