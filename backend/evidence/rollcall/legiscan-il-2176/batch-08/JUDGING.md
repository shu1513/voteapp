# Illinois batch-08 — measures 98 to 119 of the unbatched pool

22 measures read, **15 kept: 22 rolls / 1,595 records across 132 candidates.**
Illinois now holds **11,294 roll-call records**, local `voteapp` only.

## What was kept

| measure | rolls | area |
|---|---|---|
| HB 4273 fire insurance renewal notice and rate review | H 72-38, S 43-14 | `corporate_accountability` |
| HB 4277 notary applications after a felony sentence | H 75-33, S 36-19 | `civil_rights` |
| HB 5284 menopause treatment coverage | H 78-33 | `healthcare_affordability` |
| HB 5316 radiological environmental sampling | H 70-35, S 44-13 | `environment_and_public_health` |
| HB 5460 school social work scholarships | H 76-32 | `public_education_quality` |
| HB 5492 six-month hormone therapy supply | H 75-39, S 38-19 | `healthcare_affordability` |
| SB 19 crime victims and the Prisoner Review Board | S 32-22, H 74-37 | `public_safety_and_crime_control` |
| SB 24 missing person reports, no waiting period | H 87-28 | `public_safety_and_crime_control` |
| SB 25 the 2025 energy law | H 70-37, S 37-22 | `environment_and_public_health` |
| SB 67 nurse agency licensing | H 75-40 | `corporate_accountability` |
| SB 69 equine therapy coverage | H 78-33 | `healthcare_affordability` |
| SB 175 Klinefelter syndrome testing coverage | H 76-33 | `healthcare_affordability` |
| SB 212 paid break time for nursing mothers | H 82-27 | `corporate_accountability` |
| SB 714 car insurance renewal notice and rate review | S 42-14, H 70-38 | `corporate_accountability` |
| SB 1173 no notary fee for a homeless status form | H 81-28 | `civil_rights` |

**SB 25 is the session's largest measure and it is filed under a swimming pool
title.** `SWIMMING FACILITY COLD SPA` became the Municipal and Cooperative
Electric Utility Transparent Planning Act plus a full energy package: open
resource planning for city and cooperative utilities, storage, a solar bill of
rights, thermal energy networks, electric vehicle charging, and changes to how
the Commerce Commission reviews utility plans and rates. It is kept as one
measure with one direction because the package was built and voted as one clean
energy law, and the description says what it does rather than repeating a title
that names none of it.

## One more sine-die date skew

HB 5492's House concurrence, roll 1718098, is stamped 2026-05-31 by LegiScan and
**6/1/2026** by the ILGA action trail, so it carries an `official_vote_date`
override. All 95 of its records landed on 2026-06-01. SB 24's House third
reading shares the 2025-05-31 stamp and the trail agrees with it, so that roll
got none. Four Illinois rolls now carry the override across batches 03, 06 and 08.

## HB 5295 is not imported again

The pool held HB 5295's House concurrence of 2026-05-31. **batch-01 already
filled this measure's House slot** with the April third reading, and that
description already tells the reader that the Senate changed the definitions and
access rules and that the final version became law. Marked
`screened:duplicate-chamber`, the same call batch-06 made for SB 2339.

## The drops

**HB 5511**, the Children's Social Media Safety Act, is the one dropped for a
contested direction. It makes device makers hand app developers an age-bracket
signal for the primary user, and makes developers treat an under-18 signal as
authoritative. That is a child-safety gain bought with an age check on every
user, and a reader could reasonably want it either way.

Five found no research area: HB 4255 (big cats in traveling animal acts),
HB 5228, HB 5269, HB 5551 and **SB 453 — the labor gap for the fourth batch
running.**

## Reconciliation

| step | records |
|---|---|
| predicted from `crosswalk.json` | 1,595 |
| dry run | 1,595 `insert` |
| real run | 1,595 `insert` |
| re-run | 1,595 `unchanged` |
| rows in the local database | 1,595 |

Median Flesch-Kincaid grade **6.7**, worst **8.5**, no sentence over 45 words,
no British spellings.

## What is left

24 rolls of the original 204 still carry `candidate:unbatched`.
