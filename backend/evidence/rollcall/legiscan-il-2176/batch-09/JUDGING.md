# Illinois batch-09 — the last 21 measures, and the campaign is finished

21 measures read, **16 kept: 18 rolls / 1,520 records across 132 candidates.**
Illinois now holds **12,814 roll-call records**, local `voteapp` only.

**Every row in `../survey/divided-enacted-worklist.tsv` now carries a
disposition. Nothing is left `candidate:unbatched`.**

## What was kept

| measure | rolls | area |
|---|---|---|
| SB 1181 Public Expression Protection Act | H 75-38 | `civil_rights` |
| SB 1261 community land trust task force | H 71-37 | `housing_affordability` |
| SB 1344 sewer camera work as public works | H 87-30 | `corporate_accountability` |
| SB 1441 Secure Choice auto-enrollment | H 74-37 | `reduce_wealth_gap` |
| SB 1519 school referrals to police | S 37-17, H 69-44 | `civil_rights` |
| SB 3020 harassment under the domestic violence law | H 91-23 | `public_safety_and_crime_control` |
| SB 3037 Dark Sky Act | H 75-36 | `environment_and_public_health` |
| SB 3066 Service Appointment Fairness Act | H 70-40 | `corporate_accountability` |
| SB 3222 Illinois Hemp Act | H 77-31 | `environment_and_public_health` |
| SB 3314 college credit hour reporting | H 83-28 | `public_education_quality` |
| SB 3465 construction site sanitary requirements | H 71-37, S 45-13 | `corporate_accountability` |
| SB 3565 township food banks and pantries | H 73-35 | `social_programs_and_welfare` |
| SB 3917 permits for major sewage treatment plants | H 64-34 | `environment_and_public_health` |
| SB 4006 flood coverage notice and climate risk disclosure | H 78-32 | `corporate_accountability` |
| SB 4025 lead service line replacement statewide | H 83-29 | `environment_and_public_health` |
| SB 4041 airport noise contour studies | H 71-24 | `environment_and_public_health` |

**SB 3222 is the last gut-and-replace of the campaign, and a good one.** The
short title is `VIDEO STREAMING AD VOLUME`, and the bill really did start as a
rule against commercials louder than the show. House Floor Amendment 2 replaced
the whole thing with the Illinois Hemp Act — licensing, recalls, penalties, a new
fund. The imported roll is the House third reading taken **after** that amendment
was adopted, so it is a vote on the hemp law, and the description says so.

## SB 3777 is not imported again

Its Senate concurrence was in the pool; batch-01 already filled the Senate slot
for that measure. Marked `screened:duplicate-chamber`, the third time this
campaign has made that call after SB 2339 and HB 5295.

## The drops

Four, all for want of a research area: SB 1491 (police therapy dog teams),
SB 3205 (a venue change), SB 3422 (grant mechanics) and SB 3897 (a licensing
omnibus). No measure in this batch failed on direction.

## Reconciliation

| step | records |
|---|---|
| predicted from `crosswalk.json` | 1,520 |
| dry run | 1,520 `insert` |
| real run | 1,520 `insert` |
| re-run | 1,520 `unchanged` |
| rows in the local database | 1,520 |

Median Flesch-Kincaid grade **6.8**, worst **8.3**, no sentence over 45 words,
no British spellings.

## The whole worklist, closed

| disposition | rolls |
|---|---|
| `judged:batch-01` … `judged:batch-09` | 187 |
| `screened:*` | 167 |
| `dropped:*` | 73 |

427 rolls, every one of them decided.
