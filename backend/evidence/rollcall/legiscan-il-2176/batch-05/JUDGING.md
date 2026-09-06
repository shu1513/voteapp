# Illinois batch-05 — measures 26 to 50 of the unbatched pool

25 measures read, **15 kept: 23 rolls / 1,567 records across 132 candidates.**
Illinois now holds **7,391 roll-call records**, local `voteapp` only.

Every measure was read from the Illinois General Assembly's own BillStatus XML
synopsis stack and action trail. No judgment came from a short title.

## What was kept

| measure | rolls | area |
|---|---|---|
| HB 4304 higher education student support | H 72-37, S 37-19 | `civil_rights` |
| HB 4397 school psychologist qualifications | S 46-12 | `public_education_quality` |
| HB 4420 college press act | H 75-36, S 39-19 | `civil_rights` |
| HB 4514 utility rate increase forums | H 81-22 | `corporate_accountability` |
| HB 4533 DREAM Fund donation option | H 73-38, S 39-19 | `immigration` |
| HB 4577 play-based kindergarten | H 76-27, S 45-13 | `public_education_quality` |
| HB 4606 newborn home visiting | H 73-31 | `social_programs_and_welfare` |
| HB 4639 juvenile community mediation | H 65-39, S 39-18 | `public_safety_and_crime_control` |
| HB 4702 diaper labeling | H 73-38 | `environment_and_public_health` |
| HB 4788 CPR manikins | H 73-38, S 43-13 | `environment_and_public_health` |
| HB 4862 school staff mental health | H 81-27, S 43-15 | `public_education_quality` |
| HB 4890 unaccompanied children | H 82-26 | `immigration` |
| HB 4948 intelligent speed assistance | H 77-24 | `public_safety_and_crime_control` |
| HB 4966 DCFS SECURE Act | H 75-40, S 35-19 | `civil_rights` |
| HB 5070 no weaker federal pollution rules | H 69-28 | `environment_and_public_health` |

Two earlier House rolls (HB 4577 roll 1718058, HB 4966 roll 1719211) are marked
`screened:superseded-roll`: the House voted again on the enacted Senate version.

No date skew this batch. Three rolls fall on a sine-die day (HB 4304 senate and
HB 4966 house, both 2026-05-31), and the ILGA action trail dates both the same
way LegiScan does, so no `official_vote_date` override was needed.

## The one measure dropped for cutting both ways

**HB 4762** started as the Reducing Barriers to Licensure Act: it stops the state
from holding up a license application because the applicant is incarcerated, and
says the lack of a certificate of relief from disabilities cannot by itself deny
a license. The Senate then added a rule that seals a licensee's final
disciplinary orders from public view five years on. One half opens a door for
people leaving prison; the other half closes a record the public uses to check a
professional. Comparable weight, opposite directions, so the measure is dropped
rather than described in one direction.

## The other drops

Nine measures found no research area: HB 4327 (a 340B study), HB 4491
(line-of-duty illness pay — **the labor gap again**), HB 4535, HB 4536, HB 4540,
HB 4587, HB 4714, HB 4826 and HB 5135, all administrative or definitional.

## Reconciliation

| step | records |
|---|---|
| predicted from `crosswalk.json` | 1,567 |
| dry run | 1,567 `insert` |
| real run | 1,567 `insert` |
| re-run | 1,567 `unchanged` |
| rows in the local database | 1,567 |

Descriptions read at a median Flesch-Kincaid grade of **6.7**, worst **8.4**,
no sentence over 45 words, no British spellings.

## What is left

126 rolls of the original 204 still carry `candidate:unbatched`.
