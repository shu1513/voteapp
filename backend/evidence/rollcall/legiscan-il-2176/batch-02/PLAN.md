# Illinois batch-02 — selection

54 divided floor votes / 29 enacted measures / **3,319 records across 132
candidates**. Batch-01 was 22 rolls / 11 measures / 1,364 records.

This batch also completes the **disposition sweep**: every one of the 427
divided-and-enacted rolls in `../survey/divided-enacted-worklist.tsv` now
carries a verdict, so nothing in the Illinois pool is unaccounted for.

## The five filters

Applied to the 405 rolls left after batch-01. Filters 1 and 2 (divided, became
law) were already satisfied by the worklist. Filter 3 (nameable subject) was
read off the Legislative Reference Bureau synopsis — **never the short title**;
filter 4 kept one roll per measure per chamber, preferring third reading; and
filter 5 dropped anything without a defensible for/against direction.

## Selected — 29 measures

| measure | Public Act | area / yea |
|---|---|---|
| H.B. 3247 free public education regardless of immigration status | 104-0288 | `immigration` / for |
| H.B. 2436 public defender in immigration cases | 104-0225 | `immigration` / for |
| H.B. 5093 in-state tuition eligibility | 104-0511 | `immigration` / for |
| S.B. 405 school counseling regardless of citizenship status | 104-0353 | `immigration` / for |
| H.B. 5024 detention-facility siting buffer | 104-0786 | `immigration` / for |
| H.B. 1312 Illinois Bivens Act | 104-0440 | `immigration` / for |
| H.B. 460 student-aid caps under the RISE Act | 104-0164 | `immigration` / for |
| H.B. 2568 Equality for Every Family Act | 104-0448 | `civil_rights` / for |
| H.B. 1083 gender-neutral property law + name-change deed fee waiver | 104-0040 | `civil_rights` / for |
| H.B. 2425 felony-conviction discrimination in final expense policies | 104-0224 | `civil_rights` / for |
| H.B. 4758 driver's licence requirements in job postings | 104-0776 | `civil_rights` / for |
| H.B. 4379 adult changing stations | 104-0771 | `civil_rights` / for |
| H.B. 1836 Clean Slate Act | 104-0459 | `civil_rights` / for |
| H.B. 4834 Prescription Monitoring Program exemptions | 104-0535 | `data_privacy` / for |
| S.B. 2437 Medicaid doula access | 104-0009 | `healthcare_affordability` / for |
| H.B. 5390 hospital financial assistance and billing | 104-0521 | `healthcare_affordability` / for |
| H.B. 4461 hospital liens on a primary residence | 104-0490 | `healthcare_affordability` / for |
| H.B. 2517 maternal implicit-bias training | 104-0061 | `healthcare_affordability` / for |
| S.B. 3487 hospital immunization policies | 104-0729 | `environment_and_public_health` / for |
| S.B. 2164 unpaid-wage collection | 104-0135 | `corporate_accountability` / for |
| H.B. 4844 juror pay | 104-0683 | `corporate_accountability` / for |
| H.B. 4725 Attorney General worker-protection investigations | 104-0676 | `corporate_accountability` / for |
| H.B. 5090 Transportation Network Driver Labor Relations Act | 104-0788 | `corporate_accountability` / for |
| H.B. 1189 prevailing wage on federal projects | 104-0160 | `reduce_wealth_gap` / for |
| H.B. 4418 plastic-pellet stormwater controls | 104-0772 | `environment_and_public_health` / for |
| H.B. 3026 dual language education | 104-0266 | `public_education_quality` / for |
| S.B. 191 school bus seat belts | 104-0075 | `public_safety_and_crime_control` / for |
| H.B. 4571 county affordable-housing powers | 104-0554 | `housing_affordability` / for |
| H.B. 4456 low-income utility discounts | 104-0540 | `cost_of_living_reduction` / for |

Eleven research areas. `immigration` is the largest group at 13 rolls, which is
what an Illinois session produces when the federal government is running
immigration enforcement the state opposes — the mirror image of Texas, where
the same area scored `against`.

## Dropped, with reasons

**Whole measures:**

- **H.B. 3772** (preschool suspensions) — two independent reasons. The chambers
  voted different text and the roll that actually adopted the enacted version
  (house concurrence 72-40, 2026-05-30) sits outside the divided set; and
  Senate Committee Amendment 2 added a school-board liability shield for
  certain K-2 expulsions, cutting against the bill's own protections.
- **H.B. 3510** (stretch energy code) — genuinely mixed: two efficiency
  deadlines slip a year while a third is pulled forward a year.
- **S.B. 3272** (ICC renewable-plan experts) — the concrete legal effect is
  exempting the hiring from the Procurement Code; the clean-energy benefit is
  indirect.

**Single rolls, measure kept:**

- **H.B. 2568 house roll 1545195** — that vote was cast on the *trust code and
  unclaimed property* text, which the Senate later deleted entirely. Only the
  senate roll is on the Equality for Every Family Act. See JUDGING.md.
- **H.B. 5024 senate 1716945, S.B. 2437 senate 1582772, H.B. 5090 house
  1719024** — the ILGA action trail dates each a day later than LegiScan
  (past-midnight sine-die votes). Held at `pending:date-skew` rather than
  imported on a date we know is wrong.

## Fan-out arithmetic

27 house rolls at a median 92 matched candidates and 27 senate rolls at a
median 33 predicted roughly 3,375 records; the dry run planned 3,319 and the
real run inserted exactly that.
