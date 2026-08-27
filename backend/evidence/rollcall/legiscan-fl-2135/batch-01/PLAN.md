# FL 2135 — batch-01 selection

**11 votes / 8 measures / 385 records.** Local `voteapp` only; production untouched.

## How the 11 were chosen

The five filters, applied to all 760 stored floor votes:

1. **Divided** — `LEAST(yeas,nays) >= GREATEST(yeas,nays)/4`. 77 of 760 floor votes qualify (47 House, 30 Senate).
2. **Became law** — bill status 4. Cuts 77 to **37 votes over 20 measures**.
3. **Nameable subject** — a measure a voter can recognize, not a budget vehicle.
4. **One roll per measure per chamber**, preferring the third-reading passage of the text that became law.
5. **Stance-defensible** — the measure must carry a research area with an honest for/against direction. Anything that would land on `general` is dropped, not imported.

Florida's divided-and-enacted set is far smaller than Texas's (37 votes vs 813 divided actions), so filters 3-5 did most of the cutting: 20 measures in, 8 out.

## Selected

| Measure | Chamber(s) | Roll | Tally | Area | yea |
| --- | --- | --- | --- | --- | --- |
| HB 351 Dangerous Excessive Speeding | house | 1555400 | 75-38 | public_safety_and_crime_control | for |
| HB 875 Educator Preparation | house | 1555216 | 84-27 | public_education_quality | for |
| HB 903 Corrections | house / senate | 1549690 / 1560643 | 83-33 / 26-11 | civil_rights | against |
| HB 1219 Employment Agreements (CHOICE Act) | senate | 1557039 | 28-9 | corporate_accountability | against |
| HB 6025 Firearm Restrictions During Emergencies | house | 1550259 | 86-28 | gun_control | against |
| SB 56 Geoengineering and Weather Modification | senate / house | 1535606 / 1562557 | 28-9 / 82-28 | environment_and_public_health | for |
| SB 492 Mitigation Banks | house | 1561772 | 87-26 | environment_and_public_health | against |
| SB 1080 Local Government Land Regulation | senate / house | 1558940 / 1562037 | 26-8 / 84-29 | housing_affordability | for |

Seven House rolls (~50 records each) carry the batch; the four Senate rolls add 7-9 each.

## Deferred, with reasons

Twelve measures cleared filters 1-2 and were dropped later. None is judged, all stay `pending`.

**Contested direction (filter 5).** The text runs both ways inside one area, so no honest single direction:

- **HB 1205 Amendments to the State Constitution** — the session's marquee elections fight. It tightens petition verification (circulator registration, signature-verification notices, a revocation process, invalidation of forms from ineligible circulators), which reads `election_integrity`/for, while also capping how many signed forms an unregistered person may hold, barring some felons from circulating, and adding criminal penalties, which reads as restricting a lawful petition process. Also the only measure here where the two chambers voted materially different texts. **Worth a second look if the campaign wants a direction call made by hand** — the Ohio batch-04 precedent is exactly that.
- **HB 653 Aggravating Factors for Capital Felonies** — expands death-penalty eligibility; `public_safety_and_crime_control` does not carry an honest direction for that. Senate-only, ~9 records.
- **SB 1804 Capital Human Trafficking** — same reason.
- **HB 443 Education** — charter-school autonomy; `public_education_quality`/for is not defensible on its face.

**Omnibus / multi-subject.** No two-sentence description can describe them without flattening:

- **SB 700 Department of Agriculture and Consumer Services** — 30+ subjects. Sections 31-32 define "water quality additive" and bar any other additive, which is the provision that **ends community water fluoridation** (29 counties fluoridated at the time), but that is one strand of an omnibus covering electric-utility land, EV charging standards, mushroom spores, plant-based meat labeling, petroleum registration and more. **The other measure worth a hand direction call.**
- **HB 1255 Education** — School Readiness income definitions, reading plans, opioid antagonists, corporal-punishment opt-in, expulsion extensions, CLT10, a college rename.

**No nameable subject / `general` (filter 3).** HB 549 and HB 575 (renaming the Gulf of Mexico), HB 481 (anchoring limitation areas), HB 1549 (financial services assessments).

**Budget vehicles.** SB 2502 (implementing the General Appropriations Act) and SB 2510 (PreK-12 conforming bill) — the standing rule that there is no honest area for a fund-the-government vote.

## Out of scope entirely

Several marquee 2025 Florida fights never reach filter 1:

- **Immigration enforcement** passed in the 2025 **special** sessions (LegiScan 2203 / 2204), not this session's dataset.
- **The gun-age repeal (HB 759)** passed the House but died in the Senate — status 6, never enacted.
- Condo reform and hemp produced no divided-and-enacted floor roll.

This mirrors Texas SB 3 (the vetoed hemp bill), which had zero divided votes.
