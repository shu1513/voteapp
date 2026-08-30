# California batch-05 — selection (closes the both-chamber seam)

**13 roll calls / 7 measures / 509 records.** Imported to local `voteapp` 2026-08-30. Prod untouched.

This batch works the last of the both-chamber candidates. **With it, every measure in California's
divided-and-enacted set that is divided in both chambers has now been judged or dropped with a
recorded reason.**

## The ledger, closed

| | measures |
| --- | --- |
| divided-and-enacted universe | 227 |
| judged (batches 01-05) | 49 |
| dropped under filter 5 after a full read | 29 |
| excluded by standing rule: budget acts, trailer bills, single-jurisdiction | 36 |
| **both-chamber remaining** | **0** |
| divided in ONE chamber only (untouched) | 113 |

49 + 29 + 36 + 113 = 227. The one-chamber tail is the only seam left in this dataset.

Checked before starting: LegiScan still serves the **2026-08-23 cut** for session 2172 (same
`dataset_hash`), so the ~430 rolls awaiting the governor are still unavailable.

## What came through

| measure | area | yea | Assembly | Senate |
| --- | --- | --- | --- | --- |
| AB 1079 voting-rights orders not frozen on appeal | civil_rights | for | 56-17 | 25-13 |
| SB 22 cash out small gift-card balances | corporate_accountability | for | 58-16 | — |
| AB 931 consumer legal funding rules | corporate_accountability | for | 60-16 | 29-10 |
| AB 1487 2TGI wellness grant fund | social_programs_and_welfare | for | 59-15 | 30-10 |
| SB 635 sidewalk vendors' personal details | data_privacy | for | 58-15 | 29-8 |
| SB 805 plain-clothes officers must show ID | public_safety_and_crime_control | for | 60-15 | 30-10 |
| SB 358 traffic fees on low-car-trip housing | housing_affordability | for | 53-14 | 28-9 |

All seven carry a stance, and every label states `"nay": null` on purpose — none of these is a
referendum on its area.

## Dropped under filter 5 after a full read

- **SB 630 (State parks: real property)** and **AB 679 (Big Basin Redwoods, Año Nuevo, Butano)** —
  both waive Department of General Services or State Public Works Board approval for park land
  acquisitions until 2033. Faster park acquisition and weaker spending oversight are the same
  clause read two ways; neither direction is a reading of the statute rather than a value call.
- **AB 138 (State employment: state bargaining units)** — a Committee on Budget bill ratifying
  union agreements whose provisions take effect only if the Legislature appropriates the money.
  Budget measure, excluded by the appropriations precedent. Its title does not say "budget", which
  is why it survived the title-based triage and had to be caught by reading.

## Checks

- **Version check.** SB 22's only divided Senate vote (27-10, 2025-06-02) predates the final
  amendment of 2025-09-02, and its post-amendment concurrence was 37-0 — outside the divided gate.
  Following the SB 707 precedent from batch-04, **the Senate roll is dropped rather than caveated**,
  and SB 22 contributes its Assembly roll alone. All 13 remaining picks postdate their bill's last
  amendment.
- **Duplicate-date screen.** SB 635's Senate concurrence has a twin — rolls 1602329 (09-12) and
  1602895 (09-13), both 29-8. The official history records the concurrence on **09/13**, so 1602895
  is the pick. No other pick has a twin.
- **Every qualification read from the enacted section**: SB 805's five narrow exemptions and the
  agency-policy mechanism, AB 931's five-business-day cancellation right and the rule that the
  company's return is a set amount rather than a share of the winnings, AB 1079's
  Secretary-of-State escape valve and its 2026-01-01 cutoff, SB 635's state-or-federal-law carve-out.
- **Plain English from the first draft**, linted before any database write: **0 warnings across 26
  descriptions**, average sentence 23 words. A self-check also caught British spellings
  ("misdemeanour", "itemised", "programmes", "licence", "organisation") that had crept into the
  first draft; the corpus is American-spelled and now has zero.

## Left for later

**113 one-chamber measures**, each worth a single roll (~68 records in the Assembly, ~11 in the
Senate). And the autumn seam: 430 divided rolls on measures still awaiting the governor when this
dataset was cut on 2026-08-23, which needs a genuinely refreshed dataset — verify `dataset_hash`
before re-downloading.
