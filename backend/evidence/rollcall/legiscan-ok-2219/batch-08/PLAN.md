# Oklahoma batch 08

Four measures, four roll calls, 39 candidate records. Local database only. Production holds no
Oklahoma roll-call records. This batch closes the Oklahoma pool.

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| SB 20 immigration and English checks for commercial drivers | Senate | 36-10 | immigration | against |
| SB 1847 wider eligibility for Medicaid care at home | Senate | 35-11 | healthcare_affordability | for |
| SB 54 aggravated drunk driving made a felony | Senate override | 33-14 | public_safety_and_crime_control | for |
| SB 259 groundwater waste and data center cooling | Senate | 38-10 | environment_and_public_health | for |

## Checks run before importing

- **Version check.** SB 1847 came back at 0.966, with only headers and the effective date
  differing. SB 20's Senate roll adopted the conference report, SB 54's is a veto override and
  SB 259's is a fourth reading, so each is a vote on the final text.
- **Reading level**, measured before the import: median Flesch-Kincaid grade 7.9, worst 8.1.
- **The repository's plain-language lint**: 8 descriptions, 0 warnings.
- **Related records**: none flagged.

## Result

Dry run planned 39 inserts; the real run inserted 39 with no errors and nobody notified, under
the stamp `2026-09-10T06:51:15.762Z`. The convergence run reports all 39 unchanged.

Oklahoma now holds 1,593 records locally across eight batches, from 63 roll calls, for 53
candidates. Every one of the 253 closely divided, enacted measures has a written disposition
in `survey/triage.json`: 45 imported and 208 dropped.
