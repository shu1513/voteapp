# Oklahoma batch 03

Four measures, five roll calls, 164 candidate records. Local database only. Production holds
no Oklahoma roll-call records.

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| SB 176 multi-month contraceptive supply | House, Senate | 65-25, 30-16 | womens_reproductive_rights | for |
| SB 1443 anesthesia payment and patient severity | House | 68-17 | corporate_accountability | for |
| SB 631 crimes requiring 85 percent of sentence | House veto override | 68-17 | public_safety_and_crime_control | for |
| SB 1833 food aid restrictions | House | 62-17 | social_programs_and_welfare | against |

SB 1833 is the first `against` in Oklahoma on the social programmes area, and SB 631 is the
second measure in the state that became law over a veto.

## Checks run before importing

- **Version check.** None of the four needed a text diff. SB 176's two rolls are both fourth
  readings, which is the chamber's vote on the final text. SB 1443 and SB 1833 were passed by
  the House on the same day the act was enrolled. SB 631's roll is a veto override, which is
  a vote on the enrolled act itself.
- **Reading level**, measured before the import: median Flesch-Kincaid grade 7.1, worst 9.8.
  SB 631 is the worst and stays there: the crimes it lists cannot be renamed.
- **The repository's plain-language lint**: 10 descriptions, 0 warnings.
- **British spellings**: the builder refused to write until `programme` was corrected.
- **Related records**: two flags, both hand-written records about a different bill acted on
  the same day (HB 1137 on the override day, Senate Bill 20 on 22 May). Nothing retired.

## Result

Dry run planned 164 inserts; the real run inserted 164 with no errors and nobody notified,
under the stamp `2026-09-10T04:43:25.546Z`. The convergence run reports all 164 unchanged.
Oklahoma now holds 683 records locally across three batches.
