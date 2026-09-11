# Oklahoma batch 05

Six measures, ten roll calls, 264 candidate records. Local database only. Production holds no
Oklahoma roll-call records.

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| HB 2764 income tax cut with a trigger for more | House, Senate | 74-19, 34-11 | personal_income_tax_reduction | for |
| SB 460 natural gas preferred for new power plants | House | 70-19 | environment_and_public_health | against |
| SB 758 online days counted as class time | House, Senate | 53-32, 33-14 | public_education_quality | for |
| SB 212 TeachForwardOK teacher training trial | House, Senate | 51-29, 34-10 | public_education_quality | for |
| SB 1618 risk report before pretrial release | House | 62-23 | public_safety_and_crime_control | for |
| SB 743 disturbing a religious service | House, Senate | 73-21, 31-15 | public_safety_and_crime_control | for |

HB 2764 is the largest measure in the Oklahoma pool by reach into a voter's own money: it
cuts the top income tax rate and sets a trigger that can keep cutting until the rates reach
zero.

## Checks run before importing

- **Version check.** All six were diffed. HB 2764 came back at 0.988, SB 460 at 0.984, SB 758
  at 0.974, SB 212 at 0.946, SB 1618 at 0.981 and SB 743 at 0.982. Every difference is a page
  header or a dropped emergency section.
- **Filter 4.** SB 212 and SB 743 each have a later roll than the chamber's first passage
  vote, and the later one is the one recorded. SB 212's House fourth reading on 22 May
  supersedes its 6 May third reading. SB 743's Senate fourth reading falls in February 2026,
  nine months after the House vote, because the measure sat between sessions.
- **Reading level**, measured before the import: median Flesch-Kincaid grade 6.2, worst 7.2.
- **The repository's plain-language lint**: 20 descriptions, 0 warnings.
- **British spellings**: the builder wrote on the first attempt.
- **Related records**: four flags, all hand-written records about a different bill acted on
  the same day. Nothing retired.

## Result

Dry run planned 264 inserts; the real run inserted 264 with no errors and nobody notified,
under the stamp `2026-09-10T06:26:23.611Z`. The convergence run reports all 264 unchanged.
Oklahoma now holds 1,209 records locally across five batches.
