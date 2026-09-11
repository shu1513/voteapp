# Oklahoma batch 07

Six measures, six roll calls, 58 candidate records. Local database only. Production holds no
Oklahoma roll-call records.

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| HB 1168 trafficking in abortion-inducing drugs | Senate | 37-10 | womens_reproductive_rights | against |
| HB 1087 longer school year and teacher pay scale | Senate | 35-12 | public_education_quality | for |
| HB 2049 Medicaid mental health parity | Senate | 33-11 | healthcare_affordability | for |
| HB 3467 paid leave for school staff who adopt | Senate | 36-10 | social_programs_and_welfare | for |
| HB 3985 Safe Neighborhoods Act | Senate | 30-16 | public_safety_and_crime_control | for |
| SB 1451 prior voter registration | Senate | 35-9 | election_integrity | for |

Every roll here is a Senate roll, because none of these measures was closely divided in the
House. That is why six measures wrote only 58 records.

## Six measures were read and dropped

HB 2105, HB 1187, SB 1067, SB 906, SB 574 and SB 269. The reasons are in JUDGING.md and in
`survey/triage.json`.

## Checks run before importing

- **Version check.** HB 1168 came back at 0.976, HB 2049 at 0.985, HB 3985 at 0.988 and
  SB 1451 at 0.978. HB 3467 came back at 0.963, and its only differences are the effective
  date and an emergency clause, which no description mentions. HB 1087 came back at 0.922,
  because its longer school year was written in at the final stage; its Senate roll is a
  fourth reading, the chamber's vote on that final text.
- **Reading level**, measured before the import: median Flesch-Kincaid grade 7.3, worst 7.4.
  The first draft measured 8.5 and was rewritten.
- **The repository's plain-language lint**: 12 descriptions, 0 warnings.
- **Related records**: none flagged.

## Result

Dry run planned 58 inserts; the real run inserted 58 with no errors and nobody notified, under
the stamp `2026-09-10T06:49:35.333Z`. The convergence run reports all 58 unchanged. Oklahoma
now holds 1,554 records locally across seven batches.
