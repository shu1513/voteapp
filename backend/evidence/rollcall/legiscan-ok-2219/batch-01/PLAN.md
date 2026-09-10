# Oklahoma batch 01

Seven measures, eleven roll calls, 303 candidate records across 53 candidates. Local
database only. Production holds no Oklahoma roll-call records.

## How these seven were chosen

The pool is every roll call that is each chamber's last vote on a measure that became law,
where the vote was closely divided: **325 rolls on 253 measures**. Those 253 were triaged
from each measure's own description, and 192 were dropped at that stage with a written
reason, leaving 61 to read. The reasons are recorded in `../survey/triage.json`:

| dropped as | measures |
| --- | --- |
| appropriation or fund transfer | 67 |
| sunset extension or agency housekeeping | 71 |
| industry tax credit, rebate or incentive | 34 |
| school-choice financing | 8 |
| joint resolution approving agency rules | 8 |
| narrow pilot or study | 2 |

School-choice financing follows the line the campaign has held since Texas SB 2: the act
does not cut a public-school appropriation by its own terms, so no research area carries an
honest direction for it.

Batch one takes seven of the 61 that are on the House side, because the fan-out differs
sharply by chamber: a House roll reaches about 41 of our candidates and a Senate roll about
9. That is not a gap in the feed. All 101 House seats are on the November 2026 ballot but
only 24 of 48 Senate seats are, and our roster covers 55 of those 125 seats.

## What is in the batch

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| SB 364 corporal punishment | House, Senate | 63-25, 31-16 | civil_rights | for |
| SB 504 minimum marriage age | House | 51-36 | civil_rights | for |
| HB 2263 phones in school and work zones | House veto override | 68-21 | public_safety_and_crime_control | for |
| SB 1344 insulin programme | House, Senate | 68-22, 32-13 | healthcare_affordability | for |
| SB 109 cancer testing cover | House, Senate | 68-20, 35-11 | healthcare_affordability | for |
| SB 139 school phone policy | House, Senate | 51-39, 30-15 | public_education_quality | for |
| SB 889 hospital prices | House | 51-38 | healthcare_affordability | for |

Every label states the no side explicitly, and every one is null. In each case the
realistic objection runs on a different axis from the area scored — parental rights, local
control, the cost of compliance — so a no vote is not evidence of a position on the area
itself.

## Checks run before importing

- **Version check, per roll.** Four measures had a chamber vote before the other chamber
  amended, so the text that chamber passed was diffed against the act. SB 364, SB 1344 and
  SB 889 came back identical apart from page headers. SB 139 differed in one way only: the
  version the Senate passed carried a July 2025 effective date and an emergency clause that
  the final act drops. That changes when the act starts, not what it does, so no description
  states an effective date. SB 109's two rolls are both votes to adopt the conference
  committee report, which is the text that became law.
- **Reading level, measured before importing rather than after.** A first draft measured a
  median Flesch-Kincaid grade of 11.7 and was rewritten. The batch now measures a **median
  grade of 6.1 and a worst of 7.7**, longest sentence under 45 words.
- **The repository's plain-language lint**: 22 descriptions, 0 warnings.
- **British spellings**: the builder refuses to write on a word list. It caught two of my own
  slips, `licence` and `recognised`, before the file was written.
- **Related records**: one flag, on Eric Roberts. It is a hand-written record about HB 1137,
  a different bill that the House also acted on that day. Kept; nothing retired.

## Result

Dry run planned 303 inserts. The real run inserted 303, across 53 candidates, with no
errors and nobody notified. The three counts agree: the plan, the run, and the database
under this run's own stamp `rollcall:OK:%:2026-09-10T02:38:35.564Z`. A convergence run
after the import reports all 303 unchanged.

Tags: 206, predicted from the ledger before the database was read and matching exactly.
That is the yes side only, which is correct where every no side is null.
