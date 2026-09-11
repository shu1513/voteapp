# Oklahoma batch 02

Six measures, seven roll calls, 216 candidate records. Local database only. Production holds
no Oklahoma roll-call records.

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| HB 1003 age of consent and close-in-age rule | House | 60-21 | public_safety_and_crime_control | for |
| HB 1393 parental consent for alternate testing | House | 71-23 | public_education_quality | for |
| HB 1601 ARCHER Act, extending maternity leave | House | 58-27 | social_programs_and_welfare | for |
| HB 3127 medical marijuana at work | House | 47-46 | civil_rights, labor_rights | against, against |
| HB 3151 minimum school days | House, Senate | 62-28, 30-17 | public_education_quality | for |
| HB 1576 Medicaid gene testing for very ill children | Senate veto override | 36-9 | healthcare_affordability | for |

HB 3127 is the closest vote in the whole Oklahoma pool at 47-46.

## A held roll turned out to be a false positive, and it changed the pool

Batch one's tally audit matched each roll against history lines **of the same day**, and that
window is one day too narrow. HB 1576's House veto override is roll-dated 2025-05-29 while
Oklahoma's own history prints `Veto overridden: Ayes: 76 Nays: 12` on 2025-05-30. The tallies
agree exactly, so the roll was never in doubt.

Re-running the audit over a one-day window cleared that roll and left the other nineteen
standing. The config no longer holds it, and the comment there now tells the next reader to
use a one-day window.

Correcting it changed the answer for HB 1576. With the override restored, the House's last
kept roll is that 76-12 override, which is not closely divided, so the House slot leaves the
pool under filter 4. The earlier 68-21 fourth reading is **not** imported: recording it would
credit members with a position they revisited a week later, which is the mistake Pennsylvania
HB 103 paid for. HB 1576 is therefore imported on its Senate override alone. The pool is now
324 slots rather than 325.

## Checks run before importing

- **Version check.** Four measures had the House vote before the Senate amended, so the text
  the House passed was diffed against the act: HB 1393, HB 1601, HB 3127 and HB 3151 all came
  back at 0.985 or better, with every difference a page header. HB 1003's roll is a fourth
  reading, which is the chamber's vote on the final text.
- **Reading level**, measured before the import: median Flesch-Kincaid grade 7.1, worst 9.1.
- **The repository's plain-language lint**: 14 descriptions, 0 warnings.
- **British spellings**: the builder refused to write until `licence` was corrected.
- **Related records**: none flagged.

## Result

Dry run planned 216 inserts; the real run inserted 216 with no errors and nobody notified.
Plan, run and database agree, under the stamp `2026-09-10T04:38:59.607Z`. The convergence run
afterwards reports all 216 unchanged. Tags rose to 363, which is batch one's 206 plus this
batch's 157 yes-side records — predicted from the ledger before the database was read.
