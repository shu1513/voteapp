# Oklahoma batch 02

Five measures, six roll calls, 173 candidate records. It was six measures and 216 records
until HB 3127 was retracted on PR #1316 review; see below. Local database only. Production holds
no Oklahoma roll-call records.

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| HB 1003 age of consent and close-in-age rule | House | 60-21 | public_safety_and_crime_control | for |
| HB 1393 parental consent for alternate testing | House | 71-23 | public_education_quality | for |
| HB 1601 ARCHER Act, extending maternity leave | House | 58-27 | social_programs_and_welfare | for |
| HB 3151 minimum school days | House, Senate | 62-28, 30-17 | public_education_quality | for |
| HB 1576 Medicaid gene testing for very ill children | Senate veto override | 36-9 | healthcare_affordability | for |

## HB 3127 was retracted: the 47-46 roll is a failed vote

The batch imported HB 3127 on House roll 1662879 of 2026-03-12 at 47-46 and described it as a
passage. It was not. Oklahoma needs a majority of all 101 members, 51 votes, and the House
history for that day reads `Third Reading, Measure failed: Ayes: 47 Nays: 46`. Notice to
reconsider was served the same day, the House voted 70-18 to reconsider on 2026-03-16,
amended the bill on 2026-03-24 and passed it 68-27. LegiScan carries no roll for that vote,
so the only divided roll on this measure is a failed vote on an earlier text, not the
chamber's vote on the enacted act. Keeping it would credit 46 members with a no vote on an
act many of them then voted for, the mistake the HB 1576 note below warns about.

The retraction follows the Tennessee SB 766 recipe: 43 records retired through
`manual:records:retire` (`hb3127-retirements.json`), roll 1662879 set back to pending, the
entry removed from `judgments.json` and the roll's evidence JSON removed from this folder (it
stays in the out-of-repo store). Both rerun reports in this folder were regenerated after the
retraction: six files, 173 records, all unchanged.

The tally audit did not catch this because it compares counts only, and the count matched.
The verb in the history line has to be read as well; that is now trap 5 in the README.

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
  the House passed was diffed against the act: HB 1393, HB 1601 and HB 3151 all came
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
