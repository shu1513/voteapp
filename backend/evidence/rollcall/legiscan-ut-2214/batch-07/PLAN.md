# Utah 2026 General Session, batch-07

10 roll calls on 9 measures. 140 candidate records across 68 candidates.
Local database only. Production holds no Utah records. Run with `--state UT-2214`.

## What is in the batch

| measure | rolls | area | direction |
| --- | --- | --- | --- |
| SB 60 Income Tax Rate | Senate 22-7 | personal_income_tax_reduction | for |
| HB 222 Limitation of Actions (climate liability) | Senate 20-6 | environment_and_public_health | against |
| HB 84 Dangerous Weapon (campus carry) | Senate 18-5 | gun_control | against |
| SB 234 Rulemaking (no stricter than federal) | Senate 20-7 | environment_and_public_health | against |
| HB 214 Firearms Liability | Senate 19-6 | gun_control | against |
| HB 471 Social Services (Medicaid work and verification) | Senate 20-5 | healthcare_affordability | against |
| HB 357 Motor Vehicle Data Privacy | Senate 19-10 | data_privacy | for |
| HB 76 Data Center Water Transparency | Senate 19-5 | environment_and_public_health | for |
| HB 60 Water Rights | House 54-17, Senate 18-7 | environment_and_public_health | against |

Nine of the ten rolls are Senate rolls, so the reach is smaller than batch-06.
Every label states `nay` explicitly and every one is null.

## Checks run before importing

- **Version, per roll.** All 10 were cast on the substitute that was enrolled.
- **Governor action.** All 9 measures carry a `Governor Signed` line.
- **Tally and members.** All 10 match Utah's own record and were cleared name by
  name against Utah's per-roll vote sheet; every question is final passage.
- **HB 76 needed the same-day guard.** The Senate's first vote failed 13-15,
  was reconsidered an hour later, and passed 19-5. The judgment approves the
  19-5 roll and names the failed one in `acknowledge_later_rolls`.
- **Same-session overlaps.** HB 125, SB 18, SB 38 and SB 148 also amend sections
  these acts touch; none changes the provisions described. See `JUDGING.md`.
- **A correction made before import.** The first draft of HB 357's description
  said carmakers "must" offer in-car privacy controls; the act requires them
  only for model year 2030 and later, and the record says so.
- **Duplicates.** The importer flagged no related records. The bill-number sweep
  found one Utah 2026 record on these bills, a committee note on HB 214 from its
  sponsor's profile; a committee action is not a floor vote, so nothing was
  retired.
- **Prose.** Flesch-Kincaid grade median 8.4, worst 9.1, longest sentence 33
  words; the repository lint reports 0 warnings over all 20 descriptions.

## Reconciled three ways

- `import-report.json`: 10 rolls, 140 inserts, run stamp `2026-09-10T06:54:37.475Z`.
- Database on that stamp: 140 rows, 68 distinct candidates, 100 area tags.
- Convergence dry run afterwards: {'unchanged': 140}. The dry-run stamp
  `2026-09-10T06:53:01.168Z` matches 0 rows.

## Dropped

HB 314 (strands point different ways), HB 437 (process change with no
direction), HB 329 (no area covers public employment benefits). Reasons are in
`JUDGING.md` and `survey/dispositions.tsv`.

## Left

99 candidate rolls in the 2026 session, plus the four held rolls.
