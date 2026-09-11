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

## Corrections after review (2026-09-10)

Two descriptions were tightened against the enrolled text and re-applied
through `rollcall:judge`, then re-imported; `import-rerun-report.json`
shows {'rewrite': 18, 'unchanged': 122} and the convergence dry run after
it {'unchanged': 140}.

- **HB 471.** The work rules and six-month rechecks start January 1, 2027
  (§10(2)); the first draft carried no date, so they read as already in
  force beside the October 1, 2026 citizenship rule. The record now names
  the date and the verified exemptions (§26B-3-142.1(4), e.g. medically
  frail). 8 rows.
- **SB 234.** The direct-causation test applies only to a rule with no
  federal counterpart (§63G-3-306(2) refers to (1)(b)); the first draft read
  as universal. The record now scopes it and names the exemptions in (3):
  emergency rules, federally required rules, site-specific state-required
  rules. 10 rows.
- **HB 76, left as is.** The act also requires 10,000 square feet, but a
  facility drawing 75 acre-feet a year is never smaller than that, so the
  extra number would not change what a voter learns.

## Dropped

HB 314 (strands point different ways), HB 437 (process change with no
direction), HB 329 (no area covers public employment benefits). Reasons are in
`JUDGING.md` and `survey/dispositions.tsv`.

## Left

99 candidate rolls in the 2026 session, plus the four held rolls.
