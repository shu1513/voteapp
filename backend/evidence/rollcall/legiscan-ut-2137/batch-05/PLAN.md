# Utah 2025 General Session, batch-05 — the 2025 pool is closed

6 roll calls on 6 measures. 45 candidate records across 9 candidates.
Local database only. Production holds no Utah records.

## What is in the batch

| measure | roll | area | direction |
| --- | --- | --- | --- |
| HB 106 Income Tax Revisions | Senate 23-6 | personal_income_tax_reduction | for |
| HB 418 Data Sharing | Senate 20-5 | data_privacy | for |
| HB 207 Sexual Offense Revisions | Senate 16-10 | public_safety_and_crime_control | for |
| HB 274 Water Amendments | Senate 17-10 | environment_and_public_health | for |
| HB 264 Tax Incentives | Senate 16-7 | environment_and_public_health | against |
| HB 124 Education Industry Employee Privacy | Senate 16-5 | data_privacy | for |

All six are Senate rolls, and only 15 of Utah's 29 Senate seats are on the 2026
ballot, so the batch is small in records. It is here to finish the 2025 pool,
not for reach.

## Checks run before importing

- **Version, per roll.** All 6 were cast on the substitute that was enrolled.
- **Governor action.** All 6 measures carry a `Governor Signed` line.
- **Tally and members.** All 6 match Utah's own record and were cleared name by
  name against Utah's per-roll vote sheet; every question is final passage.
- **Later sessions.** Four 2026 acts touch these sections (HB 190, HB 290, HB 408
  and two sexual-offense acts). Each builds on the 2025 act; none reverses it.
  Details are in `JUDGING.md`.
- **A summary that disagrees with the act.** Utah's bill record summarizes
  HB 264 with a 2035 cutoff; the enrolled text says 2028. The record uses 2028.
- **Duplicates.** The importer flagged no related records, and the bill-number
  sweep over Utah records not written by this pipeline found none.
- **Prose.** Flesch-Kincaid grade median 8.4, worst 8.8, longest sentence 37
  words; the repository lint reports 0 warnings over all 12 descriptions.

## Reconciled three ways

- `import-report.json`: 6 rolls, 45 inserts, run stamp `2026-09-10T06:40:23.021Z`.
- Database on that stamp: 45 rows, 9 distinct candidates, 28 area tags.
- Convergence dry run afterwards: 45 unchanged. The dry-run stamp
  `2026-09-10T06:39:31.928Z` matches 0 rows.

## Every other 2025 candidate roll now has a written disposition

This batch also closes the 2025 pool. The 61 remaining candidate rolls (54
measures) were read on their official summaries, and the unclear ones on their
enrolled text, and each carries a reason in `survey/dispositions.tsv`:

- **6 more measures need your direction**, joining the eight already listed:
  HB 226 (sheriffs must alert federal immigration authorities before releasing
  an unlawfully present person after a sentence), HB 252 (no cross-sex hormones
  or sex-change surgery started for people in state custody), HB 269 (privacy in
  sex-designated areas), HB 209 (fewer homeschool paperwork steps), HB 390
  (religious student groups in higher education), and HB 479 (universities may
  pay athletes directly, the same question as Wyoming SF 44).
- **The rest are dropped**, each with its reason. The common reasons:
  - No research area covers the subject: property tax, federalism, court
    procedure, local government form, or a financing mechanism.
  - The strands point different ways (HB 503 malpractice, SB 201 rental
    rules, the land use and justice omnibus acts).
  - School-choice measures, which carry no stance under the campaign
    precedent.
  - The subject is too technical or narrow for a voter to recognize, such
    as a sunset extension or a code alignment.

## Where Utah 2025 ends

26 measures, 36 rolls, 1,146 candidate records across five batches. No 2025
roll is still marked `candidate`. The open items are the 14 direction calls.
