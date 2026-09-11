# Utah 2026 General Session, batch-06 — the first 2026 batch

9 roll calls on 7 measures. 418 candidate records across 71 candidates.
Local database only. Production holds no Utah records.

This is the first batch from LegiScan session 2214. It runs under the registry
key `UT-2214`: every import command here takes `--state UT-2214`, because the
bare key `UT` points at the 2025 session. The crosswalk is the one committed
under `legiscan-ut-2137/`, since LegiScan people ids are the same across both
sessions; the people file is this directory's `legiscan-people-ut-2214.json`.

## What is in the batch

| measure | rolls | area | direction |
| --- | --- | --- | --- |
| HB 337 Nicotine Product Tax | House 51-19 | environment_and_public_health | for |
| SB 69 School Device Revisions | House 46-21 | public_education_quality | for |
| SB 252 Water Usage at State-owned Facilities | House 41-28 | environment_and_public_health | for |
| HB 296 Water Commitment | House 48-19 | environment_and_public_health | for |
| HB 330 Liability Limitations | House 50-15, Senate 17-7 | corporate_accountability | against |
| SB 68 Disability Litigation | House 58-15, Senate 20-6 | civil_rights | against |
| SB 292 Autonomous Systems | House 55-15 | corporate_accountability | against |

Seven of the nine rolls are House rolls, which is why one batch reaches 71
candidates. Every label states `nay` explicitly and every one is null.

## Checks run before importing

- **Version, per roll.** All 9 were cast on the substitute that was enrolled.
- **Governor action.** All 7 measures carry a `Governor Signed` line.
- **Tally and members.** All 9 match Utah's own record and were cleared name by
  name against Utah's per-roll vote sheet; every question is final passage.
  None is one of the four 2026 rolls held for a tally mismatch.
- **Same-session overlaps.** 2026 is the latest session, so each act was checked
  against the other 2026 acts amending the same sections. HB 265 and HB 337 both
  amend the e-cigarette tax section but only HB 337 changes the rate; SB 46 and
  SB 252 both amend the state landscaping section in different subsections. Both
  pairs merge without conflict. Details are in `JUDGING.md`.
- **Duplicates.** The importer flagged no related records. The bill-number sweep
  over the 315 Utah 2026 records not written by this pipeline found none on
  these bills.
- **Prose.** Flesch-Kincaid grade median 7.4, worst 8.9, longest sentence 33
  words; the repository lint reports 0 warnings over all 18 descriptions.

## Reconciled three ways

- `import-report.json`: 9 rolls, 418 inserts, run stamp `2026-09-10T06:48:26.930Z`.
- Database on that stamp: 418 rows, 71 distinct candidates, 304 area tags.
- Convergence dry run afterwards: 418 unchanged. The dry-run stamp
  `2026-09-10T06:47:43.177Z` matches 0 rows.

## Dropped

- **SB 287 Targeted Advertising Tax** — no research area fits. It is an annual
  tax on large targeted-advertising companies, a revenue measure rather than a
  rule about company conduct or data collection.

## Left

112 candidate rolls in the 2026 session, plus the four held rolls.
