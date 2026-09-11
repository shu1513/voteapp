# Utah 2026 General Session, batch-08

3 roll calls on 2 measures. 125 candidate records across 71 candidates.
Local database only. Production holds no Utah records. Run with `--state UT-2214`.

| measure | rolls | area | direction |
| --- | --- | --- | --- |
| HB 192 Vehicle Inspection Fee | House 40-31 | cost_of_living_reduction | against |
| SB 314 Sleep Disorders Education | House 56-14, Senate 16-12 | environment_and_public_health | for |

Both labels state `nay: null`.

## Checks run before importing

- **Version, per roll.** All 3 were cast on the enrolled text (substitute 0).
- **Governor action.** Both measures carry a `Governor Signed` line.
- **Tally and members.** All 3 match Utah's own record and were cleared name by
  name against Utah's vote sheets.
- **SB 314's Senate vote was 16-12 on a suspension caption.** Suspending the rules
  can need two thirds, so the roll's outcome was read from Utah's action list, not
  the caption: the next action, one second later, is `Senate/ to House`, and the
  bill was signed. The roll passed the bill.
- **Same-session overlaps.** No other 2026 act amends either section.
- **Duplicates.** None flagged by the importer; the bill-number sweep found none.
- **Prose.** Flesch-Kincaid grade median 8.3, worst 8.8; the repository lint
  reports 0 warnings over all 6 descriptions.

## Reconciled three ways

- `import-report.json`: 3 rolls, 125 inserts, run stamp `2026-09-10T16:30:43.365Z`.
- Database on that stamp: 125 rows, 71 distinct candidates, 90 area tags.
- Convergence dry run afterwards: 125 unchanged. The dry-run stamp
  `2026-09-10T06:58:57.652Z` matches 0 rows.

## The rest of the 2026 pool is triaged

This batch also records a written disposition for 81 more 2026 rolls on 58
measures, in `survey/dispositions.tsv`:

- **13 need your direction call.** They are the same contested classes as 2025:
  HB 174, HB 258, HB 404, HB 209, SB 268, SB 295, SB 170, HB 259, HB 204, SB 174,
  HB 118, HB 136 and HB 293.
- **The rest are dropped, each with a reason.** The reasons are: no area fits;
  strands point different ways; the school-choice precedent; the Arkansas
  ballot-initiative precedent; or the subject is too narrow to recognize.

14 candidate rolls remain in 2026 and are still unread: HB 48, HB 54, HB 185,
HB 188, HB 273, HB 331, HB 419, HB 436, HB 539, HB 540, SB 26, SB 58, SB 153
and SB 251.
