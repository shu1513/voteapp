# Oklahoma batch 09

One measure, one roll call and 38 candidate records, plus label fixes to earlier batches after a
review on 2026-09-11. Local database only. Production holds no Oklahoma roll-call records.

| measure | roll | tally | area | direction |
| --- | --- | --- | --- | --- |
| SB 1027 citizen petition rules | House | 69-23 | election_integrity | for |

## Label fixes in earlier batches

- **SB 2084** (batch-04) changed from `government_spending_reduction`, yes = for, to
  `labor_rights`, yes = against. The Labor Rights area did not exist when it was judged.
- **HB 3127** (batch-02) kept `civil_rights`, yes = against, and added `labor_rights`,
  yes = against. It was then retracted on PR review: the 47-46 roll is a failed vote. See
  `batch-02/PLAN.md`. Its 43 records are retired.
- **SB 250, HB 1601 and HB 3467.** The Labor Rights backfill (PR #1305) added `labor_rights` to
  these three rolls in the database but not to their batch files. A re-run from those files
  would have removed the tag. The files now match the database. No record changed.

The reasons for each call, and for the four calls reviewed and kept, are in JUDGING.md.

## Checks run before importing

- **Version check.** The House roll is the third reading of the House committee version. The
  Senate then took the House changes 39-7 with no further change. The House version and the
  act match at 0.980, and the only differences are page headers.
- **Reading level**: Flesch-Kincaid grade 6.1.
- **The repository's plain-language lint**: 2 descriptions, 0 warnings.
- **Related records**: none flagged.
- **Judge dry runs** on batches 02, 04, 07 and 09 showed only the intended rolls changing.

## Result

- Batch 09: the dry run planned 38 inserts, and the real run inserted 38 with no errors and
  nobody notified, under the stamp `2026-09-11T05:39:23.609Z`. The database holds 38 rows under
  that stamp. The convergence run reports all 38 unchanged. The 30 yes voters carry the tag;
  the 8 no voters carry none, because the no side is null.
- Batches 02 and 04 were re-imported (`import-rerun-report-2026-09-11.json`, stamps
  `2026-09-11T05:39:12.446Z` and `2026-09-11T05:39:17.768Z`). All 216 and 262 records came back
  unchanged; only tags moved. HB 3127's 22 yes voters carried both labels until the roll was
  retracted. SB 2084's 34 yes
  voters carry `labor_rights`, and its old spending tags are gone. The
  `import-dry-run-rerun-report.json` files in those two folders are now this review's check
  before the re-import.
- Oklahoma now holds 1,588 records from 63 rolls for 53 candidates, locally, after the HB 3127
  retraction.
