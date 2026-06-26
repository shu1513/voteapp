# Tennessee Campaign Finance

Tennessee campaign finance ingestion is rolled out behind flags only:

- `TENNESSEE_CAMPAIGN_FINANCE_ENABLED` controls ballot lookup visibility.
- `TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED` controls sync and scheduler workers.

Initial eligible offices are Governor, State Senate, and State House. The CAMP resolver matches normalized candidate name, office, district when applicable, and election year. Ambiguous CAMP matches are skipped.

## Data Slices

Phase 1 writes direct candidate contribution summaries and top direct breakdowns from CAMP contribution exports.

Phase 2 writes independent expenditure support/opposition from broad CAMP expenditure exports. The sync locally filters rows where `Type` is independent, `Candidate For` matches the candidate, and `S/O` is support or opposition. It stores support/oppose totals, outside group amounts, expenditure counts, and source URLs.

Phase 3 writes outside group contributor evidence for the PACs/groups found in Phase 2. Tennessee does not publish a direct industry field, so industries are inferred conservatively from organization donor names and employer labels through the shared finance label classification path. Individual names are not used as industry evidence unless employer data exists. Occupation labels are stored as outside group contribution evidence, but are not treated as industry labels.

## BullMQ Scheduler

Tennessee sync uses an isolated BullMQ queue:

- `TENNESSEE_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE`
- `TENNESSEE_CAMPAIGN_FINANCE_SYNC_DAILY_CRON`
- `TENNESSEE_CAMPAIGN_FINANCE_SYNC_DAILY_TZ`

Scripts:

- `npm run tennessee-candidates:finance:scheduler:upsert`
- `npm run tennessee-candidates:finance:scheduler:worker`
- `npm run tennessee-candidates:finance:scheduler:trigger`
- `npm run tennessee-candidates:finance:sync-due`
- `npm run tennessee-candidates:finance:probe`

When `TENNESSEE_CAMPAIGN_FINANCE_ENABLED=false`, the recurring scheduler upsert removes the Tennessee job scheduler and the worker exits without processing. When `TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED=false`, manual enqueue and job execution are no-ops unless explicitly forced, and force still cannot bypass the master Tennessee flag.

## Validation Gates

Before enabling in staging or production, run a low-limit dry run and the live probe for:

- a known direct-only candidate,
- a candidate with independent expenditures,
- an outside PAC/group with classifiable contributors,
- a dry-run batch sync with low candidate limits.

Keep both Tennessee flags disabled by default, then enable first in staging with low scheduler limits.
