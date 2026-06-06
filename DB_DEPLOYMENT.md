# Database Deployment (Tracked Migrations)

This project now uses a lightweight migration tracker:
- SQL files remain in `db/migrations/`
- applied history is stored in DB table `schema_migrations`
- runner script: `backend/src/scripts/dbMigrate.ts`

## Commands

From `backend/`:

```bash
npm run db:migrate:status
npm run db:migrate
npm run db:migrate:baseline
```

What each does:
- `db:migrate:status`: shows applied/pending migration files.
- `db:migrate`: applies pending SQL files in filename order and records each in `schema_migrations`.
- `db:migrate:baseline`: marks all current migration files as already applied **without executing SQL**.

## When to use baseline

Use baseline once for an existing environment where migrations were applied manually in the past.

Typical one-time flow for an existing DB:
1. Ensure schema is already in expected state.
2. Run `npm run db:migrate:baseline`.
3. From then on, use only `npm run db:migrate`.

Do **not** run baseline on fresh DBs.

## Fresh environment

For a fresh DB:

```bash
cd backend
npm run db:migrate
```

This applies all migration files from `001_init.sql` onward and records them.

## Safe deploy order

1. Apply DB migrations first.
2. Deploy backend code second.
3. Run pipeline jobs after both are in sync.

## Seeded Domain Data Order

After `db:migrate`, run domain seed scripts in this order:

1. `npm run elections:offices:seed`
2. `npm run db:seed:research-areas`
3. `npm run db:seed:office-research-areas`

This order ensures office and alias rows exist before office-to-research-area mappings are seeded.

## Candidate Record Rollover Rollout

Candidate record research now has a daily rollover scheduler and a feature flag gate.

### New scripts

From `backend/`:

1. `npm run candidates:record:produce-rollover`
2. `npm run candidates:record:scheduler:upsert`
3. `npm run candidates:record:scheduler:worker`
4. `npm run candidates:record:scheduler:trigger`

### Recommended rollout order

1. Deploy code with `CANDIDATE_RECORD_ENABLE_DAILY_ROLLOVER_PRODUCER=false`.
2. Start the scheduler worker process.
3. Upsert the recurring daily scheduler job.
4. Trigger one manual dry run window by setting low cap first:
   - `CANDIDATE_RECORDS_ROLLOVER_MAX_ENQUEUE=100`
   - `npm run candidates:record:scheduler:trigger -- --force`
   - Without `--force`, this trigger is a no-op while `CANDIDATE_RECORD_ENABLE_DAILY_ROLLOVER_PRODUCER=false`.
5. Enable `CANDIDATE_RECORD_ENABLE_DAILY_ROLLOVER_PRODUCER=true`.
6. Observe emitted/skipped counts for 1-2 daily cycles, then raise cap.

### Candidate record rollover env vars

1. `CANDIDATE_RECORD_ENABLE_DAILY_ROLLOVER_PRODUCER` (default `false`)
2. `CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS` (default `30`)
3. `CANDIDATE_RECORDS_ROLLOVER_MAX_ENQUEUE` (default `2000`)
4. `CANDIDATE_RECORDS_OVERLAP_DAYS` (default `45`)

### Candidate record live AI validation

From `backend/` (non-prod DB/Redis only):

1. AI-level probe (discovery + combined schema/url repair + area labeling):
   - `npm run candidates:record:live:ai-probe`
   - Optional target: `npm run candidates:record:live:ai-probe -- --candidate-id=<uuid> --election-id=<uuid>`
2. Pipeline smoke (enqueue one candidate/election draft, run enricher, snapshot before/after):
   - `npm run candidates:record:live:pipeline-smoke`
   - Optional target: `npm run candidates:record:live:pipeline-smoke -- --candidate-id=<uuid> --election-id=<uuid> --loops=3 --batch-size=50`

## Notes

- Migration runner enforces checksum consistency for already-applied files.
- Do not edit historical migration files after they are applied in shared environments.
- Add new migrations as new numbered files (e.g., `029_...sql`).
- Ballot-measure detail rows are intentionally constrained to 0-or-1 per `elections.id` (`UNIQUE (election_id)`), as enforced by migration `035_propositions_unique_election_id.sql`.
- Migration `087_prevent_office_alias_reassignment.sql` prevents changing `office_title_aliases.office_id`.
  Future migrations that intentionally rehome aliases must handle that explicitly instead of using `ON CONFLICT ... DO UPDATE SET office_id`.

## Candidate record identity migrations

Candidate record identity is now managed entirely through forward migrations:

1. `066_drop_candidate_records_source_name.sql` moved identity keys to the old v2 shape.
2. `067_fix_candidate_record_v2_title_normalization.sql` fixed v2 SQL/runtime normalization parity.
3. `069_drop_candidate_records_title_and_rekey_v3.sql` rekeys records to the current v3 shape based on `description`, `source_url`, and `event_date`, rehomes area tags for duplicate rows, and drops `candidate_records.title`.

The old migration-066 helper scripts were removed because they queried `candidate_records.title` and are invalid after `069`. For current deployments, run `npm run db:migrate`; do not run obsolete v2 preflight/rehome commands.
