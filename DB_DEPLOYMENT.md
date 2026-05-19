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

## Notes

- Migration runner enforces checksum consistency for already-applied files.
- Do not edit historical migration files after they are applied in shared environments.
- Add new migrations as new numbered files (e.g., `029_...sql`).
