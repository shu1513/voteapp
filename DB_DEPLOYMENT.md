# Database Deployment Notes (Current Approach)

This repo currently uses SQL files directly (no migration framework yet).

## Why this matters

Application code now depends on these `staging_items` columns for `state_resources` pipeline:
- `schema_version`
- `prompt_version`
- `failure_debug`
- `ai_raw_debug`

If those columns/constraints are missing in an environment, producer/validator/writer behavior will break.

## Safe deploy order

1. Apply database schema changes first.
2. Deploy backend code second.
3. Run pipeline jobs after both are in sync.

## New environment (local, fresh staging, fresh prod clone)

Run:

```bash
cd /path/to/voteApp
psql -d <db_name> -f ./001_init.sql
```

## Existing environment with data (manual patch for this change)

Run this before deploying backend code that expects `schema_version`:

```sql
ALTER TABLE staging_items
  ADD COLUMN IF NOT EXISTS schema_version text;

ALTER TABLE staging_items
  ADD COLUMN IF NOT EXISTS prompt_version text;

ALTER TABLE staging_items
  ADD COLUMN IF NOT EXISTS failure_debug jsonb;

ALTER TABLE staging_items
  ADD COLUMN IF NOT EXISTS ai_raw_debug jsonb;

ALTER TABLE staging_items
  DROP CONSTRAINT IF EXISTS chk_staging_items_state_resources_metadata;

ALTER TABLE staging_items
  ADD CONSTRAINT chk_staging_items_state_resources_metadata
  CHECK (
    item_type <> 'state_resources'
    OR (
      schema_version IS NOT NULL AND btrim(schema_version) <> ''
      AND prompt_version IS NOT NULL AND btrim(prompt_version) <> ''
    )
  );

ALTER TABLE staging_items
  DROP CONSTRAINT IF EXISTS chk_staging_items_failure_debug_json;

ALTER TABLE staging_items
  ADD CONSTRAINT chk_staging_items_failure_debug_json
  CHECK (failure_debug IS NULL OR jsonb_typeof(failure_debug) = 'object');

ALTER TABLE staging_items
  DROP CONSTRAINT IF EXISTS chk_staging_items_ai_raw_debug_json;

ALTER TABLE staging_items
  ADD CONSTRAINT chk_staging_items_ai_raw_debug_json
  CHECK (ai_raw_debug IS NULL OR jsonb_typeof(ai_raw_debug) = 'object');
```

To remove obsolete district columns in existing environments:

```bash
psql -d <db_name> -f ./005_drop_district_registered_voters_and_boundary_data.sql
```

To migrate district type values (`city` -> `place`, `school` -> `school_unified`) and enable school subtypes:

```bash
psql -d <db_name> -f ./006_migrate_district_type_place_and_school_variants.sql
```

To rename district identifier column and relax uniqueness safely for compact GEOIDs:

```bash
psql -d <db_name> -f ./007_rename_district_fips_code_to_geoid_compact.sql
```

If `008_rename_district_type_place_to_incorporated_place.sql` was applied, migrate district type back to `place`:

```bash
psql -d <db_name> -f ./009_rename_district_type_incorporated_place_to_place.sql
```

## Pipeline stream expectations

- Producer writes draft items to `staging:draft` only.
- Validator reads only `staging:pending` (enriched items).
- Writer reads only `staging:validated`.

This avoids draft rows being rejected as if they were enriched payloads.

## Later improvement

Adopt a real migration tool (for example `node-pg-migrate`) and move schema changes to versioned migration files.
