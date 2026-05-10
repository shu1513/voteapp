-- Safety migration for mixed deployment histories:
-- - Fresh databases created from 001_init.sql already have elections.sources.
-- - Older/divergent environments may still be missing the column.
-- This file is idempotent and safe in both cases:
-- it ensures the column exists, validates existing data, enforces NOT NULL +
-- non-empty array constraint, and removes any conflicting default.
ALTER TABLE elections
  ADD COLUMN IF NOT EXISTS sources jsonb;

DO $$
DECLARE
  null_count bigint;
  invalid_type_count bigint;
  empty_array_count bigint;
BEGIN
  SELECT COUNT(*) INTO null_count
  FROM elections
  WHERE sources IS NULL;

  IF null_count > 0 THEN
    RAISE EXCEPTION
      'Cannot enforce non-empty elections.sources: % row(s) have NULL sources. Backfill sources first.',
      null_count;
  END IF;

  SELECT COUNT(*) INTO invalid_type_count
  FROM elections
  WHERE jsonb_typeof(sources) <> 'array';

  IF invalid_type_count > 0 THEN
    RAISE EXCEPTION
      'Cannot enforce non-empty elections.sources: % row(s) are not JSON arrays',
      invalid_type_count;
  END IF;

  SELECT COUNT(*) INTO empty_array_count
  FROM elections
  WHERE jsonb_array_length(sources) = 0;

  IF empty_array_count > 0 THEN
    RAISE EXCEPTION
      'Cannot enforce non-empty elections.sources: % row(s) have empty arrays. Backfill sources first.',
      empty_array_count;
  END IF;

  ALTER TABLE elections
  ALTER COLUMN sources SET NOT NULL;

  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_elections_sources_json'
      AND conrelid = 'elections'::regclass
  ) THEN
    ALTER TABLE elections
    DROP CONSTRAINT chk_elections_sources_json;
  END IF;

  ALTER TABLE elections
  ADD CONSTRAINT chk_elections_sources_json
  CHECK (jsonb_typeof(sources) = 'array' AND jsonb_array_length(sources) > 0);
END
$$;

ALTER TABLE elections
ALTER COLUMN sources DROP DEFAULT;
