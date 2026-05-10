ALTER TABLE elections
  ADD COLUMN IF NOT EXISTS sources jsonb NOT NULL DEFAULT '[]'::jsonb;

DO $$
DECLARE
  invalid_type_count bigint;
  empty_array_count bigint;
BEGIN
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
