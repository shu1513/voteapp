BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'districts'
      AND column_name = 'fips_code'
  ) THEN
    ALTER TABLE districts
      RENAME COLUMN fips_code TO geoid_compact;
  END IF;
END
$$;

ALTER TABLE districts
  DROP CONSTRAINT IF EXISTS districts_fips_code_key;

ALTER TABLE districts
  DROP CONSTRAINT IF EXISTS uq_districts_type_geoid_compact;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_districts_type_geoid_compact'
      AND conrelid = 'districts'::regclass
  ) THEN
    ALTER TABLE districts
      ADD CONSTRAINT uq_districts_type_geoid_compact
      UNIQUE (district_type, geoid_compact);
  END IF;
END
$$;

COMMIT;
