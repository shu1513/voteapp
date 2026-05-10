ALTER TABLE elections
  ADD COLUMN IF NOT EXISTS sources jsonb NOT NULL DEFAULT '[]'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_elections_sources_json'
      AND conrelid = 'elections'::regclass
  ) THEN
    ALTER TABLE elections
    ADD CONSTRAINT chk_elections_sources_json
    CHECK (jsonb_typeof(sources) = 'array');
  END IF;
END
$$;
