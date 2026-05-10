ALTER TABLE staging_items
ADD COLUMN IF NOT EXISTS ai_raw_debug jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_staging_items_ai_raw_debug_json'
      AND conrelid = 'staging_items'::regclass
  ) THEN
    ALTER TABLE staging_items
    ADD CONSTRAINT chk_staging_items_ai_raw_debug_json
    CHECK (ai_raw_debug IS NULL OR jsonb_typeof(ai_raw_debug) = 'object');
  END IF;
END
$$;
