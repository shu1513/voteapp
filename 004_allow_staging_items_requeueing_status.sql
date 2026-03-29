DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_staging_items_status'
      AND conrelid = 'staging_items'::regclass
      AND pg_get_constraintdef(oid) ILIKE '%requeueing%'
  ) THEN
    RETURN;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_staging_items_status'
      AND conrelid = 'staging_items'::regclass
  ) THEN
    ALTER TABLE staging_items
    DROP CONSTRAINT chk_staging_items_status;
  END IF;

  ALTER TABLE staging_items
  ADD CONSTRAINT chk_staging_items_status
  CHECK (status IN ('pending', 'validated', 'rejected', 'written', 'failed', 'requeueing'));
END
$$;
