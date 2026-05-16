DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'elections'
      AND column_name = 'title'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'elections'
      AND column_name = 'official_ballot_title'
  ) THEN
    ALTER TABLE elections
      RENAME COLUMN title TO official_ballot_title;
  END IF;
END $$;

ALTER TABLE elections
  DROP COLUMN IF EXISTS last_researched;
