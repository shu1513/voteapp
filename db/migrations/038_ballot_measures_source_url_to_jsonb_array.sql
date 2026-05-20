BEGIN;

ALTER TABLE public.ballot_measures
  ALTER COLUMN source_url TYPE jsonb
  USING CASE
    WHEN source_url IS NULL OR btrim(source_url) = '' THEN '[]'::jsonb
    WHEN left(btrim(source_url), 1) = '[' THEN source_url::jsonb
    ELSE jsonb_build_array(source_url)
  END;

ALTER TABLE public.ballot_measures
  ALTER COLUMN source_url SET DEFAULT '[]'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_ballot_measures_source_url_array'
  ) THEN
    ALTER TABLE public.ballot_measures
      ADD CONSTRAINT chk_ballot_measures_source_url_array
      CHECK (jsonb_typeof(source_url) = 'array');
  END IF;
END$$;

COMMIT;
