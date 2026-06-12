BEGIN;

ALTER TABLE public.presidential_state_primary_dates
  ALTER COLUMN primary_date DROP NOT NULL,
  ADD COLUMN IF NOT EXISTS date_research_status text,
  ADD COLUMN IF NOT EXISTS last_researched_at timestamptz,
  ADD COLUMN IF NOT EXISTS next_research_at timestamptz,
  ADD COLUMN IF NOT EXISTS research_attempt_count integer,
  ADD COLUMN IF NOT EXISTS last_research_summary text,
  ADD COLUMN IF NOT EXISTS last_research_error text;

UPDATE public.presidential_state_primary_dates
SET date_research_status = CASE
    WHEN primary_date IS NOT NULL THEN 'official_found'
    ELSE 'pending'
  END
WHERE date_research_status IS NULL
   OR length(trim(date_research_status)) = 0;

UPDATE public.presidential_state_primary_dates
SET research_attempt_count = 0
WHERE research_attempt_count IS NULL;

UPDATE public.presidential_state_primary_dates
SET next_research_at = NULL
WHERE date_research_status = 'official_found'
  AND next_research_at IS NOT NULL;

ALTER TABLE public.presidential_state_primary_dates
  ALTER COLUMN date_research_status SET DEFAULT 'pending',
  ALTER COLUMN date_research_status SET NOT NULL,
  ALTER COLUMN research_attempt_count SET DEFAULT 0,
  ALTER COLUMN research_attempt_count SET NOT NULL;

ALTER TABLE public.presidential_state_primary_dates
  DROP CONSTRAINT IF EXISTS chk_presidential_state_primary_dates_date_research_status,
  DROP CONSTRAINT IF EXISTS chk_presidential_state_primary_dates_research_attempt_count,
  DROP CONSTRAINT IF EXISTS chk_presidential_state_primary_dates_primary_date_status,
  DROP CONSTRAINT IF EXISTS chk_presidential_state_primary_dates_official_found_next_research,
  DROP CONSTRAINT IF EXISTS chk_presidential_state_primary_dates_last_research_summary_text,
  DROP CONSTRAINT IF EXISTS chk_presidential_state_primary_dates_last_research_error_text;

ALTER TABLE public.presidential_state_primary_dates
  ADD CONSTRAINT chk_presidential_state_primary_dates_date_research_status
    CHECK (date_research_status IN ('pending', 'not_official_yet', 'official_found', 'error')),
  ADD CONSTRAINT chk_presidential_state_primary_dates_research_attempt_count
    CHECK (research_attempt_count >= 0),
  ADD CONSTRAINT chk_presidential_state_primary_dates_primary_date_status
    CHECK ((date_research_status = 'official_found') = (primary_date IS NOT NULL)),
  ADD CONSTRAINT chk_presidential_state_primary_dates_official_found_next_research
    CHECK (date_research_status <> 'official_found' OR next_research_at IS NULL),
  ADD CONSTRAINT chk_presidential_state_primary_dates_last_research_summary_text
    CHECK (last_research_summary IS NULL OR length(trim(last_research_summary)) > 0),
  ADD CONSTRAINT chk_presidential_state_primary_dates_last_research_error_text
    CHECK (last_research_error IS NULL OR length(trim(last_research_error)) > 0);

CREATE INDEX IF NOT EXISTS idx_presidential_state_primary_dates_research_due
  ON public.presidential_state_primary_dates (date_research_status, next_research_at)
  WHERE date_research_status <> 'official_found';

COMMIT;
