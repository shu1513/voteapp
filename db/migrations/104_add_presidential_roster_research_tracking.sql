BEGIN;

ALTER TABLE public.presidential_cycles
  ADD COLUMN IF NOT EXISTS roster_research_last_attempted_at timestamptz,
  ADD COLUMN IF NOT EXISTS roster_research_next_at timestamptz,
  ADD COLUMN IF NOT EXISTS roster_research_attempt_count integer,
  ADD COLUMN IF NOT EXISTS roster_research_last_status text,
  ADD COLUMN IF NOT EXISTS roster_research_last_error text;

UPDATE public.presidential_cycles
SET roster_research_attempt_count = 0
WHERE roster_research_attempt_count IS NULL;

ALTER TABLE public.presidential_cycles
  ALTER COLUMN roster_research_attempt_count SET DEFAULT 0,
  ALTER COLUMN roster_research_attempt_count SET NOT NULL;

ALTER TABLE public.presidential_cycles
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_roster_research_attempt_count,
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_roster_research_last_status,
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_roster_research_last_error;

ALTER TABLE public.presidential_cycles
  ADD CONSTRAINT chk_presidential_cycles_roster_research_attempt_count
    CHECK (roster_research_attempt_count >= 0),
  ADD CONSTRAINT chk_presidential_cycles_roster_research_last_status
    CHECK (
      roster_research_last_status IS NULL
      OR roster_research_last_status IN ('succeeded', 'failed')
    ),
  ADD CONSTRAINT chk_presidential_cycles_roster_research_last_error
    CHECK (
      roster_research_last_error IS NULL
      OR length(trim(roster_research_last_error)) > 0
    );

CREATE INDEX IF NOT EXISTS idx_presidential_cycles_roster_research_due
  ON public.presidential_cycles (roster_research_next_at)
  WHERE stage = 'primary'
    AND status = 'active';

COMMIT;
