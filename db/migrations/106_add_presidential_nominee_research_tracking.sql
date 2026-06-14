BEGIN;

ALTER TABLE public.presidential_cycles
  ADD COLUMN IF NOT EXISTS nominee_research_last_attempted_at timestamptz,
  ADD COLUMN IF NOT EXISTS nominee_research_next_at timestamptz,
  ADD COLUMN IF NOT EXISTS nominee_research_attempt_count integer,
  ADD COLUMN IF NOT EXISTS nominee_research_last_status text,
  ADD COLUMN IF NOT EXISTS nominee_research_last_error text;

UPDATE public.presidential_cycles
SET nominee_research_attempt_count = 0
WHERE nominee_research_attempt_count IS NULL;

ALTER TABLE public.presidential_cycles
  ALTER COLUMN nominee_research_attempt_count SET DEFAULT 0,
  ALTER COLUMN nominee_research_attempt_count SET NOT NULL;

ALTER TABLE public.presidential_cycles
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_nominee_research_attempt_count,
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_nominee_research_last_status,
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_nominee_research_last_error;

ALTER TABLE public.presidential_cycles
  ADD CONSTRAINT chk_presidential_cycles_nominee_research_attempt_count
    CHECK (nominee_research_attempt_count >= 0),
  ADD CONSTRAINT chk_presidential_cycles_nominee_research_last_status
    CHECK (
      nominee_research_last_status IS NULL
      OR nominee_research_last_status IN ('succeeded', 'failed')
    ),
  ADD CONSTRAINT chk_presidential_cycles_nominee_research_last_error
    CHECK (
      nominee_research_last_error IS NULL
      OR length(trim(nominee_research_last_error)) > 0
    );

CREATE INDEX IF NOT EXISTS idx_presidential_cycles_nominee_research_due
  ON public.presidential_cycles (nominee_research_next_at)
  WHERE stage = 'primary'
    AND status = 'active';

COMMIT;
