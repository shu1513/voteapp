BEGIN;

ALTER TABLE public.presidential_cycles
  ADD COLUMN IF NOT EXISTS nominee_candidate_id uuid,
  ADD COLUMN IF NOT EXISTS nominee_confirmed_at timestamptz,
  ADD COLUMN IF NOT EXISTS nominee_sources jsonb;

UPDATE public.presidential_cycles
SET nominee_sources = '[]'::jsonb
WHERE nominee_sources IS NULL;

ALTER TABLE public.presidential_cycles
  ALTER COLUMN nominee_sources SET DEFAULT '[]'::jsonb,
  ALTER COLUMN nominee_sources SET NOT NULL;

ALTER TABLE public.presidential_cycles
  DROP CONSTRAINT IF EXISTS fk_presidential_cycles_nominee_candidate,
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_nominee_sources_json,
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_nominee_primary_only,
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_nominee_complete_state;

ALTER TABLE public.presidential_cycles
  ADD CONSTRAINT fk_presidential_cycles_nominee_candidate
    FOREIGN KEY (nominee_candidate_id)
    REFERENCES public.candidates (id),
  ADD CONSTRAINT chk_presidential_cycles_nominee_sources_json
    CHECK (jsonb_typeof(nominee_sources) = 'array'),
  ADD CONSTRAINT chk_presidential_cycles_nominee_primary_only
    CHECK (nominee_candidate_id IS NULL OR stage = 'primary'),
  ADD CONSTRAINT chk_presidential_cycles_nominee_complete_state
    CHECK (
      (
        nominee_candidate_id IS NULL
        AND nominee_confirmed_at IS NULL
        AND nominee_sources = '[]'::jsonb
      )
      OR
      (
        nominee_candidate_id IS NOT NULL
        AND nominee_confirmed_at IS NOT NULL
        AND status = 'completed'
      )
    );

CREATE INDEX IF NOT EXISTS idx_presidential_cycles_nominee_candidate
  ON public.presidential_cycles (nominee_candidate_id)
  WHERE nominee_candidate_id IS NOT NULL;

COMMIT;
