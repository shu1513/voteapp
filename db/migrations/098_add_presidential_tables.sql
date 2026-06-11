BEGIN;

CREATE TABLE IF NOT EXISTS public.presidential_cycles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  election_year integer NOT NULL,
  stage text NOT NULL,
  party text,
  election_date date,
  status text NOT NULL DEFAULT 'upcoming',
  sources jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_presidential_cycles_election_year
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT chk_presidential_cycles_stage
    CHECK (stage IN ('primary', 'general')),
  CONSTRAINT chk_presidential_cycles_party_stage
    CHECK (
      (stage = 'general' AND party IS NULL AND election_date IS NOT NULL)
      OR
      (stage = 'primary' AND party IS NOT NULL AND length(trim(party)) > 0 AND election_date IS NULL)
    ),
  CONSTRAINT chk_presidential_cycles_status
    CHECK (status IN ('upcoming', 'active', 'completed')),
  CONSTRAINT chk_presidential_cycles_sources_json
    CHECK (jsonb_typeof(sources) = 'array')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_presidential_cycles_primary_party
  ON public.presidential_cycles (election_year, party)
  WHERE stage = 'primary';

CREATE UNIQUE INDEX IF NOT EXISTS uq_presidential_cycles_general
  ON public.presidential_cycles (election_year)
  WHERE stage = 'general';

CREATE INDEX IF NOT EXISTS idx_presidential_cycles_year_stage
  ON public.presidential_cycles (election_year, stage);

DROP TRIGGER IF EXISTS trg_presidential_cycles_set_updated_at
  ON public.presidential_cycles;
CREATE TRIGGER trg_presidential_cycles_set_updated_at
BEFORE UPDATE ON public.presidential_cycles
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.presidential_cycle_candidates (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  cycle_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  party text NOT NULL,
  running_mate_candidate_id uuid,
  status text NOT NULL DEFAULT 'active',
  sources jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_presidential_cycle_candidates_cycle
    FOREIGN KEY (cycle_id)
    REFERENCES public.presidential_cycles (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_presidential_cycle_candidates_candidate
    FOREIGN KEY (candidate_id)
    REFERENCES public.candidates (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_presidential_cycle_candidates_running_mate
    FOREIGN KEY (running_mate_candidate_id)
    REFERENCES public.candidates (id)
    ON DELETE SET NULL,
  CONSTRAINT uq_presidential_cycle_candidates_cycle_candidate
    UNIQUE (cycle_id, candidate_id),
  CONSTRAINT chk_presidential_cycle_candidates_party_nonempty
    CHECK (length(trim(party)) > 0),
  CONSTRAINT chk_presidential_cycle_candidates_not_own_running_mate
    CHECK (running_mate_candidate_id IS NULL OR candidate_id <> running_mate_candidate_id),
  CONSTRAINT chk_presidential_cycle_candidates_status
    CHECK (status IN ('active', 'withdrawn')),
  CONSTRAINT chk_presidential_cycle_candidates_sources_json
    CHECK (jsonb_typeof(sources) = 'array')
);

CREATE INDEX IF NOT EXISTS idx_presidential_cycle_candidates_cycle
  ON public.presidential_cycle_candidates (cycle_id);

CREATE INDEX IF NOT EXISTS idx_presidential_cycle_candidates_candidate
  ON public.presidential_cycle_candidates (candidate_id);

CREATE INDEX IF NOT EXISTS idx_presidential_cycle_candidates_running_mate
  ON public.presidential_cycle_candidates (running_mate_candidate_id)
  WHERE running_mate_candidate_id IS NOT NULL;

DROP TRIGGER IF EXISTS trg_presidential_cycle_candidates_set_updated_at
  ON public.presidential_cycle_candidates;
CREATE TRIGGER trg_presidential_cycle_candidates_set_updated_at
BEFORE UPDATE ON public.presidential_cycle_candidates
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.presidential_state_primary_dates (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  cycle_id uuid NOT NULL,
  state_fips text NOT NULL,
  primary_date date NOT NULL,
  sources jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_presidential_state_primary_dates_cycle
    FOREIGN KEY (cycle_id)
    REFERENCES public.presidential_cycles (id)
    ON DELETE CASCADE,
  CONSTRAINT uq_presidential_state_primary_dates_cycle_state
    UNIQUE (cycle_id, state_fips),
  CONSTRAINT chk_presidential_state_primary_dates_state_fips
    CHECK (state_fips ~ '^[0-9]{2}$'),
  CONSTRAINT chk_presidential_state_primary_dates_sources_json
    CHECK (jsonb_typeof(sources) = 'array')
);

CREATE INDEX IF NOT EXISTS idx_presidential_state_primary_dates_state
  ON public.presidential_state_primary_dates (state_fips, primary_date);

DROP TRIGGER IF EXISTS trg_presidential_state_primary_dates_set_updated_at
  ON public.presidential_state_primary_dates;
CREATE TRIGGER trg_presidential_state_primary_dates_set_updated_at
BEFORE UPDATE ON public.presidential_state_primary_dates
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE OR REPLACE FUNCTION public.enforce_presidential_primary_date_cycle()
RETURNS trigger AS $$
DECLARE
  cycle_stage text;
BEGIN
  SELECT stage
  INTO cycle_stage
  FROM public.presidential_cycles
  WHERE id = NEW.cycle_id;

  IF cycle_stage IS NULL THEN
    RAISE EXCEPTION 'Missing presidential cycle for primary date: %', NEW.cycle_id;
  END IF;

  IF cycle_stage <> 'primary' THEN
    RAISE EXCEPTION 'presidential_state_primary_dates can only reference primary cycles; got %', cycle_stage;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_presidential_state_primary_dates_primary_cycle
  ON public.presidential_state_primary_dates;
CREATE TRIGGER trg_presidential_state_primary_dates_primary_cycle
BEFORE INSERT OR UPDATE OF cycle_id ON public.presidential_state_primary_dates
FOR EACH ROW
EXECUTE FUNCTION public.enforce_presidential_primary_date_cycle();

CREATE OR REPLACE FUNCTION public.prevent_presidential_cycle_stage_invalidating_primary_dates()
RETURNS trigger AS $$
BEGIN
  IF OLD.stage = 'primary'
     AND NEW.stage <> 'primary'
     AND EXISTS (
       SELECT 1
       FROM public.presidential_state_primary_dates AS primary_date
       WHERE primary_date.cycle_id = OLD.id
     ) THEN
    RAISE EXCEPTION 'Cannot change presidential cycle % away from primary while primary dates reference it', OLD.id;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_presidential_cycles_preserve_primary_date_parent
  ON public.presidential_cycles;
CREATE TRIGGER trg_presidential_cycles_preserve_primary_date_parent
BEFORE UPDATE OF stage ON public.presidential_cycles
FOR EACH ROW
EXECUTE FUNCTION public.prevent_presidential_cycle_stage_invalidating_primary_dates();

COMMIT;
