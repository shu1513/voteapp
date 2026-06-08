BEGIN;

ALTER TABLE public.elections
  ADD COLUMN IF NOT EXISTS election_night_results_checked_at timestamptz,
  ADD COLUMN IF NOT EXISTS certified_results_checked_at timestamptz;

ALTER TABLE public.candidate_elections
  DROP CONSTRAINT IF EXISTS chk_candidate_elections_status;

ALTER TABLE public.candidate_elections
  ADD CONSTRAINT chk_candidate_elections_status
  CHECK (status IN ('declared', 'withdrawn', 'won', 'lost', 'advanced', 'runoff'));

CREATE TABLE public.election_result_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state text NOT NULL,
  election_date date NOT NULL,
  pass_type text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  scheduled_for timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  source_summary jsonb,
  raw_payload jsonb,
  run_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_election_result_runs_state_nonempty
    CHECK (btrim(state) <> ''),
  CONSTRAINT chk_election_result_runs_pass_type
    CHECK (pass_type IN ('election_night', 'certified')),
  CONSTRAINT chk_election_result_runs_status
    CHECK (status IN ('pending', 'running', 'completed', 'partial', 'failed')),
  CONSTRAINT chk_election_result_runs_source_summary_json
    CHECK (source_summary IS NULL OR jsonb_typeof(source_summary) = 'object'),
  CONSTRAINT chk_election_result_runs_raw_payload_json
    CHECK (raw_payload IS NULL OR jsonb_typeof(raw_payload) = 'object')
);

CREATE INDEX idx_election_result_runs_state_date_pass
  ON public.election_result_runs (state, election_date, pass_type);

CREATE INDEX idx_election_result_runs_status_scheduled_for
  ON public.election_result_runs (status, scheduled_for);

DROP TRIGGER IF EXISTS trg_election_result_runs_set_updated_at
  ON public.election_result_runs;
CREATE TRIGGER trg_election_result_runs_set_updated_at
BEFORE UPDATE ON public.election_result_runs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.candidate_election_results (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  election_result_run_id uuid,
  pass_type text NOT NULL,
  candidate_election_id uuid NOT NULL,
  election_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  result_status text NOT NULL,
  outcome text NOT NULL,
  vote_count bigint,
  vote_percent numeric(7,4),
  rank integer,
  source_url text NOT NULL,
  source_type text NOT NULL,
  retrieved_at timestamptz NOT NULL DEFAULT now(),
  match_method text NOT NULL DEFAULT 'unknown',
  match_confidence numeric(5,4),
  raw_result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_candidate_election_results_run
    FOREIGN KEY (election_result_run_id)
    REFERENCES public.election_result_runs (id)
    ON DELETE SET NULL,
  CONSTRAINT fk_candidate_election_results_candidate_election
    FOREIGN KEY (candidate_election_id)
    REFERENCES public.candidate_elections (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_candidate_election_results_election
    FOREIGN KEY (election_id)
    REFERENCES public.elections (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_candidate_election_results_candidate
    FOREIGN KEY (candidate_id)
    REFERENCES public.candidates (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_candidate_election_results_candidate_election_pair
    FOREIGN KEY (candidate_id, election_id)
    REFERENCES public.candidate_elections (candidate_id, election_id)
    ON DELETE CASCADE,
  CONSTRAINT uq_candidate_election_results_candidate_pass
    UNIQUE (candidate_election_id, pass_type),
  CONSTRAINT chk_candidate_election_results_pass_type
    CHECK (pass_type IN ('election_night', 'certified')),
  CONSTRAINT chk_candidate_election_results_status
    CHECK (result_status IN (
      'projected',
      'unofficial_partial',
      'unofficial_complete',
      'certified',
      'recount',
      'correction',
      'not_found',
      'not_final_yet'
    )),
  CONSTRAINT chk_candidate_election_results_outcome
    CHECK (outcome IN (
      'leading',
      'trailing',
      'projected_winner',
      'too_close',
      'won',
      'lost',
      'advanced',
      'runoff',
      'unknown'
    )),
  CONSTRAINT chk_candidate_election_results_vote_count
    CHECK (vote_count IS NULL OR vote_count >= 0),
  CONSTRAINT chk_candidate_election_results_vote_percent
    CHECK (vote_percent IS NULL OR (vote_percent >= 0 AND vote_percent <= 100)),
  CONSTRAINT chk_candidate_election_results_rank
    CHECK (rank IS NULL OR rank > 0),
  CONSTRAINT chk_candidate_election_results_source_url_nonempty
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_candidate_election_results_source_type
    CHECK (source_type IN ('official', 'ap', 'news', 'other')),
  CONSTRAINT chk_candidate_election_results_match_method_nonempty
    CHECK (btrim(match_method) <> ''),
  CONSTRAINT chk_candidate_election_results_match_confidence
    CHECK (match_confidence IS NULL OR (match_confidence >= 0 AND match_confidence <= 1)),
  CONSTRAINT chk_candidate_election_results_raw_result_json
    CHECK (raw_result IS NULL OR jsonb_typeof(raw_result) = 'object')
);

CREATE INDEX idx_candidate_election_results_election_id
  ON public.candidate_election_results (election_id);

CREATE INDEX idx_candidate_election_results_candidate_id
  ON public.candidate_election_results (candidate_id);

CREATE INDEX idx_candidate_election_results_run_id
  ON public.candidate_election_results (election_result_run_id);

DROP TRIGGER IF EXISTS trg_candidate_election_results_set_updated_at
  ON public.candidate_election_results;
CREATE TRIGGER trg_candidate_election_results_set_updated_at
BEFORE UPDATE ON public.candidate_election_results
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.unmatched_candidate_election_results (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  election_result_run_id uuid,
  pass_type text NOT NULL,
  election_id uuid NOT NULL,
  candidate_name text NOT NULL,
  party text,
  result_status text NOT NULL,
  outcome text NOT NULL,
  vote_count bigint,
  vote_percent numeric(7,4),
  rank integer,
  reason text NOT NULL,
  source_url text NOT NULL,
  source_type text NOT NULL,
  raw_result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_unmatched_candidate_election_results_run
    FOREIGN KEY (election_result_run_id)
    REFERENCES public.election_result_runs (id)
    ON DELETE SET NULL,
  CONSTRAINT fk_unmatched_candidate_election_results_election
    FOREIGN KEY (election_id)
    REFERENCES public.elections (id)
    ON DELETE CASCADE,
  CONSTRAINT chk_unmatched_candidate_election_results_pass_type
    CHECK (pass_type IN ('election_night', 'certified')),
  CONSTRAINT chk_unmatched_candidate_election_results_candidate_name_nonempty
    CHECK (btrim(candidate_name) <> ''),
  CONSTRAINT chk_unmatched_candidate_election_results_status
    CHECK (result_status IN (
      'projected',
      'unofficial_partial',
      'unofficial_complete',
      'certified',
      'recount',
      'correction',
      'not_found',
      'not_final_yet'
    )),
  CONSTRAINT chk_unmatched_candidate_election_results_outcome
    CHECK (outcome IN (
      'leading',
      'trailing',
      'projected_winner',
      'too_close',
      'won',
      'lost',
      'advanced',
      'runoff',
      'unknown'
    )),
  CONSTRAINT chk_unmatched_candidate_election_results_vote_count
    CHECK (vote_count IS NULL OR vote_count >= 0),
  CONSTRAINT chk_unmatched_candidate_election_results_vote_percent
    CHECK (vote_percent IS NULL OR (vote_percent >= 0 AND vote_percent <= 100)),
  CONSTRAINT chk_unmatched_candidate_election_results_rank
    CHECK (rank IS NULL OR rank > 0),
  CONSTRAINT chk_unmatched_candidate_election_results_reason_nonempty
    CHECK (btrim(reason) <> ''),
  CONSTRAINT chk_unmatched_candidate_election_results_source_url_nonempty
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_unmatched_candidate_election_results_source_type
    CHECK (source_type IN ('official', 'ap', 'news', 'other')),
  CONSTRAINT chk_unmatched_candidate_election_results_raw_result_json
    CHECK (raw_result IS NULL OR jsonb_typeof(raw_result) = 'object')
);

CREATE INDEX idx_unmatched_candidate_election_results_election_id
  ON public.unmatched_candidate_election_results (election_id);

CREATE INDEX idx_unmatched_candidate_election_results_run_id
  ON public.unmatched_candidate_election_results (election_result_run_id);

DROP TRIGGER IF EXISTS trg_unmatched_candidate_election_results_set_updated_at
  ON public.unmatched_candidate_election_results;
CREATE TRIGGER trg_unmatched_candidate_election_results_set_updated_at
BEFORE UPDATE ON public.unmatched_candidate_election_results
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.ballot_measure_results (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  election_result_run_id uuid,
  pass_type text NOT NULL,
  ballot_measure_id uuid NOT NULL,
  election_id uuid NOT NULL,
  result_status text NOT NULL,
  outcome text NOT NULL,
  yes_votes bigint,
  no_votes bigint,
  yes_percent numeric(7,4),
  no_percent numeric(7,4),
  source_url text NOT NULL,
  source_type text NOT NULL,
  retrieved_at timestamptz NOT NULL DEFAULT now(),
  raw_result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_ballot_measure_results_run
    FOREIGN KEY (election_result_run_id)
    REFERENCES public.election_result_runs (id)
    ON DELETE SET NULL,
  CONSTRAINT fk_ballot_measure_results_measure
    FOREIGN KEY (ballot_measure_id)
    REFERENCES public.ballot_measures (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_ballot_measure_results_election
    FOREIGN KEY (election_id)
    REFERENCES public.elections (id)
    ON DELETE CASCADE,
  CONSTRAINT uq_ballot_measure_results_measure_pass
    UNIQUE (ballot_measure_id, pass_type),
  CONSTRAINT chk_ballot_measure_results_pass_type
    CHECK (pass_type IN ('election_night', 'certified')),
  CONSTRAINT chk_ballot_measure_results_status
    CHECK (result_status IN (
      'projected',
      'unofficial_partial',
      'unofficial_complete',
      'certified',
      'recount',
      'correction',
      'not_found',
      'not_final_yet'
    )),
  CONSTRAINT chk_ballot_measure_results_outcome
    CHECK (outcome IN ('passing', 'failing', 'passed', 'failed', 'unknown')),
  CONSTRAINT chk_ballot_measure_results_yes_votes
    CHECK (yes_votes IS NULL OR yes_votes >= 0),
  CONSTRAINT chk_ballot_measure_results_no_votes
    CHECK (no_votes IS NULL OR no_votes >= 0),
  CONSTRAINT chk_ballot_measure_results_yes_percent
    CHECK (yes_percent IS NULL OR (yes_percent >= 0 AND yes_percent <= 100)),
  CONSTRAINT chk_ballot_measure_results_no_percent
    CHECK (no_percent IS NULL OR (no_percent >= 0 AND no_percent <= 100)),
  CONSTRAINT chk_ballot_measure_results_source_url_nonempty
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_ballot_measure_results_source_type
    CHECK (source_type IN ('official', 'ap', 'news', 'other')),
  CONSTRAINT chk_ballot_measure_results_raw_result_json
    CHECK (raw_result IS NULL OR jsonb_typeof(raw_result) = 'object')
);

CREATE INDEX idx_ballot_measure_results_election_id
  ON public.ballot_measure_results (election_id);

CREATE INDEX idx_ballot_measure_results_run_id
  ON public.ballot_measure_results (election_result_run_id);

DROP TRIGGER IF EXISTS trg_ballot_measure_results_set_updated_at
  ON public.ballot_measure_results;
CREATE TRIGGER trg_ballot_measure_results_set_updated_at
BEFORE UPDATE ON public.ballot_measure_results
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
