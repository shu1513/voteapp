BEGIN;

DROP TABLE IF EXISTS public.candidate_election_results;
DROP TABLE IF EXISTS public.unmatched_candidate_election_results;

CREATE TABLE public.election_results (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  election_result_run_id uuid,
  pass_type text NOT NULL,
  election_id uuid NOT NULL,
  result_status text NOT NULL,
  outcome text NOT NULL,
  winners jsonb NOT NULL DEFAULT '[]'::jsonb,
  match_status text NOT NULL DEFAULT 'unmatched',
  source_url text NOT NULL,
  source_type text NOT NULL,
  retrieved_at timestamptz NOT NULL DEFAULT now(),
  raw_result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_election_results_run
    FOREIGN KEY (election_result_run_id)
    REFERENCES public.election_result_runs (id)
    ON DELETE SET NULL,
  CONSTRAINT fk_election_results_election
    FOREIGN KEY (election_id)
    REFERENCES public.elections (id)
    ON DELETE CASCADE,
  CONSTRAINT uq_election_results_election_pass
    UNIQUE (election_id, pass_type),
  CONSTRAINT chk_election_results_pass_type
    CHECK (pass_type IN ('election_night', 'certified')),
  CONSTRAINT chk_election_results_status
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
  CONSTRAINT chk_election_results_outcome
    CHECK (outcome IN (
      'leading',
      'projected_winner',
      'too_close',
      'won',
      'advanced',
      'runoff',
      'unknown'
    )),
  CONSTRAINT chk_election_results_winners_json
    CHECK (jsonb_typeof(winners) = 'array'),
  CONSTRAINT chk_election_results_match_status
    CHECK (match_status IN ('matched', 'partial', 'unmatched', 'not_applicable', 'not_found')),
  CONSTRAINT chk_election_results_source_url_nonempty
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_election_results_source_type
    CHECK (source_type IN ('official', 'ap', 'news', 'other')),
  CONSTRAINT chk_election_results_raw_result_json
    CHECK (raw_result IS NULL OR jsonb_typeof(raw_result) = 'object')
);

CREATE INDEX idx_election_results_election_id
  ON public.election_results (election_id);

CREATE INDEX idx_election_results_run_id
  ON public.election_results (election_result_run_id);

CREATE INDEX idx_election_results_match_status
  ON public.election_results (match_status);

DROP TRIGGER IF EXISTS trg_election_results_set_updated_at
  ON public.election_results;
CREATE TRIGGER trg_election_results_set_updated_at
BEFORE UPDATE ON public.election_results
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

ALTER TABLE public.ballot_measure_results
  DROP COLUMN IF EXISTS yes_votes,
  DROP COLUMN IF EXISTS no_votes,
  DROP COLUMN IF EXISTS yes_percent,
  DROP COLUMN IF EXISTS no_percent;

COMMIT;
