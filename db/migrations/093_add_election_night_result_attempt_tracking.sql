BEGIN;

ALTER TABLE public.elections
  ADD COLUMN IF NOT EXISTS election_night_results_attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS election_night_results_last_attempted_at timestamptz;

ALTER TABLE public.elections
  DROP CONSTRAINT IF EXISTS chk_elections_election_night_results_attempt_count;

ALTER TABLE public.elections
  ADD CONSTRAINT chk_elections_election_night_results_attempt_count
  CHECK (election_night_results_attempt_count >= 0);

COMMIT;
