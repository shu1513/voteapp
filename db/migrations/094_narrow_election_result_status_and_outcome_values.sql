BEGIN;

UPDATE public.election_results
SET result_status = CASE result_status
    WHEN 'unofficial_partial' THEN 'unofficial'
    WHEN 'unofficial_complete' THEN 'unofficial'
    WHEN 'recount' THEN 'not_final_yet'
    WHEN 'correction' THEN 'certified'
    ELSE result_status
  END,
  outcome = CASE outcome
    WHEN 'leading' THEN 'unknown'
    WHEN 'projected_winner' THEN 'won'
    ELSE outcome
  END;

UPDATE public.ballot_measure_results
SET result_status = CASE result_status
    WHEN 'unofficial_partial' THEN 'unofficial'
    WHEN 'unofficial_complete' THEN 'unofficial'
    WHEN 'recount' THEN 'not_final_yet'
    WHEN 'correction' THEN 'certified'
    ELSE result_status
  END,
  outcome = CASE outcome
    WHEN 'passing' THEN 'passed'
    WHEN 'failing' THEN 'failed'
    ELSE outcome
  END;

ALTER TABLE public.election_results
  DROP CONSTRAINT IF EXISTS chk_election_results_status,
  DROP CONSTRAINT IF EXISTS chk_election_results_outcome;

ALTER TABLE public.election_results
  ADD CONSTRAINT chk_election_results_status
  CHECK (result_status IN (
    'projected',
    'unofficial',
    'certified',
    'not_found',
    'not_final_yet'
  )),
  ADD CONSTRAINT chk_election_results_outcome
  CHECK (outcome IN (
    'too_close',
    'won',
    'advanced',
    'runoff',
    'unknown'
  ));

ALTER TABLE public.ballot_measure_results
  DROP CONSTRAINT IF EXISTS chk_ballot_measure_results_status,
  DROP CONSTRAINT IF EXISTS chk_ballot_measure_results_outcome;

ALTER TABLE public.ballot_measure_results
  ADD CONSTRAINT chk_ballot_measure_results_status
  CHECK (result_status IN (
    'projected',
    'unofficial',
    'certified',
    'not_found',
    'not_final_yet'
  )),
  ADD CONSTRAINT chk_ballot_measure_results_outcome
  CHECK (outcome IN ('passed', 'failed', 'unknown'));

COMMIT;
