BEGIN;

LOCK TABLE public.ballot_measure_results IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.ballot_measures IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
  mismatched_count integer;
BEGIN
  SELECT COUNT(*)
  INTO mismatched_count
  FROM public.ballot_measure_results result
  LEFT JOIN public.ballot_measures measure
    ON measure.id = result.ballot_measure_id
   AND measure.election_id = result.election_id
  WHERE measure.id IS NULL;

  IF mismatched_count > 0 THEN
    RAISE EXCEPTION
      'Cannot enforce ballot_measure_results measure/election consistency: found % mismatched row(s)',
      mismatched_count;
  END IF;
END $$;

ALTER TABLE public.ballot_measures
  DROP CONSTRAINT IF EXISTS uq_ballot_measures_id_election;

ALTER TABLE public.ballot_measures
  ADD CONSTRAINT uq_ballot_measures_id_election
  UNIQUE (id, election_id);

ALTER TABLE public.ballot_measure_results
  DROP CONSTRAINT IF EXISTS fk_ballot_measure_results_measure,
  DROP CONSTRAINT IF EXISTS fk_ballot_measure_results_election;

ALTER TABLE public.ballot_measure_results
  ADD CONSTRAINT fk_ballot_measure_results_measure_election
  FOREIGN KEY (ballot_measure_id, election_id)
  REFERENCES public.ballot_measures (id, election_id)
  ON DELETE CASCADE;

COMMIT;
