BEGIN;

ALTER TABLE public.manual_candidate_finance_filing_targets
  DROP CONSTRAINT manual_candidate_finance_filing_targets_candidate_election_fk,
  ADD CONSTRAINT manual_candidate_finance_filing_targets_candidate_election_fk
    FOREIGN KEY (candidate_id, election_id)
    REFERENCES public.candidate_elections(candidate_id, election_id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT;

COMMIT;
