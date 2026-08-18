BEGIN;

-- Candidate records are candidate-wide, but sweep evidence is gathered and
-- validated for one research context at a time. A candidate may have more
-- than one qualifying election (or a presidential-cycle context), so a
-- candidate-only primary key caused the newest sweep to overwrite evidence
-- for every other context. Preserve each validated ledger independently.
ALTER TABLE public.candidate_record_sweep_confirmations
    DROP CONSTRAINT candidate_record_sweep_confirmations_pkey;

ALTER TABLE public.candidate_record_sweep_confirmations
    ADD CONSTRAINT candidate_record_sweep_confirmations_pkey
    PRIMARY KEY (candidate_id, context_type, context_id);

COMMENT ON TABLE public.candidate_record_sweep_confirmations IS
    'Validated candidate-record sweep evidence, one row per candidate and research context.';

COMMIT;
