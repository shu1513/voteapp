BEGIN;

-- Sweep confirmations were only writable for completeness claims
-- (no_records_found / only_general_labels): the CHECK constraint required at
-- least one claimed gap id, so an evidenced full discovery sweep that FOUND
-- stance-labeled records could not persist its per-question ledger — it
-- lived only in run artifacts, and nothing in the database distinguished
-- that finished sweep from a skipped one. Allow an empty claim set:
-- confirmed_gap_ids = '{}' means "full discovery sweep ran with evidence;
-- stance-labeled records were found; no completeness claims asserted".
-- The subset check (only known completeness gap ids) stays.
ALTER TABLE public.candidate_record_sweep_confirmations
    DROP CONSTRAINT chk_candidate_record_sweep_confirmations_gap_ids;

ALTER TABLE public.candidate_record_sweep_confirmations
    ADD CONSTRAINT chk_candidate_record_sweep_confirmations_gap_ids
    CHECK (
        confirmed_gap_ids <@ ARRAY[
            'candidate_records.no_records_found',
            'candidate_records.only_general_labels'
        ]::text[]
    );

COMMIT;
