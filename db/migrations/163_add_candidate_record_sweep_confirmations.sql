BEGIN;

-- Persisted sweep-completeness confirmations for manual candidate-record
-- passes. The evidence-file guard (manual:candidate-records:write /
-- manual:presidential-records:write) validates the per-question evidence
-- ledger at write time, but until now discarded it: nothing in the database
-- distinguished an evidence-backed confirmed-null candidate from a skipped
-- sweep, so manual:records:audit flagged legitimately confirmed candidates
-- as suspects forever. This table stores the confirmation the writer already
-- verified, one row per candidate (a newer sweep supersedes the older
-- confirmation), so the audit can separate confirmed nulls from real
-- suspects.
CREATE TABLE public.candidate_record_sweep_confirmations (
    candidate_id uuid PRIMARY KEY,
    -- Which completeness claims the evidence backs; subset of the
    -- sweep-completeness gap ids the writers enforce.
    confirmed_gap_ids text[] NOT NULL,
    -- The validated per-question ledger, verbatim: {"entries": [{"question",
    -- "finding"}, ...]}. Stored so a later session can review what was
    -- actually searched without hunting for the run's scratchpad files.
    evidence jsonb NOT NULL,
    -- The research context the sweep ran under. Records are candidate-wide,
    -- so this is provenance, not identity.
    context_type text NOT NULL,
    context_id uuid,
    confirmed_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_candidate_record_sweep_confirmations_candidate
        FOREIGN KEY (candidate_id)
        REFERENCES public.candidates(id)
        ON DELETE CASCADE,
    CONSTRAINT chk_candidate_record_sweep_confirmations_gap_ids
        CHECK (
            confirmed_gap_ids <@ ARRAY[
                'candidate_records.no_records_found',
                'candidate_records.only_general_labels'
            ]::text[]
            AND coalesce(array_length(confirmed_gap_ids, 1), 0) >= 1
        ),
    CONSTRAINT chk_candidate_record_sweep_confirmations_context_type
        CHECK (context_type IN ('election', 'presidential_cycle'))
);

COMMIT;
