BEGIN;

-- The roll-call importer (docs/plans/roll-call-vote-import.md) is a fourth
-- candidate_records writer, so it gets its own provenance value:
--
--   rollcall_import  records fanned out from an approved legislative_votes
--                    row; origin_run_id =
--                    rollcall:<jurisdiction>:<chamber>:<session>:<roll>:<iso-ts>
--
-- Promotion copies origin verbatim, so this migration must be applied on
-- production before any rollcall_import row is promoted.
--
-- The importer also re-keys duplicate hand-written roll-call records in
-- place with candidate_record_identity_transitions.reason =
-- 'rollcall_normalization'. That column has no CHECK, so no DDL is needed
-- for it; the value is listed here so the ledger's vocabulary stays in one
-- place with migration 209's comment.
ALTER TABLE public.candidate_records
    DROP CONSTRAINT candidate_records_origin_check;

ALTER TABLE public.candidate_records
    ADD CONSTRAINT candidate_records_origin_check
    CHECK (origin IS NULL OR origin IN ('ai_enricher', 'repair', 'manual', 'rollcall_import'));

COMMIT;
