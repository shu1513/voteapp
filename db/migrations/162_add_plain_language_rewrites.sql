BEGIN;

-- Audit log for the one-off plain-language backfill (plan-content-wording.md
-- Phase 2). One row per (table, row, column) the backfill touched:
--   applied — the rewrite passed mechanical checks and the independent
--             fact-consistency verifier, and the target column was updated;
--             original_text preserves what research originally produced so
--             any later report can be diffed and reverted one row at a time.
--   flagged — the rewrite failed a check; the target column was NOT updated.
--             rewritten_text keeps the rejected rewrite and flag_reason keeps
--             why, for manual review. Flagged rows are never auto-retried:
--             the unique key doubles as the resume marker, so a re-run skips
--             every row that already has an audit row of either status.
CREATE TABLE public.plain_language_rewrites (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    target_table text NOT NULL
        CHECK (target_table IN ('candidates', 'ballot_measures', 'candidate_records')),
    target_id uuid NOT NULL,
    target_column text NOT NULL
        CHECK (target_column IN ('summary', 'what_yes_means', 'what_no_means', 'description')),
    status text NOT NULL CHECK (status IN ('applied', 'flagged')),
    original_text text NOT NULL,
    rewritten_text text NOT NULL,
    flag_reason text,
    provider text NOT NULL,
    model text NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT chk_plain_language_rewrites_flag_reason
        CHECK ((status = 'flagged') = (flag_reason IS NOT NULL)),
    CONSTRAINT uq_plain_language_rewrites_target
        UNIQUE (target_table, target_id, target_column)
);

COMMENT ON TABLE public.plain_language_rewrites IS
    'Audit + resume log for the Phase 2 plain-language backfill. applied = column updated (original_text is the pre-rewrite value); flagged = rewrite rejected, column untouched.';

COMMIT;
