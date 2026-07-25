BEGIN;

-- Per-record provenance: which writer INTRODUCED the record's current
-- (normalized) content, and which run/session it came from. The 2026-07-15
-- poisoning cleanup needed days of forensics to find ~5,400 affected rows;
-- with these columns a poisoned cohort is a WHERE clause
-- (origin_run_id = ...) instead of a reconstruction. Re-imports of identical
-- content keep the original attribution (enforced in candidateRecordStore.ts)
-- so later reruns cannot rotate a poisoned cohort out of that query.
--
-- origin values:
--   ai_enricher  discovery-pass records written by the candidate-record enricher
--   repair       records whose source URL came from the AI source-repair pass
--   manual       manual research writers (district and presidential)
-- NULL means "written before provenance existed" — existing rows are left
-- NULL on purpose: back-stamping them would assert an origin nobody recorded.
--
-- origin_run_id: enricher staging-stream run_id, or the manual writer's
-- manual key plus a per-import timestamp
-- (manual:candidate-records:<election>:<candidate>:<iso-ts>). Nullable —
-- old enricher runs can carry an empty run_id.
ALTER TABLE public.candidate_records
    ADD COLUMN origin text,
    ADD COLUMN origin_run_id text;

ALTER TABLE public.candidate_records
    ADD CONSTRAINT candidate_records_origin_check
    CHECK (origin IS NULL OR origin IN ('ai_enricher', 'repair', 'manual'));

-- The forensics query this table exists to serve is a point lookup on
-- origin_run_id; partial because legacy rows stay NULL forever.
CREATE INDEX idx_candidate_records_origin_run_id
    ON public.candidate_records (origin_run_id)
    WHERE origin_run_id IS NOT NULL;

COMMIT;
