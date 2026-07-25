BEGIN;

-- Per-record provenance: which writer produced the current content of a
-- candidate record, and which run/session it came from. The 2026-07-15
-- poisoning cleanup needed days of forensics to find ~5,400 affected rows;
-- with these columns a poisoned cohort is a WHERE clause
-- (origin_run_id = ...) instead of a reconstruction.
--
-- origin values:
--   ai_enricher  discovery-pass records written by the candidate-record enricher
--   repair       records whose source URL came from the AI source-repair pass
--   manual       manual research writers (district and presidential)
-- NULL means "written before provenance existed" — existing rows are left
-- NULL on purpose: back-stamping them would assert an origin nobody recorded.
--
-- origin_run_id: enricher staging-stream run_id, or the manual writer's
-- manual key (manual:candidate-records:<election>:<candidate> /
-- manual:presidential-records:...). Nullable — old enricher runs can carry
-- an empty run_id.
ALTER TABLE public.candidate_records
    ADD COLUMN origin text,
    ADD COLUMN origin_run_id text;

ALTER TABLE public.candidate_records
    ADD CONSTRAINT candidate_records_origin_check
    CHECK (origin IS NULL OR origin IN ('ai_enricher', 'repair', 'manual'));

COMMIT;
