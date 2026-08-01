BEGIN;

-- Soft retirement for canonical candidate_records rows whose CLAIM was judged
-- wrong or unsupportable (wrong attribution, unsupported claim, stale-by-design
-- aggregate) — defects the repair scripts cannot fix by swapping a URL or a
-- date. The wave 18-20 backfill audits accumulated 32 such rows with no
-- sanctioned correction path.
--
-- Soft, not DELETE, for two reasons:
--   1. user_candidate_follow_notification_events and candidate_record_area_tags
--      cascade on record deletion — hard-deleting a record erases the audit
--      trail of notifications already sent about it.
--   2. The retired row keeps its (candidate_id, record_identity_key) slot, so
--      a later research sweep that re-derives the same claim folds into the
--      retired row (candidateRecordStore upserts never clear retired_at) and
--      stays hidden instead of silently resurrecting a withdrawn claim.
--
-- Read rule: retired rows leave every serving and work-feed path (API detail,
-- ballot lookup, follow digests/notifications, plain-language backfill,
-- completeness audits, sweep-confirmation checks). They stay visible to
-- provenance-by-run forensics (origin_run_id) and to record-id point lookups
-- for content reports, which may be about the retired row itself.
ALTER TABLE public.candidate_records
    ADD COLUMN retired_at timestamptz,
    ADD COLUMN retired_reason text;

-- A retirement without a reason is unreviewable; a reason without a
-- retirement is a stray write. Both-or-neither.
ALTER TABLE public.candidate_records
    ADD CONSTRAINT candidate_records_retirement_check
    CHECK ((retired_at IS NULL) = (retired_reason IS NULL));

COMMIT;
