BEGIN;

-- Bulk population-ordered backfill seeds (Nov-2026 scope sweep) need their own
-- trigger_source: 'manual_seed' is an operator override that bypasses the
-- freshness and deferral gates at claim time, which is wrong for a 14k-row
-- backfill — those rows must skip when a district turns fresh and must wait
-- out active deferrals. Every claim-time gate compares against 'manual_seed'
-- only, so 'bulk_backfill' rows take the normal gated path with no code change.
ALTER TABLE public.manual_district_research_requests
  DROP CONSTRAINT IF EXISTS chk_manual_district_research_requests_trigger_source;

ALTER TABLE public.manual_district_research_requests
  ADD CONSTRAINT chk_manual_district_research_requests_trigger_source
    CHECK (trigger_source IN ('address_resolve', 'me_address_update', 'manual_seed', 'bulk_backfill'));

COMMIT;
