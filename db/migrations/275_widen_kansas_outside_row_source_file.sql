BEGIN;

-- Kansas outside rows: allow a viewer-sourced artifact as a row's source
-- (plan-kansas-finance.md, Phase 5).
--
-- The KPDC archive splits a multi-page IE filing into one scanned PDF per
-- page, and a page can simply be missing: American Conservative Fund's
-- 7/27/2026 filing prints a $486,070.00 total but the archive lists only
-- four of its five pages, so the period could never reconcile. The SOS CFR
-- viewer serves the SAME filing complete, as one PDF named by its filing id
-- (sos.ks.gov/srvimages/campaignfinance/filings/cyYYYY/cmMM/<id>.pdf) — the
-- plan's own rule that the viewer, not the archive, is the completeness
-- reference. Rows recovered that way are named "SOS_<id>.pdf" so a row's
-- source is legible at a glance, and they carry the same filing_key as the
-- archive pages of the filing, so the per-filing checksum sees one filing.

ALTER TABLE public.ks_candidate_finance_outside_rows
  DROP CONSTRAINT IF EXISTS ks_candidate_finance_outside_rows_file_name_check;

ALTER TABLE public.ks_candidate_finance_outside_rows
  ADD CONSTRAINT ks_candidate_finance_outside_rows_file_name_check
  CHECK (source_file_name ~ '^(IE_[A-Za-z0-9]+_[A-Za-z0-9]+|SOS_[0-9]+)\.pdf$');

COMMIT;
