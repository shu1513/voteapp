BEGIN;

-- Kansas transcribed IE rows (migration 271): an explicit filing identity.
--
-- KPDC scans a multi-page independent-expenditure filing as ONE PDF PER PAGE,
-- and every page repeats the filing's "Total this Period" (American
-- Conservative Fund's 8/20/2026 filing is five files, each printing
-- $309,228.84). The checksum must sum those pages before comparing them to
-- the total, and it must not guess which files are pages of one filing from
-- the amount alone: two separate filings can print the same total, and then
-- two complete filings would be quarantined while two half-transcribed ones
-- would pass.
--
-- filing_key is set by the transcriber, from what the pages themselves show
-- (the same signature date and filing stamp, a continuation of one table),
-- and is shared by every row of every page of that filing — e.g.
-- "signed 2026-08-20". NULL means the file is a whole filing by itself, which
-- is the usual case. Rows of one file must all carry the same key.
ALTER TABLE public.ks_candidate_finance_outside_rows
  ADD COLUMN IF NOT EXISTS filing_key text;

ALTER TABLE public.ks_candidate_finance_outside_rows
  DROP CONSTRAINT IF EXISTS ks_candidate_finance_outside_rows_filing_key_check;
ALTER TABLE public.ks_candidate_finance_outside_rows
  ADD CONSTRAINT ks_candidate_finance_outside_rows_filing_key_check
  CHECK (filing_key IS NULL OR btrim(filing_key) <> '');

COMMIT;
