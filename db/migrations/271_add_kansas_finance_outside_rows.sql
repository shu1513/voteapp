BEGIN;

-- Kansas transcribed independent-expenditure rows (plan-kansas-finance.md,
-- Phase 5 — outside spending, path 1: dedicated IE statements, K.S.A.
-- 25-4150). The statements are scanned PDFs on the KPDC "Others" tree, so
-- each printed row is transcribed by hand and the candidate sync reads the
-- rows from here. One row per PRINTED row of a statement.
--
--   * filer_name is the spender as the IE index lists it ("Kansas Comeback
--     PAC"); it is the outside group's identity.
--   * period_due_key is the "period covered" box the filer CHECKED, by its
--     due month (202601 = 1/1-12/31/25, 202607 = 1/1-7/23/26, 202610 =
--     7/24-10/22/26, 202701 = 10/23-12/31/26) — not the filename token,
--     which is the folder the scan was filed under (IE_KC4_2607 covers the
--     202610 period).
--   * statement_total is the statement's printed "Total this Period", a
--     CUMULATIVE control total within one period (repeated on every row of
--     the statement). The sync sorts a filer's statements of one period by
--     it and requires the running row sum to match each one, so a misread
--     or missing row fails that filer's period closed.
--   * target_committee_id is the supported/opposed candidate's link recipe
--     ("7:72:STAVOLA:MICHAEL"; statewide "1::MASTERSON:TY"), resolved by the
--     transcriber under the plan's rules (exact district, direction only
--     from the printed Supported/Opposed). NULL, with support_oppose NULL,
--     when the printed row names more than one candidate against a single
--     amount (unallocated) or the candidate cannot be resolved: such a row
--     still counts toward the statement total but never toward a
--     candidate. named_committee_ids then lists the recipes of every
--     candidate the row names, so each of them is "partial" (named by
--     spending that cannot be allocated) rather than "none found" and
--     publishes no outside figure at all — never an explicit-rows-only
--     total, never an arbitrary split. target_as_filed keeps the printed
--     text either way.
-- Vendors are payees, not contributors; no contributor names are stored
-- (K.S.A. 25-4154(d)).

CREATE TABLE IF NOT EXISTS public.ks_candidate_finance_outside_rows (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  election_year integer NOT NULL,
  filer_name text NOT NULL,
  source_file_name text NOT NULL,
  source_url text NOT NULL,
  period_due_key text NOT NULL,
  statement_total numeric(16,2) NOT NULL,
  row_index integer NOT NULL,
  row_date date,
  vendor_name text,
  target_committee_id text,
  named_committee_ids text[],
  target_as_filed text NOT NULL,
  support_oppose text,
  amount numeric(16,2) NOT NULL,
  transcribed_by text NOT NULL,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ks_candidate_finance_outside_rows_year_check
    CHECK (election_year BETWEEN 2024 AND 2100),
  CONSTRAINT ks_candidate_finance_outside_rows_filer_name_check
    CHECK (btrim(filer_name) <> ''),
  CONSTRAINT ks_candidate_finance_outside_rows_file_name_check
    CHECK (source_file_name ~ '^IE_[A-Za-z0-9]+_[A-Za-z0-9]+\.pdf$'),
  CONSTRAINT ks_candidate_finance_outside_rows_source_url_check
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT ks_candidate_finance_outside_rows_period_check
    CHECK (period_due_key ~ '^20[0-9]{2}(01|07|10)$'),
  CONSTRAINT ks_candidate_finance_outside_rows_statement_total_check
    CHECK (statement_total >= 0),
  CONSTRAINT ks_candidate_finance_outside_rows_row_index_check
    CHECK (row_index >= 1),
  CONSTRAINT ks_candidate_finance_outside_rows_target_check
    CHECK (
      target_committee_id IS NULL
      OR target_committee_id ~ '^[0-9]+:[0-9]*:[A-Z0-9][A-Z0-9 ]*:[A-Z0-9][A-Z0-9 ]*$'
    ),
  CONSTRAINT ks_candidate_finance_outside_rows_named_check
    CHECK (
      named_committee_ids IS NULL
      OR (target_committee_id IS NULL AND cardinality(named_committee_ids) >= 1)
    ),
  CONSTRAINT ks_candidate_finance_outside_rows_target_as_filed_check
    CHECK (btrim(target_as_filed) <> ''),
  -- Written so it can never evaluate to NULL (a NULL CHECK passes): the
  -- two nullities must agree, and a present direction must be in vocabulary.
  CONSTRAINT ks_candidate_finance_outside_rows_direction_check
    CHECK (
      (target_committee_id IS NULL) = (support_oppose IS NULL)
      AND (support_oppose IS NULL OR support_oppose IN ('support', 'oppose'))
    ),
  CONSTRAINT ks_candidate_finance_outside_rows_amount_check
    CHECK (amount >= 0),
  CONSTRAINT ks_candidate_finance_outside_rows_transcribed_by_check
    CHECK (btrim(transcribed_by) <> ''),
  CONSTRAINT ks_candidate_finance_outside_rows_unique
    UNIQUE (source_file_name, row_index)
);

CREATE INDEX IF NOT EXISTS ks_candidate_finance_outside_rows_target_idx
  ON public.ks_candidate_finance_outside_rows (election_year, target_committee_id);

DROP TRIGGER IF EXISTS ks_candidate_finance_outside_rows_set_updated_at
  ON public.ks_candidate_finance_outside_rows;
CREATE TRIGGER ks_candidate_finance_outside_rows_set_updated_at
BEFORE UPDATE ON public.ks_candidate_finance_outside_rows
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
