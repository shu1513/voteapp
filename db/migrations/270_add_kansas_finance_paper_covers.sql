BEGIN;

-- Kansas transcribed paper covers (plan-kansas-finance.md, Phase 4 — paper
-- filers). A paper (scanned) report has no viewer cover to open, and OCR of
-- the KPDC scans recovered 2 of 5 covers in Phase 0 — below the plan's gate —
-- so a paper cover's seven lines are transcribed by hand from the scan and
-- the sync reads them from here in place of the cover it cannot open.
--
-- One row per scanned report VERSION: the link's viewer search recipe
-- (ks_candidate_finance_links.committee_id) plus the KPDC filename
-- ("H058AS_202607.pdf"; an amendment is "H058AS_amend202607.pdf"). The
-- filename fixes the period and the version exactly as the paper inventory
-- reads it, so a transcribed cover matches the ledger's canonical version
-- and nothing else. Amounts are dollars as printed on the cover; the
-- arithmetic CHECK is the form's own self-check (1 + 2 = 3, 3 - 4 = 5), so a
-- misread digit fails at insert. Totals only — no Schedule A/B rows and no
-- contributor names (K.S.A. 25-4154(d)).

CREATE TABLE IF NOT EXISTS public.ks_candidate_finance_paper_covers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  committee_id text NOT NULL,
  election_year integer NOT NULL,
  source_file_name text NOT NULL,
  source_url text NOT NULL,
  cash_beginning numeric(16,2) NOT NULL,
  total_contributions numeric(16,2) NOT NULL,
  cash_available numeric(16,2) NOT NULL,
  total_expenditures numeric(16,2) NOT NULL,
  cash_close numeric(16,2) NOT NULL,
  in_kind numeric(16,2) NOT NULL,
  other_transactions numeric(16,2),
  transcribed_by text NOT NULL,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ks_candidate_finance_paper_covers_year_check
    CHECK (election_year BETWEEN 2024 AND 2100),
  CONSTRAINT ks_candidate_finance_paper_covers_committee_id_check
    CHECK (committee_id ~ '^[0-9]+:[0-9]*:[A-Z0-9][A-Z0-9 ]*:[A-Z0-9][A-Z0-9 ]*$'),
  CONSTRAINT ks_candidate_finance_paper_covers_file_name_check
    CHECK (source_file_name ~ '^[A-Za-z0-9]+_[A-Za-z0-9]+\.pdf$'),
  CONSTRAINT ks_candidate_finance_paper_covers_source_url_check
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT ks_candidate_finance_paper_covers_transcribed_by_check
    CHECK (btrim(transcribed_by) <> ''),
  CONSTRAINT ks_candidate_finance_paper_covers_arithmetic_check
    CHECK (
      cash_beginning + total_contributions = cash_available
      AND cash_available - total_expenditures = cash_close
    ),
  CONSTRAINT ks_candidate_finance_paper_covers_unique
    UNIQUE (committee_id, election_year, source_file_name)
);

DROP TRIGGER IF EXISTS ks_candidate_finance_paper_covers_set_updated_at
  ON public.ks_candidate_finance_paper_covers;
CREATE TRIGGER ks_candidate_finance_paper_covers_set_updated_at
BEFORE UPDATE ON public.ks_candidate_finance_paper_covers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
