BEGIN;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS public.candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  fec_candidate_id text NOT NULL,
  election_year integer NOT NULL,
  total_receipts numeric(16,2),
  total_disbursements numeric(16,2),
  cash_on_hand numeric(16,2),
  debts_owed numeric(16,2),
  individual_itemized_total numeric(16,2),
  individual_unitemized_total numeric(16,2),
  other_committee_contributions numeric(16,2),
  transfers_from_affiliated_committees numeric(16,2),
  outside_support_total numeric(16,2),
  outside_oppose_total numeric(16,2),
  source_url text,
  last_synced_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_candidate_finance_summaries_fec_year
    UNIQUE (fec_candidate_id, election_year),
  CONSTRAINT chk_candidate_finance_summaries_fec_candidate_id
    CHECK (btrim(fec_candidate_id) <> ''),
  CONSTRAINT chk_candidate_finance_summaries_election_year
    CHECK (election_year BETWEEN 1970 AND 2100),
  CONSTRAINT chk_candidate_finance_summaries_nonnegative_amounts
    CHECK (
      (total_receipts IS NULL OR total_receipts >= 0)
      AND (total_disbursements IS NULL OR total_disbursements >= 0)
      AND (cash_on_hand IS NULL OR cash_on_hand >= 0)
      AND (debts_owed IS NULL OR debts_owed >= 0)
      AND (individual_itemized_total IS NULL OR individual_itemized_total >= 0)
      AND (individual_unitemized_total IS NULL OR individual_unitemized_total >= 0)
      AND (other_committee_contributions IS NULL OR other_committee_contributions >= 0)
      AND (transfers_from_affiliated_committees IS NULL OR transfers_from_affiliated_committees >= 0)
      AND (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    ),
  CONSTRAINT chk_candidate_finance_summaries_source_url
    CHECK (source_url IS NULL OR btrim(source_url) <> '')
);

CREATE INDEX IF NOT EXISTS idx_candidate_finance_summaries_fec_year
  ON public.candidate_finance_summaries (fec_candidate_id, election_year DESC);

DROP TRIGGER IF EXISTS trg_candidate_finance_summaries_set_updated_at
  ON public.candidate_finance_summaries;
CREATE TRIGGER trg_candidate_finance_summaries_set_updated_at
BEFORE UPDATE ON public.candidate_finance_summaries
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.candidate_finance_direct_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  fec_candidate_id text NOT NULL,
  election_year integer NOT NULL,
  category_type text NOT NULL,
  category_name text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_candidate_finance_direct_breakdowns_key
    UNIQUE (fec_candidate_id, election_year, category_type, category_name),
  CONSTRAINT chk_candidate_finance_direct_breakdowns_fec_candidate_id
    CHECK (btrim(fec_candidate_id) <> ''),
  CONSTRAINT chk_candidate_finance_direct_breakdowns_election_year
    CHECK (election_year BETWEEN 1970 AND 2100),
  CONSTRAINT chk_candidate_finance_direct_breakdowns_category_type
    CHECK (category_type IN ('occupation', 'employer', 'industry', 'contribution_size')),
  CONSTRAINT chk_candidate_finance_direct_breakdowns_category_name
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT chk_candidate_finance_direct_breakdowns_amount
    CHECK (amount >= 0),
  CONSTRAINT chk_candidate_finance_direct_breakdowns_contributor_count
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT chk_candidate_finance_direct_breakdowns_source_url
    CHECK (source_url IS NULL OR btrim(source_url) <> '')
);

CREATE INDEX IF NOT EXISTS idx_candidate_finance_direct_breakdowns_lookup
  ON public.candidate_finance_direct_breakdowns (fec_candidate_id, election_year DESC, category_type, amount DESC);

DROP TRIGGER IF EXISTS trg_candidate_finance_direct_breakdowns_set_updated_at
  ON public.candidate_finance_direct_breakdowns;
CREATE TRIGGER trg_candidate_finance_direct_breakdowns_set_updated_at
BEFORE UPDATE ON public.candidate_finance_direct_breakdowns
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  fec_candidate_id text NOT NULL,
  election_year integer NOT NULL,
  committee_id text NOT NULL,
  committee_name text NOT NULL,
  support_oppose text NOT NULL,
  amount numeric(16,2) NOT NULL,
  source_url text,
  last_synced_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_candidate_finance_outside_groups_key
    UNIQUE (fec_candidate_id, election_year, committee_id, support_oppose),
  CONSTRAINT chk_candidate_finance_outside_groups_fec_candidate_id
    CHECK (btrim(fec_candidate_id) <> ''),
  CONSTRAINT chk_candidate_finance_outside_groups_election_year
    CHECK (election_year BETWEEN 1970 AND 2100),
  CONSTRAINT chk_candidate_finance_outside_groups_committee_id
    CHECK (btrim(committee_id) <> ''),
  CONSTRAINT chk_candidate_finance_outside_groups_committee_name
    CHECK (btrim(committee_name) <> ''),
  CONSTRAINT chk_candidate_finance_outside_groups_support_oppose
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT chk_candidate_finance_outside_groups_amount
    CHECK (amount >= 0),
  CONSTRAINT chk_candidate_finance_outside_groups_source_url
    CHECK (source_url IS NULL OR btrim(source_url) <> '')
);

CREATE INDEX IF NOT EXISTS idx_candidate_finance_outside_groups_lookup
  ON public.candidate_finance_outside_groups (fec_candidate_id, election_year DESC, support_oppose, amount DESC);

DROP TRIGGER IF EXISTS trg_candidate_finance_outside_groups_set_updated_at
  ON public.candidate_finance_outside_groups;
CREATE TRIGGER trg_candidate_finance_outside_groups_set_updated_at
BEFORE UPDATE ON public.candidate_finance_outside_groups
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.candidate_finance_outside_group_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  fec_candidate_id text NOT NULL,
  election_year integer NOT NULL,
  committee_id text NOT NULL,
  support_oppose text NOT NULL,
  category_type text NOT NULL,
  category_name text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_candidate_finance_outside_group_breakdowns_key
    UNIQUE (fec_candidate_id, election_year, committee_id, support_oppose, category_type, category_name),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_fec_candidate_id
    CHECK (btrim(fec_candidate_id) <> ''),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_election_year
    CHECK (election_year BETWEEN 1970 AND 2100),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_committee_id
    CHECK (btrim(committee_id) <> ''),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_support_oppose
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_category_type
    CHECK (category_type IN ('donor', 'occupation', 'employer', 'industry')),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_category_name
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_amount
    CHECK (amount >= 0),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_contributor_count
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT chk_candidate_finance_outside_group_breakdowns_source_url
    CHECK (source_url IS NULL OR btrim(source_url) <> '')
);

CREATE INDEX IF NOT EXISTS idx_candidate_finance_outside_group_breakdowns_lookup
  ON public.candidate_finance_outside_group_breakdowns (
    fec_candidate_id,
    election_year DESC,
    support_oppose,
    category_type,
    amount DESC
  );

DROP TRIGGER IF EXISTS trg_candidate_finance_outside_group_breakdowns_set_updated_at
  ON public.candidate_finance_outside_group_breakdowns;
CREATE TRIGGER trg_candidate_finance_outside_group_breakdowns_set_updated_at
BEFORE UPDATE ON public.candidate_finance_outside_group_breakdowns
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.finance_label_classifications (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_label text NOT NULL,
  label_type text NOT NULL,
  normalized_label text NOT NULL,
  industry_slug text,
  confidence text NOT NULL DEFAULT 'unknown',
  classification_source text NOT NULL DEFAULT 'unknown',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_finance_label_classifications_label
    UNIQUE (label_type, normalized_label),
  CONSTRAINT chk_finance_label_classifications_raw_label
    CHECK (btrim(raw_label) <> ''),
  CONSTRAINT chk_finance_label_classifications_label_type
    CHECK (label_type IN ('employer', 'occupation', 'donor', 'committee')),
  CONSTRAINT chk_finance_label_classifications_normalized_label
    CHECK (btrim(normalized_label) <> ''),
  CONSTRAINT chk_finance_label_classifications_industry_slug
    CHECK (industry_slug IS NULL OR industry_slug ~ '^[a-z0-9_]+$'),
  CONSTRAINT chk_finance_label_classifications_confidence
    CHECK (confidence IN ('high', 'medium', 'low', 'unknown')),
  CONSTRAINT chk_finance_label_classifications_source
    CHECK (classification_source IN ('rule', 'manual', 'ai', 'unknown')),
  CONSTRAINT chk_finance_label_classifications_unknown_industry
    CHECK (
      (confidence = 'unknown' AND industry_slug IS NULL)
      OR confidence <> 'unknown'
    )
);

CREATE INDEX IF NOT EXISTS idx_finance_label_classifications_industry
  ON public.finance_label_classifications (industry_slug)
  WHERE industry_slug IS NOT NULL;

DROP TRIGGER IF EXISTS trg_finance_label_classifications_set_updated_at
  ON public.finance_label_classifications;
CREATE TRIGGER trg_finance_label_classifications_set_updated_at
BEFORE UPDATE ON public.finance_label_classifications
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
