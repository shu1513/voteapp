-- San Francisco campaign-finance amount tables (plan-san-francisco-finance.md
-- Phase 5), completing the identity tables of migration 215. Modeled on the
-- Los Angeles City tables (migration 173) with SF's extras: debts_owed,
-- loans_received, public_funds_received (SF's public-financing program plays
-- the role LA's matching_funds column plays, named after the shared read-path
-- key), and methodology_version (the Phase 0 decision records the proven
-- formula version with every snapshot). No membership columns — SF does not
-- disclose member communications separately.

BEGIN;

CREATE TABLE public.sfc_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  -- Manifest headline figures (funds / expenses), dollars. The manifest funds
  -- figure INCLUDES public-financing disbursements (Phase 4 gate identity), so
  -- it is kept for reconciliation while direct_contribution_total carries
  -- donor money only — the value the read path prefers for total_raised, so
  -- "Raised" and "Public funds" stay disjoint on the card (NC pattern).
  total_receipts numeric(16,2),
  direct_contribution_total numeric(16,2),
  total_disbursements numeric(16,2),
  -- Latest Form 460: line 16 / line 19; Schedule B1 line 1 summed across
  -- filings. Loans are NEVER part of total_receipts (Phase 4 gate result).
  cash_on_hand numeric(16,2),
  debts_owed numeric(16,2),
  loans_received numeric(16,2),
  public_funds_received numeric(16,2),
  outside_support_total numeric(16,2),
  outside_oppose_total numeric(16,2),
  methodology_version text NOT NULL CHECK (btrim(methodology_version) <> ''),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  reported_through date,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (total_receipts IS NULL OR total_receipts >= 0)
    AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
    AND (total_disbursements IS NULL OR total_disbursements >= 0)
    AND (cash_on_hand IS NULL OR cash_on_hand >= 0)
    AND (debts_owed IS NULL OR debts_owed >= 0)
    AND (loans_received IS NULL OR loans_received >= 0)
    AND (public_funds_received IS NULL OR public_funds_received >= 0)
    AND (outside_support_total IS NULL OR outside_support_total >= 0)
    AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
  ),
  FOREIGN KEY (link_id, election_year)
    REFERENCES public.sfc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year)
);
CREATE INDEX sfc_candidate_finance_summaries_lookup_idx
  ON public.sfc_candidate_finance_summaries (link_id, election_year DESC);
CREATE TRIGGER sfc_candidate_finance_summaries_set_updated_at
  BEFORE UPDATE ON public.sfc_candidate_finance_summaries
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.sfc_candidate_finance_direct_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  category_type text NOT NULL
    CHECK (category_type IN ('occupation', 'employer', 'industry', 'contribution_size')),
  category_name text NOT NULL CHECK (btrim(category_name) <> ''),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0),
  contributor_count integer CHECK (contributor_count IS NULL OR contributor_count >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (link_id, election_year)
    REFERENCES public.sfc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year, category_type, category_name)
);
CREATE INDEX sfc_candidate_finance_direct_breakdowns_lookup_idx
  ON public.sfc_candidate_finance_direct_breakdowns (link_id, election_year DESC, category_type, amount DESC);
CREATE TRIGGER sfc_candidate_finance_direct_breakdowns_set_updated_at
  BEFORE UPDATE ON public.sfc_candidate_finance_direct_breakdowns
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Per-cycle amounts for the relation rows of
-- sfc_candidate_finance_outside_committee_links (migration 215). Keyed the
-- same way: spender_fppc_id carries the synthetic "name:…" identity for
-- id-less manifest spenders; direction is per relation, not per committee. No
-- expenditure_count — the SFEC manifest discloses per-relation totals only.
CREATE TABLE public.sfc_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  spender_fppc_id text NOT NULL CHECK (btrim(spender_fppc_id) <> ''),
  spender_name text NOT NULL CHECK (btrim(spender_name) <> ''),
  support_oppose text NOT NULL CHECK (support_oppose IN ('support', 'oppose')),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (link_id, election_year)
    REFERENCES public.sfc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year, spender_fppc_id, support_oppose)
);
CREATE INDEX sfc_candidate_finance_outside_groups_lookup_idx
  ON public.sfc_candidate_finance_outside_groups (link_id, election_year DESC, support_oppose, amount DESC);
CREATE TRIGGER sfc_candidate_finance_outside_groups_set_updated_at
  BEFORE UPDATE ON public.sfc_candidate_finance_outside_groups
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
