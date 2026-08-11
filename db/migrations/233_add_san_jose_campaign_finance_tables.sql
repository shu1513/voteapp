-- San José campaign-finance tables (plan-san-jose-finance.md Phase 4).
-- Modeled on the San Francisco tables (migrations 215 + 229), with SJ's
-- differences: committee identity is the FPPC id alone (the efile.systems
-- export has no contest code or filer nid); one donor-money raised figure
-- (F460 line 1 + line 4 — no manifest headline vs direct split, no public
-- financing program); cash_on_hand is signed from day one (the GA/MA lesson
-- of migrations 231/232 — an indebted committee legitimately reports a
-- negative balance); direct_coverage_note persists the per-candidate
-- coverage disclosure the sync derives from aggregator violations (e.g. a
-- committee whose pre-2025 activity is absent from the export); outside
-- groups carry expenditure_count (the transaction-level source discloses it,
-- unlike SF's manifest) and include spender_name in the unique key so two
-- id-less "Pending" spenders never collide.

BEGIN;

CREATE TABLE public.sjc_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  candidate_name_normalized text NOT NULL CHECK (btrim(candidate_name_normalized) <> ''),
  -- Filer_ID is free text upstream and may be the literal "Pending" (an
  -- FPPC id not yet assigned). The resolver never links Pending committees;
  -- the constraint makes that unrepresentable here too — case-insensitively,
  -- so an upstream re-casing ("PENDING") surfaces as a loud write failure
  -- instead of a placeholder stored as a durable committee identity.
  fppc_id text NOT NULL CHECK (btrim(fppc_id) <> '' AND lower(btrim(fppc_id)) <> 'pending'),
  committee_name text NOT NULL CHECK (btrim(committee_name) <> ''),
  link_status text NOT NULL DEFAULT 'active' CHECK (link_status IN ('active', 'needs_review', 'inactive')),
  link_source text NOT NULL DEFAULT 'manual' CHECK (link_source IN ('manual', 'efile_export')),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (candidate_id, election_id, fppc_id),
  UNIQUE (id, election_year)
);
CREATE UNIQUE INDEX sjc_candidate_finance_links_active_candidate_election_idx
  ON public.sjc_candidate_finance_links (candidate_id, election_id) WHERE link_status = 'active';
CREATE INDEX sjc_candidate_finance_links_election_candidate_idx
  ON public.sjc_candidate_finance_links (election_id, candidate_id);
CREATE TRIGGER sjc_candidate_finance_links_set_updated_at
  BEFORE UPDATE ON public.sjc_candidate_finance_links
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.sjc_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  -- Σ(F460 line 1 + line 4 Amount_A) across canonical filings: donor money
  -- only. Line 5 is never used — it includes loans, which stay separate per
  -- the shared read contract.
  total_raised numeric(16,2),
  total_spent numeric(16,2),
  -- Latest canonical Form 460: line 16 / line 19; Schedule B1 summary line 1
  -- summed across filings. cash_on_hand is a signed BALANCE (see header);
  -- every flow column stays nonnegative.
  cash_on_hand numeric(16,2),
  debts_owed numeric(16,2),
  loans_received numeric(16,2),
  outside_support_total numeric(16,2),
  outside_oppose_total numeric(16,2),
  -- One sentence naming what the direct totals do NOT cover (shared read
  -- contract direct_coverage_note); null when coverage is complete.
  direct_coverage_note text CHECK (direct_coverage_note IS NULL OR btrim(direct_coverage_note) <> ''),
  methodology_version text NOT NULL CHECK (btrim(methodology_version) <> ''),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  reported_through date,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (total_raised IS NULL OR total_raised >= 0)
    AND (total_spent IS NULL OR total_spent >= 0)
    AND (debts_owed IS NULL OR debts_owed >= 0)
    AND (loans_received IS NULL OR loans_received >= 0)
    AND (outside_support_total IS NULL OR outside_support_total >= 0)
    AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
  ),
  FOREIGN KEY (link_id, election_year)
    REFERENCES public.sjc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year)
);
CREATE INDEX sjc_candidate_finance_summaries_lookup_idx
  ON public.sjc_candidate_finance_summaries (link_id, election_year DESC);
CREATE TRIGGER sjc_candidate_finance_summaries_set_updated_at
  BEFORE UPDATE ON public.sjc_candidate_finance_summaries
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.sjc_candidate_finance_direct_breakdowns (
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
    REFERENCES public.sjc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year, category_type, category_name)
);
CREATE INDEX sjc_candidate_finance_direct_breakdowns_lookup_idx
  ON public.sjc_candidate_finance_direct_breakdowns (link_id, election_year DESC, category_type, amount DESC);
CREATE TRIGGER sjc_candidate_finance_direct_breakdowns_set_updated_at
  BEFORE UPDATE ON public.sjc_candidate_finance_direct_breakdowns
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Per-cycle outside-spending amounts, one row per (spender, direction) as
-- grouped by the Phase 3 aggregator. spender_filer_id may be the literal
-- "Pending"; spender_name is part of the unique key so two distinct id-less
-- spenders coexist (the aggregator already keys Pending spenders by name).
CREATE TABLE public.sjc_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  spender_filer_id text NOT NULL CHECK (btrim(spender_filer_id) <> ''),
  spender_name text NOT NULL CHECK (btrim(spender_name) <> ''),
  support_oppose text NOT NULL CHECK (support_oppose IN ('support', 'oppose')),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0),
  expenditure_count integer CHECK (expenditure_count IS NULL OR expenditure_count >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (link_id, election_year)
    REFERENCES public.sjc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year, spender_filer_id, spender_name, support_oppose)
);
CREATE INDEX sjc_candidate_finance_outside_groups_lookup_idx
  ON public.sjc_candidate_finance_outside_groups (link_id, election_year DESC, support_oppose, amount DESC);
CREATE TRIGGER sjc_candidate_finance_outside_groups_set_updated_at
  BEFORE UPDATE ON public.sjc_candidate_finance_outside_groups
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
