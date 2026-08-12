-- Phoenix city campaign-finance tables (plan-phoenix-finance.md Phase 1).
-- Four tables in the shipped San José shape (migration 233), with Phoenix's
-- differences:
-- * Committee identity is the City of Phoenix COP ID (e.g. CAN-25-4). A
--   candidate committee gets a NEW COP ID each election cycle (verified:
--   Hermes CAN-23-7 then CAN-25-4), so cop_id is already cycle-scoped. COP
--   IDs are assigned at registration — there is no "Pending" placeholder to
--   guard against (the SJ/SD lesson does not apply). Uppercase is enforced
--   so a re-cased id cannot slip past the (candidate, election, cop_id)
--   unique key and create a duplicate link.
-- * The portal's own election cycle (Apr 1 odd year → Mar 31 two years
--   later; registration ElectionCycle string) lives on the link row,
--   separate from VoteApp election_year. Cycle totals are NOT clipped to
--   the November election date — same documented decision as San José.
-- * outside_coverage_note is a column from day one (SD migration 235
--   precedent): Phoenix outside money flows through four channels and v1
--   measures only the portal-PAC channel systematically; unmeasured
--   channels are disclosed here, and nothing-measured writes NULL totals,
--   never zero.
-- * cash_on_hand is a signed balance (the GA/MA lesson of migrations
--   231/232 — an indebted committee legitimately reports negative).
-- * spender_name stays in the outside-groups unique key: IE-entity and EFD
--   dark-money spenders (curated channels) have no portal id, so two
--   id-less spenders must never collide.
-- Constraints whose generated names would exceed Postgres's 63-character
-- identifier limit are named explicitly.

BEGIN;

CREATE TABLE public.phx_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  candidate_name_normalized text NOT NULL CHECK (btrim(candidate_name_normalized) <> ''),
  cop_id text NOT NULL CHECK (btrim(cop_id) <> '' AND cop_id = upper(cop_id)),
  committee_name text NOT NULL CHECK (btrim(committee_name) <> ''),
  -- Registration ElectionCycle string plus the cycle's date bounds; never
  -- parse the cycle (or a district) out of COP-ID digits.
  portal_cycle_name text NOT NULL CHECK (btrim(portal_cycle_name) <> ''),
  portal_cycle_start date NOT NULL,
  portal_cycle_end date NOT NULL,
  link_status text NOT NULL DEFAULT 'active' CHECK (link_status IN ('active', 'needs_review', 'inactive')),
  link_source text NOT NULL DEFAULT 'manual' CHECK (link_source IN ('manual', 'efiling_portal')),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (portal_cycle_start < portal_cycle_end),
  CONSTRAINT phx_candidate_finance_links_candidate_election_cop_key
    UNIQUE (candidate_id, election_id, cop_id),
  UNIQUE (id, election_year)
);
CREATE UNIQUE INDEX phx_candidate_finance_links_active_candidate_election_idx
  ON public.phx_candidate_finance_links (candidate_id, election_id) WHERE link_status = 'active';
CREATE INDEX phx_candidate_finance_links_election_candidate_idx
  ON public.phx_candidate_finance_links (election_id, candidate_id);
CREATE TRIGGER phx_candidate_finance_links_set_updated_at
  BEFORE UPDATE ON public.phx_candidate_finance_links
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.phx_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  -- Report-cover arithmetic, never transaction sums (the Georgia lesson):
  -- total_raised = Schedule A line 1(m) net monetary contributions summed
  -- over canonical reports; total_spent = Schedule B line 16 cash;
  -- cash_on_hand = latest cover (d) closing balance — a signed BALANCE
  -- (see header); every flow column stays nonnegative.
  total_raised numeric(16,2),
  total_spent numeric(16,2),
  cash_on_hand numeric(16,2),
  debts_owed numeric(16,2),
  loans_received numeric(16,2),
  outside_support_total numeric(16,2),
  outside_oppose_total numeric(16,2),
  -- One sentence naming what the direct totals do NOT cover (shared read
  -- contract direct_coverage_note); null when coverage is complete.
  direct_coverage_note text CHECK (direct_coverage_note IS NULL OR btrim(direct_coverage_note) <> ''),
  -- Same disclosure for the outside totals (shared read contract
  -- outside_coverage_note): portal-PAC channel only in v1; standing-PAC /
  -- IE-entity / EFD channels named here when unmeasured or curated.
  outside_coverage_note text CHECK (outside_coverage_note IS NULL OR btrim(outside_coverage_note) <> ''),
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
    REFERENCES public.phx_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year)
);
CREATE TRIGGER phx_candidate_finance_summaries_set_updated_at
  BEFORE UPDATE ON public.phx_candidate_finance_summaries
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.phx_candidate_finance_direct_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  -- Occupation + employer come from PDF Schedule A(1)(a)/A(1)(c) itemized
  -- rows; contribution_size stays in the enum for shape parity but is
  -- unused in v1 (the A(1)(b) aggregate makes exact buckets impossible).
  category_type text NOT NULL
    CHECK (category_type IN ('occupation', 'employer', 'industry', 'contribution_size')),
  category_name text NOT NULL CHECK (btrim(category_name) <> ''),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0),
  contributor_count integer
    CONSTRAINT phx_finance_direct_breakdowns_contributor_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT phx_finance_direct_breakdowns_link_year_fkey
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.phx_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT phx_finance_direct_breakdowns_category_key
    UNIQUE (link_id, election_year, category_type, category_name)
);
CREATE INDEX phx_candidate_finance_direct_breakdowns_lookup_idx
  ON public.phx_candidate_finance_direct_breakdowns (link_id, election_year DESC, category_type, amount DESC);
CREATE TRIGGER phx_candidate_finance_direct_breakdowns_set_updated_at
  BEFORE UPDATE ON public.phx_candidate_finance_direct_breakdowns
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Per-cycle outside-spending amounts, one row per (spender, direction) as
-- grouped by the Phase 3 aggregator. spender_filer_id is a COP ID for the
-- portal-PAC channel; curated channels (IE-entity, EFD) carry whatever
-- identifier the Phase 3 supplements define — spender_name in the unique
-- key keeps id-less spenders distinct either way.
CREATE TABLE public.phx_candidate_finance_outside_groups (
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
  CONSTRAINT phx_finance_outside_groups_link_year_fkey
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.phx_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT phx_finance_outside_groups_spender_key
    UNIQUE (link_id, election_year, spender_filer_id, spender_name, support_oppose)
);
CREATE INDEX phx_candidate_finance_outside_groups_lookup_idx
  ON public.phx_candidate_finance_outside_groups (link_id, election_year DESC, support_oppose, amount DESC);
CREATE TRIGGER phx_candidate_finance_outside_groups_set_updated_at
  BEFORE UPDATE ON public.phx_candidate_finance_outside_groups
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
