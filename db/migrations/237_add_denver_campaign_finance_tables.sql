-- Denver city campaign-finance tables (plan-denver-finance.md Phase 1).
-- Standard five-table shape (migration 172 Houston is the closest template):
-- Phase 3 reuses standardStateFinanceBallotLookupLoader and
-- standardStateFinanceDueListQuery as-is, so the summaries carry the standard
-- total_receipts / direct_contribution_total / total_disbursements split
-- (receipts include Fair Elections Fund public matching; direct is private
-- donor money only — both live-verified against SearchLight in Phase 0) and
-- the links carry office_name/district (the shared due-list query selects
-- them unconditionally). The FEF disclosure is a source-level loader config
-- note, not a per-candidate column.
--
-- Denver-specific shape:
-- * Link identity is filer_id — SearchLight's stable filer number as digits
--   text (the shared read contract types committee ids as text). Committee
--   entity ids are mutable auxiliaries the Filer endpoint reports; they are
--   stored as a non-empty integer[] because the Phase 3 transaction-feed
--   row filter (every row's entity id must be in the filer's set) cannot run
--   without them.
-- * cash_on_hand is a signed balance (the GA/MA lesson of migrations
--   231/232; Johnston's 2023 year-end report closes at -$738.05).
-- * Outside spenders are keyed by SearchLight's search uniqueId ("Ind787");
--   the aggregated spender lists carry no id, so the resolver derives it and
--   the CHECK makes a raw name or a non-IE id unrepresentable here.
-- * outside_group_breakdowns stays empty in v1 (per-donor industry
--   classification is out of scope) but exists because the shared loader
--   reads all five relations.
-- Constraints and triggers on the 49-character breakdowns table are named
-- with the denver_cf_ prefix so no generated identifier exceeds Postgres's
-- 63-character limit.

BEGIN;

CREATE TABLE public.denver_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  candidate_name_normalized text NOT NULL CHECK (btrim(candidate_name_normalized) <> ''),
  office_name text NOT NULL CHECK (btrim(office_name) <> ''),
  district text CHECK (district IS NULL OR btrim(district) <> ''),
  -- SearchLight filerId: the canonical, stable identity (entity/committee
  -- ids are mutable auxiliaries). Digits-only text — a name, a uniqueId, or
  -- an empty string here is a wiring bug and must fail loudly.
  filer_id text NOT NULL CHECK (filer_id ~ '^[1-9][0-9]*$'),
  -- Committee entity ids from api/Filer/filer/{id}, refreshed on every
  -- automatic write. Non-empty: the transaction-feed row filter depends on
  -- this set, so a link without it would silently pass unfiltered rows.
  committee_entity_ids integer[] NOT NULL CHECK (cardinality(committee_entity_ids) > 0),
  committee_name text NOT NULL CHECK (btrim(committee_name) <> ''),
  link_status text NOT NULL DEFAULT 'active' CHECK (link_status IN ('active', 'needs_review', 'inactive')),
  link_source text NOT NULL DEFAULT 'manual' CHECK (link_source IN ('manual', 'searchlight')),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT denver_candidate_finance_links_candidate_election_filer_key
    UNIQUE (candidate_id, election_id, filer_id),
  UNIQUE (id, election_year)
);
CREATE UNIQUE INDEX denver_candidate_finance_links_active_candidate_election_idx
  ON public.denver_candidate_finance_links (candidate_id, election_id) WHERE link_status = 'active';
CREATE INDEX denver_candidate_finance_links_election_candidate_idx
  ON public.denver_candidate_finance_links (election_id, candidate_id);
CREATE TRIGGER denver_candidate_finance_links_set_updated_at
  BEFORE UPDATE ON public.denver_candidate_finance_links
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.denver_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  -- getContributionsTotalByCommittee: private donor money + FEF public
  -- matching (fixture 1 pins the composition every sync).
  total_receipts numeric(16,2),
  -- Overview campaignContributionsToCandidate: private donor money only.
  direct_contribution_total numeric(16,2),
  -- getExpendituresTotalByCommittee: already includes FEF-funded spending —
  -- the FEF endpoints are subsets, never added.
  total_disbursements numeric(16,2),
  -- closingBalance of the latest in-force period report per cycle: a signed
  -- BALANCE that can be legitimately negative; every flow stays nonnegative.
  cash_on_hand numeric(16,2),
  outside_support_total numeric(16,2),
  outside_oppose_total numeric(16,2),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (total_receipts IS NULL OR total_receipts >= 0)
    AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
    AND (total_disbursements IS NULL OR total_disbursements >= 0)
    AND (outside_support_total IS NULL OR outside_support_total >= 0)
    AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
  ),
  FOREIGN KEY (link_id, election_year)
    REFERENCES public.denver_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year)
);
CREATE TRIGGER denver_candidate_finance_summaries_set_updated_at
  BEFORE UPDATE ON public.denver_candidate_finance_summaries
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.denver_candidate_finance_direct_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  category_type text NOT NULL CHECK (category_type IN ('occupation', 'contribution_size')),
  category_name text NOT NULL CHECK (btrim(category_name) <> ''),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0),
  contributor_count integer
    CONSTRAINT denver_cf_direct_breakdowns_contributor_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT denver_cf_direct_breakdowns_link_year_fkey
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.denver_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT denver_cf_direct_breakdowns_category_key
    UNIQUE (link_id, election_year, category_type, category_name)
);
CREATE INDEX denver_candidate_finance_direct_breakdowns_lookup_idx
  ON public.denver_candidate_finance_direct_breakdowns (link_id, election_year DESC, category_type, amount DESC);
CREATE TRIGGER denver_candidate_finance_direct_breakdowns_set_updated_at
  BEFORE UPDATE ON public.denver_candidate_finance_direct_breakdowns
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Per-cycle outside-spending amounts, one row per (spender, direction) from
-- the server-aggregated support/oppose lists. spender_id is the resolved
-- search uniqueId — the resolver fails the candidate closed on zero or
-- multiple matches, so an id is always present and unique per spender.
CREATE TABLE public.denver_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  spender_id text NOT NULL CHECK (spender_id ~ '^Ind[0-9]+$'),
  spender_name text NOT NULL CHECK (btrim(spender_name) <> ''),
  support_oppose text NOT NULL CHECK (support_oppose IN ('support', 'oppose')),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT denver_cf_outside_groups_link_year_fkey
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.denver_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT denver_cf_outside_groups_spender_key
    UNIQUE (link_id, election_year, spender_id, support_oppose)
);
CREATE INDEX denver_candidate_finance_outside_groups_lookup_idx
  ON public.denver_candidate_finance_outside_groups (link_id, election_year DESC, support_oppose, amount DESC);
CREATE TRIGGER denver_candidate_finance_outside_groups_set_updated_at
  BEFORE UPDATE ON public.denver_candidate_finance_outside_groups
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Empty in v1 (see header). The group FK cascades, so a snapshot that
-- deletes-and-reinserts outside groups can never strand breakdown rows.
CREATE TABLE public.denver_candidate_finance_outside_group_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL
    CONSTRAINT denver_cf_outside_group_breakdowns_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  spender_id text NOT NULL
    CONSTRAINT denver_cf_outside_group_breakdowns_spender_id_check
    CHECK (spender_id ~ '^Ind[0-9]+$'),
  support_oppose text NOT NULL
    CONSTRAINT denver_cf_outside_group_breakdowns_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  category_type text NOT NULL
    CONSTRAINT denver_cf_outside_group_breakdowns_type_check
    CHECK (category_type IN ('donor', 'industry')),
  category_name text NOT NULL
    CONSTRAINT denver_cf_outside_group_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  amount numeric(16,2) NOT NULL
    CONSTRAINT denver_cf_outside_group_breakdowns_amount_check
    CHECK (amount >= 0),
  contributor_count integer
    CONSTRAINT denver_cf_outside_group_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  source_url text
    CONSTRAINT denver_cf_outside_group_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT denver_cf_outside_group_breakdowns_unique
    UNIQUE (link_id, election_year, spender_id, support_oppose, category_type, category_name),
  CONSTRAINT denver_cf_outside_group_breakdowns_group_fkey
    FOREIGN KEY (link_id, election_year, spender_id, support_oppose)
    REFERENCES public.denver_candidate_finance_outside_groups (link_id, election_year, spender_id, support_oppose)
    ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX denver_cf_outside_group_breakdowns_lookup_idx
  ON public.denver_candidate_finance_outside_group_breakdowns (link_id, election_year DESC, support_oppose, category_type, amount DESC);
CREATE TRIGGER denver_cf_outside_group_breakdowns_set_updated_at
  BEFORE UPDATE ON public.denver_candidate_finance_outside_group_breakdowns
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
