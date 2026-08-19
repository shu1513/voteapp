-- Austin (TX) city campaign-finance tables (plan-austin-finance.md Phase 1).
-- Standard five-table shape — migration 237 (Denver) is the direct template,
-- itself the Houston-172 shape — so Phase 3 reads through
-- standardStateFinanceBallotLookupLoader / standardStateFinanceDueListQuery
-- as-is (the summaries carry the standard total_receipts /
-- direct_contribution_total / total_disbursements split and the links carry
-- office_name/district).
--
-- Austin-specific shape:
-- * The City Clerk's Socrata datasets carry NO filer id: Report Detail
--   (b2pc-2s8n) keys candidate reports by the `filer_name` string and the
--   Contributions dataset (3kfv-biw6) by the same string in `recipient`.
--   Link identity is therefore filer_key — the normalized filer name
--   (backend normalizeAustinFinanceTextKey: NFKD, diacritics stripped,
--   upper-cased, non-alphanumerics collapsed to single spaces) — while
--   filer_name keeps the exact source spelling because the sync queries
--   Socrata with `filer_name = '<that string>'`. The CHECK pins the
--   normalized form so a raw name can never masquerade as a key. The office
--   is NOT part of the key: `office_sought` drifts across a filer's own rows
--   ("COUNCIL_MBR_DISTRICT_01", "... District 1", "... District One" all
--   observed live 2026-08-18), so it lives in the standard office_name /
--   district columns and Phase 3 filters reports by the parsed office code.
-- * cash_on_hand is the Texas cover's `contrib_balance` — a signed balance
--   (the GA/MA lesson of migrations 231/232); every flow stays nonnegative.
--   The cover's `outstand_loan` is a loan BALANCE, not loans received, and no
--   standard read column publishes it, so it has no column here (revisit
--   with a `loans_received` column, Denver-240 style, if Phase 3 derives loan
--   receipts from itemized rows).
-- * Outside spenders come from Direct Campaign Expenditures (8p2b-ewep)
--   `paid_by` — again a name, no id — so the outside tables key on
--   spender_key (same normalizer) + spender_name (display spelling).
--   Direction (support/oppose) is the city Committee Purpose join; DCE
--   dollars with no stated direction cannot enter a support/oppose total and
--   are reported in the loader's coverage note instead (never stored).
-- * outside_group_breakdowns is FILLED (unlike Denver): PAC receipts sit in
--   the same Contributions dataset, so per-spender donor/industry rows come
--   for free. Its FK cascades from the group row.
-- Constraints and triggers on the two long tables use the atx_cf_ prefix so
-- no generated identifier exceeds Postgres's 63-character limit.

BEGIN;

CREATE TABLE public.atx_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  candidate_name_normalized text NOT NULL CHECK (btrim(candidate_name_normalized) <> ''),
  office_name text NOT NULL CHECK (btrim(office_name) <> ''),
  district text CHECK (district IS NULL OR btrim(district) <> ''),
  -- Normalized filer name: upper-case alphanumeric tokens, single spaces.
  -- Derived by the writer from filer_name; a raw name here is a wiring bug.
  filer_key text NOT NULL CHECK (filer_key ~ '^[A-Z0-9]+( [A-Z0-9]+)*$'),
  -- Exact Socrata spelling of Report Detail `filer_name` / Contributions
  -- `recipient` — the string the sync queries by.
  filer_name text NOT NULL CHECK (btrim(filer_name) <> ''),
  link_status text NOT NULL DEFAULT 'active' CHECK (link_status IN ('active', 'needs_review', 'inactive')),
  link_source text NOT NULL DEFAULT 'manual' CHECK (link_source IN ('manual', 'austin_clerk')),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT atx_candidate_finance_links_candidate_election_filer_key
    UNIQUE (candidate_id, election_id, filer_key),
  UNIQUE (id, election_year)
);
CREATE UNIQUE INDEX atx_candidate_finance_links_active_candidate_election_idx
  ON public.atx_candidate_finance_links (candidate_id, election_id) WHERE link_status = 'active';
CREATE INDEX atx_candidate_finance_links_election_candidate_idx
  ON public.atx_candidate_finance_links (election_id, candidate_id);
CREATE TRIGGER atx_candidate_finance_links_set_updated_at
  BEFORE UPDATE ON public.atx_candidate_finance_links
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.atx_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  -- Sum of `contrib_total` over the cycle's effective reports (the Texas
  -- cover's "total political contributions", which already excludes loans).
  total_receipts numeric(16,2),
  -- Same figure: Austin has no public-financing or other non-donor receipts
  -- to split out (Houston/TEC covers publish the two columns equal as well).
  direct_contribution_total numeric(16,2),
  -- Sum of `expend_total` over the same effective reports.
  total_disbursements numeric(16,2),
  -- `contrib_balance` of the latest effective report: a signed BALANCE.
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
    REFERENCES public.atx_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year)
);
CREATE TRIGGER atx_candidate_finance_summaries_set_updated_at
  BEFORE UPDATE ON public.atx_candidate_finance_summaries
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.atx_candidate_finance_direct_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  category_type text NOT NULL CHECK (category_type IN ('occupation', 'contribution_size')),
  category_name text NOT NULL CHECK (btrim(category_name) <> ''),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0),
  contributor_count integer
    CONSTRAINT atx_cf_direct_breakdowns_contributor_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT atx_cf_direct_breakdowns_link_year_fkey
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.atx_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT atx_cf_direct_breakdowns_category_key
    UNIQUE (link_id, election_year, category_type, category_name)
);
CREATE INDEX atx_candidate_finance_direct_breakdowns_lookup_idx
  ON public.atx_candidate_finance_direct_breakdowns (link_id, election_year DESC, category_type, amount DESC);
CREATE TRIGGER atx_candidate_finance_direct_breakdowns_set_updated_at
  BEFORE UPDATE ON public.atx_candidate_finance_direct_breakdowns
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Per-cycle outside-spending amounts, one row per (spender, direction).
-- spender_key is the normalized `paid_by` name (same normalizer and CHECK as
-- filer_key); spender_name is the display spelling the aggregator chose.
CREATE TABLE public.atx_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  spender_key text NOT NULL CHECK (spender_key ~ '^[A-Z0-9]+( [A-Z0-9]+)*$'),
  spender_name text NOT NULL CHECK (btrim(spender_name) <> ''),
  support_oppose text NOT NULL CHECK (support_oppose IN ('support', 'oppose')),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT atx_cf_outside_groups_link_year_fkey
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.atx_candidate_finance_links(id, election_year)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT atx_cf_outside_groups_spender_key
    UNIQUE (link_id, election_year, spender_key, support_oppose)
);
CREATE INDEX atx_candidate_finance_outside_groups_lookup_idx
  ON public.atx_candidate_finance_outside_groups (link_id, election_year DESC, support_oppose, amount DESC);
CREATE TRIGGER atx_candidate_finance_outside_groups_set_updated_at
  BEFORE UPDATE ON public.atx_candidate_finance_outside_groups
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Per-spender funder rows (donor names / industries) behind each outside
-- group. The group FK cascades, so a snapshot that deletes-and-reinserts
-- outside groups can never strand breakdown rows.
CREATE TABLE public.atx_candidate_finance_outside_group_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL
    CONSTRAINT atx_cf_outside_group_breakdowns_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  spender_key text NOT NULL
    CONSTRAINT atx_cf_outside_group_breakdowns_spender_key_check
    CHECK (spender_key ~ '^[A-Z0-9]+( [A-Z0-9]+)*$'),
  support_oppose text NOT NULL
    CONSTRAINT atx_cf_outside_group_breakdowns_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  category_type text NOT NULL
    CONSTRAINT atx_cf_outside_group_breakdowns_type_check
    CHECK (category_type IN ('donor', 'industry')),
  category_name text NOT NULL
    CONSTRAINT atx_cf_outside_group_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  amount numeric(16,2) NOT NULL
    CONSTRAINT atx_cf_outside_group_breakdowns_amount_check
    CHECK (amount >= 0),
  contributor_count integer
    CONSTRAINT atx_cf_outside_group_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  source_url text
    CONSTRAINT atx_cf_outside_group_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT atx_cf_outside_group_breakdowns_unique
    UNIQUE (link_id, election_year, spender_key, support_oppose, category_type, category_name),
  CONSTRAINT atx_cf_outside_group_breakdowns_group_fkey
    FOREIGN KEY (link_id, election_year, spender_key, support_oppose)
    REFERENCES public.atx_candidate_finance_outside_groups (link_id, election_year, spender_key, support_oppose)
    ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX atx_cf_outside_group_breakdowns_lookup_idx
  ON public.atx_candidate_finance_outside_group_breakdowns (link_id, election_year DESC, support_oppose, category_type, amount DESC);
CREATE TRIGGER atx_cf_outside_group_breakdowns_set_updated_at
  BEFORE UPDATE ON public.atx_candidate_finance_outside_group_breakdowns
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
