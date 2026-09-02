BEGIN;

-- Idaho campaign finance (docs/plans/idaho-finance.md, Phase 1).
-- Same Civix CFIS build as New Hampshire (migration 249); mirrors that shape
-- with the plan's adaptations:
--   * Link identity is the Sunshine registration guid (one registration per
--     candidate per cycle; grid `guid` = contribution-row
--     `filerRegistrationGuid` = IE-row `candidateMeasureFilerRegistrationGuid`),
--     stored as lowercase uuid text in registration_guid with the grid's
--     filerName in filer_name. Text, not uuid, so the standard writer/loader
--     compare it like every other state's committee id.
--   * election_year floor is 2026: Nov-2026 scope only.
--   * Direct breakdowns are size buckets or contributor source types
--     (Vermont precedent): Idaho collects no donor occupation or employer.
--   * link_source values are ('manual', 'sunshine_grid').
--   * cash_on_hand is a signed balance: the grid reports negative
--     balanceOfFunds for indebted campaigns (live 2026-09-01: -$1,321.99), so
--     the amounts CHECK deliberately excludes it (Arkansas precedent, 266).
--   * Outside groups are IE filers keyed by filer_key: the filer's
--     registration guid when registered in Idaho, otherwise a name-derived
--     key (non-registered filers such as federal PACs carry no guid).

CREATE TABLE IF NOT EXISTS public.id_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL,
  candidate_name_normalized text NOT NULL,
  office_name text NOT NULL,
  district text,
  registration_guid text NOT NULL,
  filer_name text NOT NULL,
  link_status text NOT NULL DEFAULT 'active',
  link_source text NOT NULL DEFAULT 'manual',
  source_url text,
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT id_candidate_finance_links_year_check
    CHECK (election_year BETWEEN 2026 AND 2100),
  CONSTRAINT id_candidate_finance_links_candidate_name_check
    CHECK (btrim(candidate_name_normalized) <> ''),
  CONSTRAINT id_candidate_finance_links_office_name_check
    CHECK (btrim(office_name) <> ''),
  CONSTRAINT id_candidate_finance_links_district_check
    CHECK (district IS NULL OR btrim(district) <> ''),
  CONSTRAINT id_candidate_finance_links_registration_guid_check
    CHECK (registration_guid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
  CONSTRAINT id_candidate_finance_links_filer_name_check
    CHECK (btrim(filer_name) <> ''),
  CONSTRAINT id_candidate_finance_links_status_check
    CHECK (link_status IN ('active', 'inactive')),
  CONSTRAINT id_candidate_finance_links_source_check
    CHECK (link_source IN ('manual', 'sunshine_grid')),
  CONSTRAINT id_candidate_finance_links_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT id_candidate_finance_links_unique
    UNIQUE (candidate_id, election_id, registration_guid),
  CONSTRAINT id_candidate_finance_links_id_year_unique
    UNIQUE (id, election_year)
);

CREATE INDEX IF NOT EXISTS id_candidate_finance_links_election_candidate_idx
  ON public.id_candidate_finance_links (election_id, candidate_id);

CREATE INDEX IF NOT EXISTS id_candidate_finance_links_registration_year_idx
  ON public.id_candidate_finance_links (registration_guid, election_year);

DROP TRIGGER IF EXISTS id_candidate_finance_links_set_updated_at
  ON public.id_candidate_finance_links;
CREATE TRIGGER id_candidate_finance_links_set_updated_at
BEFORE UPDATE ON public.id_candidate_finance_links
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.id_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  total_receipts numeric(16,2),
  direct_contribution_total numeric(16,2),
  total_disbursements numeric(16,2),
  cash_on_hand numeric(16,2),
  outside_support_total numeric(16,2),
  outside_oppose_total numeric(16,2),
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT id_candidate_finance_summaries_year_check
    CHECK (election_year BETWEEN 2026 AND 2100),
  CONSTRAINT id_candidate_finance_summaries_amounts_check
    CHECK (
      (total_receipts IS NULL OR total_receipts >= 0)
      AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
      AND (total_disbursements IS NULL OR total_disbursements >= 0)
      AND (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    ),
  CONSTRAINT id_candidate_finance_summaries_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT id_candidate_finance_summaries_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.id_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT id_candidate_finance_summaries_unique
    UNIQUE (link_id, election_year)
);

CREATE INDEX IF NOT EXISTS id_candidate_finance_summaries_lookup_idx
  ON public.id_candidate_finance_summaries (link_id, election_year DESC);

DROP TRIGGER IF EXISTS id_candidate_finance_summaries_set_updated_at
  ON public.id_candidate_finance_summaries;
CREATE TRIGGER id_candidate_finance_summaries_set_updated_at
BEFORE UPDATE ON public.id_candidate_finance_summaries
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.id_candidate_finance_direct_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  category_type text NOT NULL,
  category_name text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT id_candidate_finance_direct_breakdowns_year_check
    CHECK (election_year BETWEEN 2026 AND 2100),
  CONSTRAINT id_candidate_finance_direct_breakdowns_type_check
    CHECK (category_type IN ('contribution_size', 'contributor_source_type')),
  CONSTRAINT id_candidate_finance_direct_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT id_candidate_finance_direct_breakdowns_amount_check
    CHECK (amount >= 0),
  CONSTRAINT id_candidate_finance_direct_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT id_candidate_finance_direct_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT id_candidate_finance_direct_breakdowns_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.id_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT id_candidate_finance_direct_breakdowns_unique
    UNIQUE (link_id, election_year, category_type, category_name)
);

CREATE INDEX IF NOT EXISTS id_candidate_finance_direct_breakdowns_lookup_idx
  ON public.id_candidate_finance_direct_breakdowns (
    link_id,
    election_year DESC,
    category_type,
    amount DESC
  );

DROP TRIGGER IF EXISTS id_candidate_finance_direct_breakdowns_set_updated_at
  ON public.id_candidate_finance_direct_breakdowns;
CREATE TRIGGER id_candidate_finance_direct_breakdowns_set_updated_at
BEFORE UPDATE ON public.id_candidate_finance_direct_breakdowns
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.id_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  filer_key text NOT NULL,
  filer_name text NOT NULL,
  support_oppose text NOT NULL,
  amount numeric(16,2) NOT NULL,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT id_candidate_finance_outside_groups_year_check
    CHECK (election_year BETWEEN 2026 AND 2100),
  CONSTRAINT id_candidate_finance_outside_groups_filer_key_check
    CHECK (btrim(filer_key) <> ''),
  CONSTRAINT id_candidate_finance_outside_groups_filer_name_check
    CHECK (btrim(filer_name) <> ''),
  CONSTRAINT id_candidate_finance_outside_groups_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT id_candidate_finance_outside_groups_amount_check
    CHECK (amount >= 0),
  CONSTRAINT id_candidate_finance_outside_groups_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT id_candidate_finance_outside_groups_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.id_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT id_candidate_finance_outside_groups_unique
    UNIQUE (link_id, election_year, filer_key, support_oppose)
);

CREATE INDEX IF NOT EXISTS id_candidate_finance_outside_groups_lookup_idx
  ON public.id_candidate_finance_outside_groups (
    link_id,
    election_year DESC,
    support_oppose,
    amount DESC
  );

DROP TRIGGER IF EXISTS id_candidate_finance_outside_groups_set_updated_at
  ON public.id_candidate_finance_outside_groups;
CREATE TRIGGER id_candidate_finance_outside_groups_set_updated_at
BEFORE UPDATE ON public.id_candidate_finance_outside_groups
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.id_candidate_finance_outside_group_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  filer_key text NOT NULL,
  support_oppose text NOT NULL,
  category_type text NOT NULL,
  category_name text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT id_cff_outside_group_breakdowns_year_check
    CHECK (election_year BETWEEN 2026 AND 2100),
  CONSTRAINT id_cff_outside_group_breakdowns_filer_key_check
    CHECK (btrim(filer_key) <> ''),
  CONSTRAINT id_cff_outside_group_breakdowns_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT id_cff_outside_group_breakdowns_type_check
    CHECK (category_type IN ('donor', 'industry')),
  CONSTRAINT id_cff_outside_group_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT id_cff_outside_group_breakdowns_amount_check
    CHECK (amount >= 0),
  CONSTRAINT id_cff_outside_group_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT id_cff_outside_group_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT id_cff_outside_group_breakdowns_unique
    UNIQUE (
      link_id,
      election_year,
      filer_key,
      support_oppose,
      category_type,
      category_name
    ),
  CONSTRAINT id_cff_outside_group_breakdowns_group_fk
    FOREIGN KEY (link_id, election_year, filer_key, support_oppose)
    REFERENCES public.id_candidate_finance_outside_groups (
      link_id,
      election_year,
      filer_key,
      support_oppose
    )
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS id_cff_outside_group_breakdowns_lookup_idx
  ON public.id_candidate_finance_outside_group_breakdowns (
    link_id,
    election_year DESC,
    support_oppose,
    category_type,
    amount DESC
  );

DROP TRIGGER IF EXISTS id_cff_outside_group_breakdowns_set_updated_at
  ON public.id_candidate_finance_outside_group_breakdowns;
CREATE TRIGGER id_cff_outside_group_breakdowns_set_updated_at
BEFORE UPDATE ON public.id_candidate_finance_outside_group_breakdowns
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
