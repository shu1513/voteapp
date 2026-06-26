BEGIN;

CREATE TABLE IF NOT EXISTS public.tn_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL,
  candidate_name_normalized text NOT NULL,
  office_name text NOT NULL,
  district text,
  camp_candidate_id text NOT NULL,
  owner_name text NOT NULL,
  committee_name text,
  link_status text NOT NULL DEFAULT 'active',
  link_source text NOT NULL DEFAULT 'manual',
  source_url text,
  report_list_url text,
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tn_candidate_finance_links_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT tn_candidate_finance_links_candidate_name_check
    CHECK (btrim(candidate_name_normalized) <> ''),
  CONSTRAINT tn_candidate_finance_links_office_name_check
    CHECK (btrim(office_name) <> ''),
  CONSTRAINT tn_candidate_finance_links_district_check
    CHECK (district IS NULL OR btrim(district) <> ''),
  CONSTRAINT tn_candidate_finance_links_camp_candidate_id_check
    CHECK (btrim(camp_candidate_id) <> ''),
  CONSTRAINT tn_candidate_finance_links_owner_name_check
    CHECK (btrim(owner_name) <> ''),
  CONSTRAINT tn_candidate_finance_links_committee_name_check
    CHECK (committee_name IS NULL OR btrim(committee_name) <> ''),
  CONSTRAINT tn_candidate_finance_links_status_check
    CHECK (link_status IN ('active', 'inactive', 'ambiguous')),
  CONSTRAINT tn_candidate_finance_links_source_check
    CHECK (link_source IN ('manual', 'tncamp_search')),
  CONSTRAINT tn_candidate_finance_links_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT tn_candidate_finance_links_report_list_url_check
    CHECK (report_list_url IS NULL OR btrim(report_list_url) <> ''),
  CONSTRAINT tn_candidate_finance_links_unique
    UNIQUE (candidate_id, election_id, camp_candidate_id),
  CONSTRAINT tn_candidate_finance_links_id_year_unique
    UNIQUE (id, election_year)
);

CREATE INDEX IF NOT EXISTS tn_candidate_finance_links_election_candidate_idx
  ON public.tn_candidate_finance_links (election_id, candidate_id);

CREATE INDEX IF NOT EXISTS tn_candidate_finance_links_camp_candidate_year_idx
  ON public.tn_candidate_finance_links (camp_candidate_id, election_year);

DROP TRIGGER IF EXISTS tn_candidate_finance_links_set_updated_at
  ON public.tn_candidate_finance_links;
CREATE TRIGGER tn_candidate_finance_links_set_updated_at
BEFORE UPDATE ON public.tn_candidate_finance_links
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.tn_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  total_receipts numeric(16,2),
  direct_contribution_total numeric(16,2),
  outside_support_total numeric(16,2),
  outside_oppose_total numeric(16,2),
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tn_candidate_finance_summaries_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT tn_candidate_finance_summaries_amounts_check
    CHECK (
      (total_receipts IS NULL OR total_receipts >= 0)
      AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
      AND (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    ),
  CONSTRAINT tn_candidate_finance_summaries_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT tn_candidate_finance_summaries_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.tn_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT tn_candidate_finance_summaries_unique
    UNIQUE (link_id, election_year)
);

DROP TRIGGER IF EXISTS tn_candidate_finance_summaries_set_updated_at
  ON public.tn_candidate_finance_summaries;
CREATE TRIGGER tn_candidate_finance_summaries_set_updated_at
BEFORE UPDATE ON public.tn_candidate_finance_summaries
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.tn_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  committee_key text NOT NULL,
  committee_name text NOT NULL,
  support_oppose text NOT NULL,
  amount numeric(16,2) NOT NULL,
  expenditure_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tn_cff_outside_groups_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT tn_cff_outside_groups_committee_key_check
    CHECK (btrim(committee_key) <> ''),
  CONSTRAINT tn_cff_outside_groups_committee_name_check
    CHECK (btrim(committee_name) <> ''),
  CONSTRAINT tn_cff_outside_groups_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT tn_cff_outside_groups_amount_check
    CHECK (amount >= 0),
  CONSTRAINT tn_cff_outside_groups_expenditure_count_check
    CHECK (expenditure_count IS NULL OR expenditure_count >= 0),
  CONSTRAINT tn_cff_outside_groups_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT tn_cff_outside_groups_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.tn_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT tn_cff_outside_groups_unique
    UNIQUE (link_id, election_year, committee_key, support_oppose)
);

CREATE INDEX IF NOT EXISTS tn_cff_outside_groups_lookup_idx
  ON public.tn_candidate_finance_outside_groups (
    link_id,
    election_year DESC,
    support_oppose,
    amount DESC
  );

DROP TRIGGER IF EXISTS tn_cff_outside_groups_set_updated_at
  ON public.tn_candidate_finance_outside_groups;
CREATE TRIGGER tn_cff_outside_groups_set_updated_at
BEFORE UPDATE ON public.tn_candidate_finance_outside_groups
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.tn_candidate_finance_outside_group_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  committee_key text NOT NULL,
  support_oppose text NOT NULL,
  category_type text NOT NULL,
  category_name text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tn_cff_outside_breakdowns_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT tn_cff_outside_breakdowns_committee_key_check
    CHECK (btrim(committee_key) <> ''),
  CONSTRAINT tn_cff_outside_breakdowns_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT tn_cff_outside_breakdowns_type_check
    CHECK (category_type IN ('donor', 'employer', 'occupation', 'industry')),
  CONSTRAINT tn_cff_outside_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT tn_cff_outside_breakdowns_amount_check
    CHECK (amount >= 0),
  CONSTRAINT tn_cff_outside_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT tn_cff_outside_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT tn_cff_outside_breakdowns_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.tn_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT tn_cff_outside_breakdowns_unique
    UNIQUE (link_id, election_year, committee_key, support_oppose, category_type, category_name)
);

CREATE INDEX IF NOT EXISTS tn_cff_outside_breakdowns_lookup_idx
  ON public.tn_candidate_finance_outside_group_breakdowns (
    link_id,
    election_year DESC,
    support_oppose,
    category_type,
    amount DESC
  );

DROP TRIGGER IF EXISTS tn_cff_outside_breakdowns_set_updated_at
  ON public.tn_candidate_finance_outside_group_breakdowns;
CREATE TRIGGER tn_cff_outside_breakdowns_set_updated_at
BEFORE UPDATE ON public.tn_candidate_finance_outside_group_breakdowns
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.tn_candidate_finance_direct_breakdowns (
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
  CONSTRAINT tn_cff_direct_breakdowns_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT tn_cff_direct_breakdowns_type_check
    CHECK (category_type IN ('occupation', 'contribution_size')),
  CONSTRAINT tn_cff_direct_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT tn_cff_direct_breakdowns_amount_check
    CHECK (amount >= 0),
  CONSTRAINT tn_cff_direct_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT tn_cff_direct_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT tn_cff_direct_breakdowns_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.tn_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT tn_cff_direct_breakdowns_unique
    UNIQUE (link_id, election_year, category_type, category_name)
);

CREATE INDEX IF NOT EXISTS tn_cff_direct_breakdowns_lookup_idx
  ON public.tn_candidate_finance_direct_breakdowns (
    link_id,
    election_year DESC,
    category_type,
    amount DESC
  );

DROP TRIGGER IF EXISTS tn_cff_direct_breakdowns_set_updated_at
  ON public.tn_candidate_finance_direct_breakdowns;
CREATE TRIGGER tn_cff_direct_breakdowns_set_updated_at
BEFORE UPDATE ON public.tn_candidate_finance_direct_breakdowns
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
