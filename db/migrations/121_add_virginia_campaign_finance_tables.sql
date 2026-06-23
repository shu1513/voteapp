BEGIN;

CREATE TABLE IF NOT EXISTS public.va_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL,
  candidate_name_normalized text NOT NULL,
  office_name text NOT NULL,
  district text,
  committee_id text NOT NULL,
  committee_code text,
  committee_name text NOT NULL,
  link_status text NOT NULL DEFAULT 'active',
  link_source text NOT NULL DEFAULT 'manual',
  source_url text,
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT va_candidate_finance_links_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT va_candidate_finance_links_candidate_name_check
    CHECK (btrim(candidate_name_normalized) <> ''),
  CONSTRAINT va_candidate_finance_links_office_name_check
    CHECK (btrim(office_name) <> ''),
  CONSTRAINT va_candidate_finance_links_district_check
    CHECK (district IS NULL OR btrim(district) <> ''),
  CONSTRAINT va_candidate_finance_links_committee_id_check
    CHECK (btrim(committee_id) <> ''),
  CONSTRAINT va_candidate_finance_links_committee_code_check
    CHECK (committee_code IS NULL OR btrim(committee_code) <> ''),
  CONSTRAINT va_candidate_finance_links_committee_name_check
    CHECK (btrim(committee_name) <> ''),
  CONSTRAINT va_candidate_finance_links_status_check
    CHECK (link_status IN ('active', 'inactive')),
  CONSTRAINT va_candidate_finance_links_source_check
    CHECK (link_source IN ('manual', 'cfreports_search', 'cfreports_xml')),
  CONSTRAINT va_candidate_finance_links_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT va_candidate_finance_links_unique
    UNIQUE (candidate_id, election_id, committee_id),
  CONSTRAINT va_candidate_finance_links_id_year_unique
    UNIQUE (id, election_year)
);

CREATE INDEX IF NOT EXISTS va_candidate_finance_links_election_candidate_idx
  ON public.va_candidate_finance_links (election_id, candidate_id);

CREATE INDEX IF NOT EXISTS va_candidate_finance_links_committee_year_idx
  ON public.va_candidate_finance_links (committee_id, election_year);

DROP TRIGGER IF EXISTS va_candidate_finance_links_set_updated_at
  ON public.va_candidate_finance_links;
CREATE TRIGGER va_candidate_finance_links_set_updated_at
BEFORE UPDATE ON public.va_candidate_finance_links
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.va_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  total_receipts numeric(16,2),
  direct_contribution_total numeric(16,2),
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT va_candidate_finance_summaries_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT va_candidate_finance_summaries_amounts_check
    CHECK (
      (total_receipts IS NULL OR total_receipts >= 0)
      AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
    ),
  CONSTRAINT va_candidate_finance_summaries_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT va_candidate_finance_summaries_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.va_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT va_candidate_finance_summaries_unique
    UNIQUE (link_id, election_year)
);

CREATE INDEX IF NOT EXISTS va_candidate_finance_summaries_lookup_idx
  ON public.va_candidate_finance_summaries (link_id, election_year DESC);

DROP TRIGGER IF EXISTS va_candidate_finance_summaries_set_updated_at
  ON public.va_candidate_finance_summaries;
CREATE TRIGGER va_candidate_finance_summaries_set_updated_at
BEFORE UPDATE ON public.va_candidate_finance_summaries
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.va_candidate_finance_direct_breakdowns (
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
  CONSTRAINT va_cff_direct_breakdowns_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT va_cff_direct_breakdowns_type_check
    CHECK (category_type IN ('occupation', 'contribution_size')),
  CONSTRAINT va_cff_direct_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT va_cff_direct_breakdowns_amount_check
    CHECK (amount >= 0),
  CONSTRAINT va_cff_direct_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT va_cff_direct_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT va_cff_direct_breakdowns_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.va_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT va_cff_direct_breakdowns_unique
    UNIQUE (link_id, election_year, category_type, category_name)
);

CREATE INDEX IF NOT EXISTS va_cff_direct_breakdowns_lookup_idx
  ON public.va_candidate_finance_direct_breakdowns (
    link_id,
    election_year DESC,
    category_type,
    amount DESC
  );

DROP TRIGGER IF EXISTS va_cff_direct_breakdowns_set_updated_at
  ON public.va_candidate_finance_direct_breakdowns;
CREATE TRIGGER va_cff_direct_breakdowns_set_updated_at
BEFORE UPDATE ON public.va_candidate_finance_direct_breakdowns
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
