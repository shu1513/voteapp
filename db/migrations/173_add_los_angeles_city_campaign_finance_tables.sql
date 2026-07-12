BEGIN;

CREATE TABLE public.lacity_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  candidate_name_normalized text NOT NULL CHECK (btrim(candidate_name_normalized) <> ''),
  office_name text NOT NULL CHECK (btrim(office_name) <> ''),
  ethics_election_id text NOT NULL CHECK (btrim(ethics_election_id) <> ''),
  ethics_candidate_person_id text NOT NULL CHECK (btrim(ethics_candidate_person_id) <> ''),
  ethics_seat_candidate_id text NOT NULL CHECK (btrim(ethics_seat_candidate_id) <> ''),
  fppc_committee_id text NOT NULL CHECK (btrim(fppc_committee_id) <> ''),
  committee_name text NOT NULL CHECK (btrim(committee_name) <> ''),
  internal_committee_person_id text,
  link_status text NOT NULL DEFAULT 'active' CHECK (link_status IN ('active', 'needs_review', 'inactive')),
  link_source text NOT NULL DEFAULT 'manual' CHECK (link_source IN ('manual', 'lacity_ethics')),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (candidate_id, election_id, fppc_committee_id),
  UNIQUE (id, election_year)
);
CREATE UNIQUE INDEX lacity_candidate_finance_links_active_candidate_election_idx
  ON public.lacity_candidate_finance_links (candidate_id, election_id) WHERE link_status = 'active';
CREATE INDEX lacity_candidate_finance_links_election_candidate_idx ON public.lacity_candidate_finance_links (election_id, candidate_id);
CREATE INDEX lacity_candidate_finance_links_ethics_candidate_idx ON public.lacity_candidate_finance_links (ethics_election_id, ethics_candidate_person_id);
CREATE TRIGGER lacity_candidate_finance_links_set_updated_at BEFORE UPDATE ON public.lacity_candidate_finance_links FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.lacity_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), link_id uuid NOT NULL, election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  total_receipts numeric(16,2), total_disbursements numeric(16,2), cash_on_hand numeric(16,2), matching_funds numeric(16,2),
  outside_support_total numeric(16,2), outside_oppose_total numeric(16,2), membership_support_total numeric(16,2), membership_oppose_total numeric(16,2),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''), reported_through date, last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((total_receipts IS NULL OR total_receipts >= 0) AND (total_disbursements IS NULL OR total_disbursements >= 0) AND (cash_on_hand IS NULL OR cash_on_hand >= 0) AND (matching_funds IS NULL OR matching_funds >= 0) AND (outside_support_total IS NULL OR outside_support_total >= 0) AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0) AND (membership_support_total IS NULL OR membership_support_total >= 0) AND (membership_oppose_total IS NULL OR membership_oppose_total >= 0)),
  FOREIGN KEY (link_id, election_year) REFERENCES public.lacity_candidate_finance_links(id, election_year) ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year)
);
CREATE INDEX lacity_candidate_finance_summaries_lookup_idx ON public.lacity_candidate_finance_summaries (link_id, election_year DESC);
CREATE TRIGGER lacity_candidate_finance_summaries_set_updated_at BEFORE UPDATE ON public.lacity_candidate_finance_summaries FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.lacity_candidate_finance_direct_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), link_id uuid NOT NULL, election_year integer NOT NULL,
  category_type text NOT NULL CHECK (category_type IN ('occupation', 'employer', 'industry', 'contribution_size')),
  category_name text NOT NULL CHECK (btrim(category_name) <> ''), amount numeric(16,2) NOT NULL CHECK (amount >= 0), contributor_count integer CHECK (contributor_count IS NULL OR contributor_count >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''), last_synced_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (link_id, election_year) REFERENCES public.lacity_candidate_finance_links(id, election_year) ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year, category_type, category_name)
);
CREATE INDEX lacity_candidate_finance_direct_breakdowns_lookup_idx ON public.lacity_candidate_finance_direct_breakdowns (link_id, election_year DESC, category_type, amount DESC);
CREATE TRIGGER lacity_candidate_finance_direct_breakdowns_set_updated_at BEFORE UPDATE ON public.lacity_candidate_finance_direct_breakdowns FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.lacity_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), link_id uuid NOT NULL, election_year integer NOT NULL,
  spender_id text NOT NULL CHECK (btrim(spender_id) <> ''), spender_name text NOT NULL CHECK (btrim(spender_name) <> ''), support_oppose text NOT NULL CHECK (support_oppose IN ('support', 'oppose')),
  amount numeric(16,2) NOT NULL CHECK (amount >= 0), expenditure_count integer CHECK (expenditure_count IS NULL OR expenditure_count >= 0),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''), last_synced_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (link_id, election_year) REFERENCES public.lacity_candidate_finance_links(id, election_year) ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE (link_id, election_year, spender_id, support_oppose)
);
CREATE INDEX lacity_candidate_finance_outside_groups_lookup_idx ON public.lacity_candidate_finance_outside_groups (link_id, election_year DESC, support_oppose, amount DESC);
CREATE TRIGGER lacity_candidate_finance_outside_groups_set_updated_at BEFORE UPDATE ON public.lacity_candidate_finance_outside_groups FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
