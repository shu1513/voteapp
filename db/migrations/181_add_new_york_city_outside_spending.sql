BEGIN;

ALTER TABLE public.nyc_candidate_finance_summaries
  ADD COLUMN outside_support_total numeric(16,2),
  ADD COLUMN outside_oppose_total numeric(16,2),
  ADD CONSTRAINT nyc_candidate_finance_summaries_outside_amounts_check
    CHECK (
      (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    ) NOT VALID;

ALTER TABLE public.nyc_candidate_finance_summaries
  VALIDATE CONSTRAINT nyc_candidate_finance_summaries_outside_amounts_check;

CREATE TABLE public.nyc_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  spender_id text NOT NULL,
  spender_name text NOT NULL,
  support_oppose text NOT NULL,
  amount numeric(16,2) NOT NULL,
  expenditure_count integer NOT NULL,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT nyc_candidate_finance_outside_groups_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT nyc_candidate_finance_outside_groups_spender_id_check
    CHECK (btrim(spender_id) <> ''),
  CONSTRAINT nyc_candidate_finance_outside_groups_spender_name_check
    CHECK (btrim(spender_name) <> ''),
  CONSTRAINT nyc_candidate_finance_outside_groups_direction_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT nyc_candidate_finance_outside_groups_amount_check
    CHECK (amount >= 0),
  CONSTRAINT nyc_candidate_finance_outside_groups_count_check
    CHECK (expenditure_count >= 0),
  CONSTRAINT nyc_candidate_finance_outside_groups_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT nyc_candidate_finance_outside_groups_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.nyc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT nyc_candidate_finance_outside_groups_unique
    UNIQUE (link_id, election_year, spender_id, support_oppose)
);

CREATE INDEX nyc_candidate_finance_outside_groups_lookup_idx
  ON public.nyc_candidate_finance_outside_groups (
    link_id,
    election_year DESC,
    support_oppose,
    amount DESC
  );

CREATE TRIGGER nyc_candidate_finance_outside_groups_set_updated_at
BEFORE UPDATE ON public.nyc_candidate_finance_outside_groups
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.nyc_candidate_finance_outside_group_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  spender_id text NOT NULL,
  support_oppose text NOT NULL,
  category_type text NOT NULL,
  category_name text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text NOT NULL,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_spender_id_check
    CHECK (btrim(spender_id) <> ''),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_direction_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_type_check
    CHECK (category_type IN ('donor', 'industry')),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_amount_check
    CHECK (amount >= 0),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_source_url_check
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_unique
    UNIQUE (link_id, election_year, spender_id, support_oppose, category_type, category_name),
  CONSTRAINT nyc_candidate_finance_outside_group_breakdowns_group_fk
    FOREIGN KEY (link_id, election_year, spender_id, support_oppose)
    REFERENCES public.nyc_candidate_finance_outside_groups (
      link_id,
      election_year,
      spender_id,
      support_oppose
    )
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE INDEX nyc_candidate_finance_outside_group_breakdowns_lookup_idx
  ON public.nyc_candidate_finance_outside_group_breakdowns (
    link_id,
    election_year DESC,
    support_oppose,
    category_type,
    amount DESC
  );

CREATE TRIGGER nyc_candidate_finance_outside_group_breakdowns_set_updated_at
BEFORE UPDATE ON public.nyc_candidate_finance_outside_group_breakdowns
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
