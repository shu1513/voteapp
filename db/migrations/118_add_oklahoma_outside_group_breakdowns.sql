BEGIN;

CREATE TABLE IF NOT EXISTS public.ok_candidate_finance_outside_group_breakdowns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  committee_id text NOT NULL,
  support_oppose text NOT NULL,
  category_type text NOT NULL,
  category_name text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ok_candidate_finance_outside_group_breakdowns_year_check
    CHECK (election_year BETWEEN 2014 AND 2100),
  CONSTRAINT ok_cff_outside_group_breakdowns_committee_id_check
    CHECK (btrim(committee_id) <> ''),
  CONSTRAINT ok_cff_outside_group_breakdowns_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT ok_candidate_finance_outside_group_breakdowns_type_check
    CHECK (category_type IN ('donor', 'industry')),
  CONSTRAINT ok_candidate_finance_outside_group_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT ok_candidate_finance_outside_group_breakdowns_amount_check
    CHECK (amount >= 0),
  CONSTRAINT ok_candidate_finance_outside_group_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT ok_candidate_finance_outside_group_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT ok_candidate_finance_outside_group_breakdowns_unique
    UNIQUE (
      link_id,
      election_year,
      committee_id,
      support_oppose,
      category_type,
      category_name
    ),
  CONSTRAINT ok_candidate_finance_outside_group_breakdowns_group_fk
    FOREIGN KEY (link_id, election_year, committee_id, support_oppose)
    REFERENCES public.ok_candidate_finance_outside_groups (
      link_id,
      election_year,
      committee_id,
      support_oppose
    )
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ok_candidate_finance_outside_group_breakdowns_lookup_idx
  ON public.ok_candidate_finance_outside_group_breakdowns (
    link_id,
    election_year DESC,
    support_oppose,
    category_type,
    amount DESC
  );

DROP TRIGGER IF EXISTS ok_candidate_finance_outside_group_breakdowns_set_updated_at
  ON public.ok_candidate_finance_outside_group_breakdowns;
CREATE TRIGGER ok_candidate_finance_outside_group_breakdowns_set_updated_at
BEFORE UPDATE ON public.ok_candidate_finance_outside_group_breakdowns
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
