BEGIN;

ALTER TABLE public.ok_candidate_finance_summaries
  ADD COLUMN IF NOT EXISTS outside_support_total numeric(16,2),
  ADD COLUMN IF NOT EXISTS outside_oppose_total numeric(16,2);

ALTER TABLE public.ok_candidate_finance_summaries
  DROP CONSTRAINT IF EXISTS ok_candidate_finance_summaries_amounts_check;

ALTER TABLE public.ok_candidate_finance_summaries
  ADD CONSTRAINT ok_candidate_finance_summaries_amounts_check
  CHECK (
    (total_receipts IS NULL OR total_receipts >= 0)
    AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
    AND (outside_support_total IS NULL OR outside_support_total >= 0)
    AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
  );

CREATE TABLE IF NOT EXISTS public.ok_candidate_finance_outside_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  committee_id text NOT NULL,
  committee_name text NOT NULL,
  support_oppose text NOT NULL,
  amount numeric(16,2) NOT NULL,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ok_candidate_finance_outside_groups_year_check
    CHECK (election_year BETWEEN 2014 AND 2100),
  CONSTRAINT ok_candidate_finance_outside_groups_committee_id_check
    CHECK (btrim(committee_id) <> ''),
  CONSTRAINT ok_candidate_finance_outside_groups_committee_name_check
    CHECK (btrim(committee_name) <> ''),
  CONSTRAINT ok_candidate_finance_outside_groups_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT ok_candidate_finance_outside_groups_amount_check
    CHECK (amount >= 0),
  CONSTRAINT ok_candidate_finance_outside_groups_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT ok_candidate_finance_outside_groups_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.ok_candidate_finance_links(id, election_year)
    ON DELETE CASCADE,
  CONSTRAINT ok_candidate_finance_outside_groups_unique
    UNIQUE (link_id, election_year, committee_id, support_oppose)
);

CREATE INDEX IF NOT EXISTS ok_candidate_finance_outside_groups_lookup_idx
  ON public.ok_candidate_finance_outside_groups (
    link_id,
    election_year DESC,
    support_oppose,
    amount DESC
  );

DROP TRIGGER IF EXISTS ok_candidate_finance_outside_groups_set_updated_at
  ON public.ok_candidate_finance_outside_groups;
CREATE TRIGGER ok_candidate_finance_outside_groups_set_updated_at
BEFORE UPDATE ON public.ok_candidate_finance_outside_groups
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
