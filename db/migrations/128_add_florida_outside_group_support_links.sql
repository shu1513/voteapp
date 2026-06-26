BEGIN;

CREATE TABLE IF NOT EXISTS public.fl_candidate_finance_outside_group_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_election_id uuid NOT NULL REFERENCES public.candidate_elections(id) ON DELETE CASCADE,
  committee_id text,
  committee_name text NOT NULL,
  support_oppose text NOT NULL,
  confidence text NOT NULL DEFAULT 'high',
  amount numeric(16,2),
  evidence_url text,
  evidence_note text,
  link_source text NOT NULL DEFAULT 'manual',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fl_cff_outside_group_links_committee_id_check
    CHECK (committee_id IS NULL OR btrim(committee_id) <> ''),
  CONSTRAINT fl_cff_outside_group_links_committee_name_check
    CHECK (btrim(committee_name) <> ''),
  CONSTRAINT fl_cff_outside_group_links_support_oppose_check
    CHECK (support_oppose IN ('support', 'oppose')),
  CONSTRAINT fl_cff_outside_group_links_confidence_check
    CHECK (confidence IN ('high', 'medium', 'low')),
  CONSTRAINT fl_cff_outside_group_links_amount_check
    CHECK (amount IS NULL OR amount >= 0),
  CONSTRAINT fl_cff_outside_group_links_evidence_url_check
    CHECK (evidence_url IS NULL OR btrim(evidence_url) <> ''),
  CONSTRAINT fl_cff_outside_group_links_evidence_note_check
    CHECK (evidence_note IS NULL OR btrim(evidence_note) <> ''),
  CONSTRAINT fl_cff_outside_group_links_source_check
    CHECK (link_source IN ('manual', 'name_heuristic', 'independent_expenditure')),
  CONSTRAINT fl_cff_outside_group_links_unique
    UNIQUE (candidate_election_id, committee_name, support_oppose, link_source)
);

CREATE INDEX IF NOT EXISTS fl_cff_outside_group_links_candidate_election_idx
  ON public.fl_candidate_finance_outside_group_links (
    candidate_election_id,
    support_oppose,
    confidence,
    committee_name
  );

DROP TRIGGER IF EXISTS fl_cff_outside_group_links_set_updated_at
  ON public.fl_candidate_finance_outside_group_links;
CREATE TRIGGER fl_cff_outside_group_links_set_updated_at
BEFORE UPDATE ON public.fl_candidate_finance_outside_group_links
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
