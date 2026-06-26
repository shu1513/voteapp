BEGIN;

CREATE TABLE IF NOT EXISTS public.ut_candidate_finance_supporting_committees (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  committee_name text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ut_candidate_finance_supporting_committees_year_check
    CHECK (election_year BETWEEN 1998 AND 2100),
  CONSTRAINT ut_candidate_finance_supporting_committees_name_check
    CHECK (btrim(committee_name) <> ''),
  CONSTRAINT ut_candidate_finance_supporting_committees_amount_check
    CHECK (amount >= 0),
  CONSTRAINT ut_candidate_finance_supporting_committees_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT ut_candidate_finance_supporting_committees_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT ut_candidate_finance_supporting_committees_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.ut_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT ut_candidate_finance_supporting_committees_unique
    UNIQUE (link_id, election_year, committee_name)
);

CREATE INDEX IF NOT EXISTS ut_candidate_finance_supporting_committees_lookup_idx
  ON public.ut_candidate_finance_supporting_committees (
    link_id,
    election_year DESC,
    amount DESC
  );

DROP TRIGGER IF EXISTS ut_candidate_finance_supporting_committees_set_updated_at
  ON public.ut_candidate_finance_supporting_committees;
CREATE TRIGGER ut_candidate_finance_supporting_committees_set_updated_at
BEFORE UPDATE ON public.ut_candidate_finance_supporting_committees
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.ut_candidate_finance_supporting_committee_industries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  supporting_committee_name text NOT NULL,
  industry_slug text NOT NULL,
  amount numeric(16,2) NOT NULL,
  contributor_count integer,
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ut_candidate_finance_supporting_committee_industries_year_check
    CHECK (election_year BETWEEN 1998 AND 2100),
  CONSTRAINT ut_candidate_finance_supporting_committee_industries_committee_check
    CHECK (btrim(supporting_committee_name) <> ''),
  CONSTRAINT ut_candidate_finance_supporting_committee_industries_slug_check
    CHECK (btrim(industry_slug) <> ''),
  CONSTRAINT ut_candidate_finance_supporting_committee_industries_amount_check
    CHECK (amount >= 0),
  CONSTRAINT ut_candidate_finance_supporting_committee_industries_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT ut_candidate_finance_supporting_committee_industries_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT ut_candidate_finance_supporting_committee_industries_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.ut_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT ut_candidate_finance_supporting_committee_industries_unique
    UNIQUE (link_id, election_year, supporting_committee_name, industry_slug)
);

CREATE INDEX IF NOT EXISTS ut_candidate_finance_supporting_committee_industries_lookup_idx
  ON public.ut_candidate_finance_supporting_committee_industries (
    link_id,
    election_year DESC,
    supporting_committee_name,
    amount DESC
  );

DROP TRIGGER IF EXISTS ut_candidate_finance_supporting_committee_industries_set_updated_at
  ON public.ut_candidate_finance_supporting_committee_industries;
CREATE TRIGGER ut_candidate_finance_supporting_committee_industries_set_updated_at
BEFORE UPDATE ON public.ut_candidate_finance_supporting_committee_industries
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
