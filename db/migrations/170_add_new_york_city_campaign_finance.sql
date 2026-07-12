BEGIN;

-- NYC offices use the Campaign Finance Board, not the NYSBOE provider.
INSERT INTO public.offices (scope, canonical_name, summary)
VALUES
  (
    'place',
    'Public Advocate',
    'Serves as a citywide ombudsman, investigates city services, advocates for residents, and performs legislative duties defined by the city charter.'
  ),
  (
    'place',
    'Comptroller',
    'Oversees municipal accounting, audits city agencies, reviews contracts, and monitors the city''s finances and pension funds.'
  ),
  (
    'county',
    'Borough President',
    'Represents a borough in city government, advocates for borough priorities, and exercises budget, land-use, and appointment powers defined by the city charter.'
  )
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT office.id, desired.scope, desired.alias_text, desired.normalized_alias
FROM (
  VALUES
    ('place', 'Public Advocate', 'Public Advocate', 'public advocate'),
    ('place', 'Comptroller', 'Comptroller', 'comptroller'),
    ('place', 'Comptroller', 'City Comptroller', 'city comptroller'),
    ('county', 'Borough President', 'Borough President', 'borough president')
) AS desired(scope, canonical_name, alias_text, normalized_alias)
JOIN public.offices AS office
  ON office.scope = desired.scope
 AND office.canonical_name = desired.canonical_name
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Existing seeded databases become usable immediately. Fresh installs receive
-- these same authoritative sets from db/seeds/office_research_areas_v1.sql.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT target.id, source_link.research_area_id
FROM public.offices AS target
JOIN public.offices AS source
  ON (
    (target.scope = 'place' AND target.canonical_name = 'Comptroller'
      AND source.scope = 'statewide' AND source.canonical_name = 'Comptroller')
    OR
    (target.scope = 'county' AND target.canonical_name = 'Borough President'
      AND source.scope = 'county' AND source.canonical_name = 'County Executive')
  )
JOIN public.office_research_areas AS source_link
  ON source_link.office_id = source.id
ON CONFLICT (office_id, research_area_id) DO NOTHING;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT public_advocate.id, area.id
FROM public.offices AS public_advocate
JOIN public.research_areas AS area
  ON area.slug = ANY (ARRAY[
    'anti_corruption',
    'civil_rights',
    'environment_and_public_health',
    'government_efficiency',
    'government_spending_reduction',
    'housing_affordability',
    'public_infrastructure',
    'public_safety_and_crime_control',
    'social_programs_and_welfare'
  ]::text[])
WHERE public_advocate.scope = 'place'
  AND public_advocate.canonical_name = 'Public Advocate'
ON CONFLICT (office_id, research_area_id) DO NOTHING;

CREATE TABLE public.nyc_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL,
  candidate_name_normalized text NOT NULL,
  office_code text NOT NULL,
  borough_code text,
  cfb_candidate_id text NOT NULL,
  cfb_candidate_name text NOT NULL,
  link_status text NOT NULL DEFAULT 'active',
  link_source text NOT NULL DEFAULT 'manual',
  source_url text,
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT nyc_candidate_finance_links_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT nyc_candidate_finance_links_candidate_name_check
    CHECK (btrim(candidate_name_normalized) <> ''),
  CONSTRAINT nyc_candidate_finance_links_office_code_check
    CHECK (office_code IN ('1', '2', '3', '4')),
  CONSTRAINT nyc_candidate_finance_links_borough_code_check
    CHECK (
      (office_code IN ('1', '2', '3') AND borough_code IS NULL)
      OR (office_code = '4' AND borough_code IN ('X', 'K', 'M', 'Q', 'S'))
    ),
  CONSTRAINT nyc_candidate_finance_links_cfb_candidate_id_check
    CHECK (btrim(cfb_candidate_id) <> ''),
  CONSTRAINT nyc_candidate_finance_links_cfb_candidate_name_check
    CHECK (btrim(cfb_candidate_name) <> ''),
  CONSTRAINT nyc_candidate_finance_links_status_check
    CHECK (link_status IN ('active', 'inactive')),
  CONSTRAINT nyc_candidate_finance_links_source_check
    CHECK (link_source IN ('manual', 'cfb_csv')),
  CONSTRAINT nyc_candidate_finance_links_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT nyc_candidate_finance_links_unique
    UNIQUE (candidate_id, election_id, cfb_candidate_id),
  CONSTRAINT nyc_candidate_finance_links_id_year_unique
    UNIQUE (id, election_year)
);

CREATE INDEX nyc_candidate_finance_links_election_candidate_idx
  ON public.nyc_candidate_finance_links (election_id, candidate_id);

CREATE INDEX nyc_candidate_finance_links_cfb_cycle_idx
  ON public.nyc_candidate_finance_links (cfb_candidate_id, election_year);

CREATE UNIQUE INDEX nyc_candidate_finance_links_active_candidate_election_idx
  ON public.nyc_candidate_finance_links (candidate_id, election_id)
  WHERE link_status = 'active';

CREATE TRIGGER nyc_candidate_finance_links_set_updated_at
BEFORE UPDATE ON public.nyc_candidate_finance_links
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.nyc_candidate_finance_summaries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  link_id uuid NOT NULL,
  election_year integer NOT NULL,
  private_contributions numeric(16,2),
  net_expenditures numeric(16,2),
  outstanding_bills numeric(16,2),
  public_funds numeric(16,2),
  source_url text,
  last_synced_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT nyc_candidate_finance_summaries_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT nyc_candidate_finance_summaries_amounts_check
    CHECK (
      (private_contributions IS NULL OR private_contributions >= 0)
      AND (net_expenditures IS NULL OR net_expenditures >= 0)
      AND (outstanding_bills IS NULL OR outstanding_bills >= 0)
      AND (public_funds IS NULL OR public_funds >= 0)
    ),
  CONSTRAINT nyc_candidate_finance_summaries_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT nyc_candidate_finance_summaries_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.nyc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT nyc_candidate_finance_summaries_unique
    UNIQUE (link_id, election_year)
);

CREATE INDEX nyc_candidate_finance_summaries_lookup_idx
  ON public.nyc_candidate_finance_summaries (link_id, election_year DESC);

CREATE TRIGGER nyc_candidate_finance_summaries_set_updated_at
BEFORE UPDATE ON public.nyc_candidate_finance_summaries
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE public.nyc_candidate_finance_direct_breakdowns (
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
  CONSTRAINT nyc_candidate_finance_direct_breakdowns_year_check
    CHECK (election_year BETWEEN 2000 AND 2100),
  CONSTRAINT nyc_candidate_finance_direct_breakdowns_type_check
    CHECK (category_type IN ('occupation', 'employer', 'industry', 'contribution_size')),
  CONSTRAINT nyc_candidate_finance_direct_breakdowns_name_check
    CHECK (btrim(category_name) <> ''),
  CONSTRAINT nyc_candidate_finance_direct_breakdowns_amount_check
    CHECK (amount >= 0),
  CONSTRAINT nyc_candidate_finance_direct_breakdowns_count_check
    CHECK (contributor_count IS NULL OR contributor_count >= 0),
  CONSTRAINT nyc_candidate_finance_direct_breakdowns_source_url_check
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT nyc_candidate_finance_direct_breakdowns_link_year_fk
    FOREIGN KEY (link_id, election_year)
    REFERENCES public.nyc_candidate_finance_links(id, election_year)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT nyc_candidate_finance_direct_breakdowns_unique
    UNIQUE (link_id, election_year, category_type, category_name)
);

CREATE INDEX nyc_candidate_finance_direct_breakdowns_lookup_idx
  ON public.nyc_candidate_finance_direct_breakdowns (
    link_id,
    election_year DESC,
    category_type,
    amount DESC
  );

CREATE TRIGGER nyc_candidate_finance_direct_breakdowns_set_updated_at
BEFORE UPDATE ON public.nyc_candidate_finance_direct_breakdowns
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
