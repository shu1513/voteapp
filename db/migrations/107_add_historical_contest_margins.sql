BEGIN;

CREATE TABLE IF NOT EXISTS public.historical_contest_margins (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source text NOT NULL,
  source_url text,
  election_year integer NOT NULL,
  state text NOT NULL,
  state_fips text NOT NULL,
  office_type text NOT NULL,
  district_type text NOT NULL,
  district_key text NOT NULL,
  mit_office text NOT NULL,
  mit_district text NOT NULL,
  winner_party text,
  runner_up_party text,
  winner_votes bigint,
  runner_up_votes bigint,
  total_votes bigint NOT NULL,
  margin_percent numeric(6,2) NOT NULL,
  competitiveness_label text NOT NULL,
  stale_after_redistricting boolean NOT NULL DEFAULT false,
  imported_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_historical_contest_margins_source_year_state_office_district
    UNIQUE (source, election_year, state, office_type, district_type, district_key),
  CONSTRAINT chk_historical_contest_margins_source_nonempty
    CHECK (btrim(source) <> ''),
  CONSTRAINT chk_historical_contest_margins_source_url_nonempty
    CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  CONSTRAINT chk_historical_contest_margins_election_year
    CHECK (election_year BETWEEN 1800 AND 2100),
  CONSTRAINT chk_historical_contest_margins_state
    CHECK (state ~ '^[A-Z]{2}$'),
  CONSTRAINT chk_historical_contest_margins_state_fips
    CHECK (state_fips ~ '^[0-9]{2}$'),
  CONSTRAINT chk_historical_contest_margins_office_type
    CHECK (
      office_type IN (
        'US_PRESIDENT',
        'US_SENATE',
        'US_HOUSE',
        'GOVERNOR',
        'STATE_SENATE',
        'STATE_HOUSE'
      )
    ),
  CONSTRAINT chk_historical_contest_margins_district_type
    CHECK (district_type IN ('statewide', 'us_house', 'state_upper', 'state_lower')),
  CONSTRAINT chk_historical_contest_margins_district_key_nonempty
    CHECK (btrim(district_key) <> ''),
  CONSTRAINT chk_historical_contest_margins_mit_office_nonempty
    CHECK (btrim(mit_office) <> ''),
  CONSTRAINT chk_historical_contest_margins_mit_district_nonempty
    CHECK (btrim(mit_district) <> ''),
  CONSTRAINT chk_historical_contest_margins_winner_party_nonempty
    CHECK (winner_party IS NULL OR btrim(winner_party) <> ''),
  CONSTRAINT chk_historical_contest_margins_runner_up_party_nonempty
    CHECK (runner_up_party IS NULL OR btrim(runner_up_party) <> ''),
  CONSTRAINT chk_historical_contest_margins_winner_votes
    CHECK (winner_votes IS NULL OR winner_votes >= 0),
  CONSTRAINT chk_historical_contest_margins_runner_up_votes
    CHECK (runner_up_votes IS NULL OR runner_up_votes >= 0),
  CONSTRAINT chk_historical_contest_margins_total_votes
    CHECK (total_votes >= 0),
  CONSTRAINT chk_historical_contest_margins_margin_percent
    CHECK (margin_percent >= 0 AND margin_percent <= 100),
  CONSTRAINT chk_historical_contest_margins_competitiveness_label
    CHECK (
      competitiveness_label IN (
        'toss_up',
        'very_competitive',
        'competitive',
        'somewhat_competitive',
        'safe'
      )
    ),
  CONSTRAINT chk_historical_contest_margins_vote_order
    CHECK (
      winner_votes IS NULL
      OR runner_up_votes IS NULL
      OR winner_votes >= runner_up_votes
    ),
  CONSTRAINT chk_historical_contest_margins_winner_votes_le_total
    CHECK (winner_votes IS NULL OR winner_votes <= total_votes),
  CONSTRAINT chk_historical_contest_margins_runner_up_votes_le_total
    CHECK (runner_up_votes IS NULL OR runner_up_votes <= total_votes),
  CONSTRAINT chk_historical_contest_margins_top_two_votes_le_total
    CHECK (
      winner_votes IS NULL
      OR runner_up_votes IS NULL
      OR winner_votes + runner_up_votes <= total_votes
    )
);

CREATE INDEX IF NOT EXISTS idx_historical_contest_margins_lookup
  ON public.historical_contest_margins (
    state,
    office_type,
    district_type,
    district_key,
    election_year DESC
  );

CREATE INDEX IF NOT EXISTS idx_historical_contest_margins_source_year
  ON public.historical_contest_margins (source, election_year);

CREATE INDEX IF NOT EXISTS idx_historical_contest_margins_label
  ON public.historical_contest_margins (competitiveness_label);

DROP TRIGGER IF EXISTS trg_historical_contest_margins_set_updated_at
  ON public.historical_contest_margins;
CREATE TRIGGER trg_historical_contest_margins_set_updated_at
BEFORE UPDATE ON public.historical_contest_margins
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
