BEGIN;

CREATE TABLE IF NOT EXISTS election_seed_urls (
  district_id uuid NOT NULL,
  contest_family text NOT NULL,
  url text NOT NULL,
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pk_election_seed_urls PRIMARY KEY (district_id, contest_family, url),
  CONSTRAINT fk_election_seed_urls_district
    FOREIGN KEY (district_id)
    REFERENCES districts(id)
    ON DELETE CASCADE,
  CONSTRAINT chk_election_seed_urls_contest_family
    CHECK (contest_family IN ('all', 'non_judicial_office', 'judicial_office', 'ballot_measure'))
);

CREATE INDEX IF NOT EXISTS idx_election_seed_urls_lookup
  ON election_seed_urls (district_id, contest_family, last_seen_at DESC);

DROP TRIGGER IF EXISTS trg_election_seed_urls_set_updated_at ON election_seed_urls;
CREATE TRIGGER trg_election_seed_urls_set_updated_at
BEFORE UPDATE ON election_seed_urls
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
