BEGIN;

ALTER TABLE public.election_seed_urls
  DROP CONSTRAINT IF EXISTS chk_election_seed_urls_contest_family;

ALTER TABLE public.election_seed_urls
  ADD CONSTRAINT chk_election_seed_urls_contest_family
  CHECK (
    contest_family IN (
      'all',
      'non_judicial_office',
      'judicial_office',
      'ballot_measure',
      'us_senate'
    )
  );

COMMIT;
