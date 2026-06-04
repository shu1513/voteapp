BEGIN;

ALTER TABLE public.elections
  ADD COLUMN IF NOT EXISTS discovery_contest_family text;

ALTER TABLE public.elections
  DROP CONSTRAINT IF EXISTS chk_elections_discovery_contest_family;

ALTER TABLE public.elections
  ADD CONSTRAINT chk_elections_discovery_contest_family
  CHECK (
    discovery_contest_family IS NULL
    OR discovery_contest_family IN (
      'all',
      'non_judicial_office',
      'judicial_office',
      'ballot_measure',
      'us_senate'
    )
  );

COMMIT;
