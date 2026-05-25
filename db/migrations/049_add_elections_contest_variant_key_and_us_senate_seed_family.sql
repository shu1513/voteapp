BEGIN;

ALTER TABLE public.elections
  ADD COLUMN IF NOT EXISTS contest_variant_key text;

UPDATE public.elections
SET contest_variant_key = ''
WHERE contest_variant_key IS NULL;

UPDATE public.elections
SET contest_variant_key = CASE
  WHEN election_stage = 'special'
    OR official_ballot_title ~* '\munexpired[[:space:]]+term\M'
    OR official_ballot_title ~* '\mspecial[[:space:]]+election\M'
    OR official_ballot_title ~* '\mvacancy\M'
    OR official_ballot_title ~* '\mremainder[[:space:]]+of([[:space:]]+the)?[[:space:]]+term\M'
  THEN 'special'
  ELSE 'regular'
END
WHERE race_type = 'office'
  AND (
    official_ballot_title ~* '\munited[[:space:]]+states[[:space:]]+senator\M'
    OR official_ballot_title ~* '\mu\.?[[:space:]]*s\.?[[:space:]]+senator\M'
    OR official_ballot_title ~* '\munited[[:space:]]+states[[:space:]]+senate\M'
    OR official_ballot_title ~* '\mu\.?[[:space:]]*s\.?[[:space:]]+senate\M'
  );

ALTER TABLE public.elections
  ALTER COLUMN contest_variant_key SET DEFAULT '',
  ALTER COLUMN contest_variant_key SET NOT NULL;

DROP INDEX IF EXISTS uq_elections_district_title_key_date;

CREATE UNIQUE INDEX IF NOT EXISTS uq_elections_district_title_key_date_variant
  ON public.elections (
    district_id,
    official_ballot_title_key,
    election_date,
    contest_variant_key
  );

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

