BEGIN;

ALTER TABLE elections
  ADD COLUMN IF NOT EXISTS election_stage text;

UPDATE elections
SET election_stage = CASE
  WHEN official_ballot_title ~* '\mrunoff\M' THEN 'runoff'
  WHEN official_ballot_title ~* '\mspecial\M' THEN 'special'
  WHEN official_ballot_title ~* '\mprimary\M' THEN 'primary'
  WHEN official_ballot_title ~* '\mgeneral\M' THEN 'general'
  ELSE election_stage
END
WHERE election_stage IS NULL;

ALTER TABLE elections
  DROP CONSTRAINT IF EXISTS chk_elections_election_stage;

ALTER TABLE elections
  ADD CONSTRAINT chk_elections_election_stage
  CHECK (
    election_stage IS NULL
    OR election_stage IN ('primary', 'general', 'runoff', 'special')
  );

COMMIT;
