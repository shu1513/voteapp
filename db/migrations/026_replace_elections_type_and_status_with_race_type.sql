BEGIN;

ALTER TABLE elections
  DROP CONSTRAINT IF EXISTS chk_election_type;

ALTER TABLE elections
  DROP CONSTRAINT IF EXISTS chk_results_status;

ALTER TABLE elections
  ADD COLUMN IF NOT EXISTS race_type text;

UPDATE elections
SET race_type = CASE
  WHEN official_ballot_title ~* '(proposition|measure|amendment|referendum|initiative|bond|question)'
    THEN 'ballot_measure'
  ELSE 'office'
END
WHERE race_type IS NULL;

ALTER TABLE elections
  ALTER COLUMN race_type SET NOT NULL;

ALTER TABLE elections
  DROP CONSTRAINT IF EXISTS chk_elections_race_type;

ALTER TABLE elections
  ADD CONSTRAINT chk_elections_race_type
  CHECK (race_type IN ('office', 'ballot_measure'));

ALTER TABLE elections
  DROP COLUMN IF EXISTS election_type;

ALTER TABLE elections
  DROP COLUMN IF EXISTS results_status;

COMMIT;
