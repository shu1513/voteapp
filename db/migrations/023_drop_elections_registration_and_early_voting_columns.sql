ALTER TABLE elections
  DROP CONSTRAINT IF EXISTS chk_early_voting_window;

ALTER TABLE elections
  DROP COLUMN IF EXISTS registration_deadline,
  DROP COLUMN IF EXISTS early_voting_start,
  DROP COLUMN IF EXISTS early_voting_end;
