ALTER TABLE elections
  RENAME COLUMN title TO official_ballot_title;

ALTER TABLE elections
  DROP COLUMN IF EXISTS last_researched;
