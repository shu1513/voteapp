BEGIN;

ALTER TABLE staging_items
  DROP CONSTRAINT IF EXISTS chk_staging_items_status;

ALTER TABLE staging_items
  ADD CONSTRAINT chk_staging_items_status
  CHECK (status IN ('pending', 'validated', 'rejected', 'written', 'failed', 'requeueing', 'no_results'));

WITH ranked AS (
  SELECT
    id,
    row_number() OVER (
      PARTITION BY district_id, official_ballot_title, election_date
      ORDER BY updated_at DESC, created_at DESC, id DESC
    ) AS rn
  FROM elections
)
DELETE FROM elections e
USING ranked r
WHERE e.id = r.id
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_elections_district_title_date
  ON elections (district_id, official_ballot_title, election_date);

COMMIT;
