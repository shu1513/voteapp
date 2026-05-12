UPDATE state_resources
SET online_registration_available = false
WHERE online_registration_available IS NULL;

-- Normalize existing inconsistent pairs before enforcing strict consistency:
-- 1) If online registration is marked unavailable, deadline rule must be null.
UPDATE state_resources
SET online_registration_deadline_rule = NULL
WHERE online_registration_available = false
  AND online_registration_deadline_rule IS NOT NULL;

-- 2) If online registration is marked available but deadline rule is missing,
--    prefer non-fabricated data and flip availability to false.
UPDATE state_resources
SET online_registration_available = false
WHERE online_registration_available = true
  AND online_registration_deadline_rule IS NULL;

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_online_registration_consistency;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_online_registration_consistency
  CHECK (
    (
      online_registration_available = true
      AND online_registration_deadline_rule IS NOT NULL
    )
    OR (
      online_registration_available = false
      AND online_registration_deadline_rule IS NULL
    )
  );

ALTER TABLE state_resources
  ALTER COLUMN online_registration_available SET NOT NULL;

UPDATE state_resources
SET voter_registration_url = 'https://vote.gov/register'
WHERE voter_registration_url IS DISTINCT FROM 'https://vote.gov/register';

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_voter_registration_url_fixed;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_voter_registration_url_fixed
  CHECK (voter_registration_url = 'https://vote.gov/register');
