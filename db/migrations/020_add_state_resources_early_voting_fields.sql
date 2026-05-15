ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS early_voting_available boolean,
  ADD COLUMN IF NOT EXISTS early_voting_start_date_rule text,
  ADD COLUMN IF NOT EXISTS early_voting_end_date_rule text;

UPDATE state_resources
SET early_voting_available = false
WHERE early_voting_available IS NULL;

UPDATE state_resources
SET early_voting_start_date_rule = NULL
WHERE early_voting_available = false
  AND early_voting_start_date_rule IS NOT NULL;

UPDATE state_resources
SET early_voting_end_date_rule = NULL
WHERE early_voting_available = false
  AND early_voting_end_date_rule IS NOT NULL;

UPDATE state_resources
SET early_voting_available = false,
    early_voting_start_date_rule = NULL,
    early_voting_end_date_rule = NULL
WHERE early_voting_available = true
  AND (early_voting_start_date_rule IS NULL OR early_voting_end_date_rule IS NULL);

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_early_voting_start_date_rule_text;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_early_voting_start_date_rule_text
  CHECK (
    early_voting_start_date_rule IS NULL
    OR (
      btrim(early_voting_start_date_rule) <> ''
      AND char_length(early_voting_start_date_rule) <= 1000
    )
  );

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_early_voting_end_date_rule_text;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_early_voting_end_date_rule_text
  CHECK (
    early_voting_end_date_rule IS NULL
    OR (
      btrim(early_voting_end_date_rule) <> ''
      AND char_length(early_voting_end_date_rule) <= 1000
    )
  );

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_early_voting_consistency;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_early_voting_consistency
  CHECK (
    (
      early_voting_available = true
      AND early_voting_start_date_rule IS NOT NULL
      AND early_voting_end_date_rule IS NOT NULL
    )
    OR (
      early_voting_available = false
      AND early_voting_start_date_rule IS NULL
      AND early_voting_end_date_rule IS NULL
    )
  );

ALTER TABLE state_resources
  ALTER COLUMN early_voting_available SET NOT NULL;

UPDATE state_resources
SET sources = jsonb_set(
  jsonb_set(
    jsonb_set(
      sources,
      '{early_voting_available}',
      CASE
        WHEN jsonb_typeof(sources->'early_voting_available') = 'array'
          AND jsonb_array_length(sources->'early_voting_available') > 0
          THEN sources->'early_voting_available'
        WHEN jsonb_typeof(sources->'same_day_registration_available') = 'array'
          AND jsonb_array_length(sources->'same_day_registration_available') > 0
          THEN sources->'same_day_registration_available'
        ELSE '["https://www.ncsl.org/elections-and-campaigns/same-day-voter-registration"]'::jsonb
      END,
      true
    ),
    '{early_voting_start_date_rule}',
    CASE
      WHEN jsonb_typeof(sources->'early_voting_start_date_rule') = 'array'
        AND jsonb_array_length(sources->'early_voting_start_date_rule') > 0
        THEN sources->'early_voting_start_date_rule'
      WHEN jsonb_typeof(sources->'mail_ballot_request_deadline_rule') = 'array'
        AND jsonb_array_length(sources->'mail_ballot_request_deadline_rule') > 0
        THEN sources->'mail_ballot_request_deadline_rule'
      ELSE '["https://vote.gov/register"]'::jsonb
    END,
    true
  ),
  '{early_voting_end_date_rule}',
  CASE
    WHEN jsonb_typeof(sources->'early_voting_end_date_rule') = 'array'
      AND jsonb_array_length(sources->'early_voting_end_date_rule') > 0
      THEN sources->'early_voting_end_date_rule'
    WHEN jsonb_typeof(sources->'mail_ballot_return_deadline_rule') = 'array'
      AND jsonb_array_length(sources->'mail_ballot_return_deadline_rule') > 0
      THEN sources->'mail_ballot_return_deadline_rule'
    ELSE '["https://vote.gov/register"]'::jsonb
  END,
  true
)
WHERE
  NOT (sources ? 'early_voting_available')
  OR jsonb_typeof(sources->'early_voting_available') <> 'array'
  OR jsonb_array_length(sources->'early_voting_available') = 0
  OR NOT (sources ? 'early_voting_start_date_rule')
  OR jsonb_typeof(sources->'early_voting_start_date_rule') <> 'array'
  OR jsonb_array_length(sources->'early_voting_start_date_rule') = 0
  OR NOT (sources ? 'early_voting_end_date_rule')
  OR jsonb_typeof(sources->'early_voting_end_date_rule') <> 'array'
  OR jsonb_array_length(sources->'early_voting_end_date_rule') = 0;

CREATE OR REPLACE FUNCTION is_valid_state_resource_sources(data jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT
        data IS NOT NULL
        AND jsonb_typeof(data) = 'object'
        AND NOT EXISTS (
            SELECT 1
            FROM unnest(ARRAY[
                'polling_place_url',
                'mail_voting_available',
                'mail_ballot_request_deadline_rule',
                'mail_ballot_return_deadline_rule',
                'mail_ballot_return_deadline_type',
                'early_voting_available',
                'early_voting_start_date_rule',
                'early_voting_end_date_rule',
                'polling_hours',
                'id_requirements',
                'same_day_registration_available',
                'online_registration_available',
                'online_registration_deadline_rule',
                'in_person_registration_deadline_rule'
            ]) AS required_key
            WHERE NOT (
                data ? required_key
                AND jsonb_typeof(data->required_key) = 'array'
                AND jsonb_array_length(data->required_key) > 0
            )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM jsonb_each(data) AS e(key, value)
            WHERE
                key NOT IN (
                    'polling_place_url',
                    'mail_voting_available',
                    'mail_ballot_request_deadline_rule',
                    'mail_ballot_return_deadline_rule',
                    'mail_ballot_return_deadline_type',
                    'early_voting_available',
                    'early_voting_start_date_rule',
                    'early_voting_end_date_rule',
                    'polling_hours',
                    'id_requirements',
                    'same_day_registration_available',
                    'online_registration_available',
                    'online_registration_deadline_rule',
                    'in_person_registration_deadline_rule'
                )
                OR jsonb_typeof(value) <> 'array'
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(value) AS item
                    WHERE
                        jsonb_typeof(item) <> 'string'
                        OR btrim(item #>> '{}') = ''
                        OR (item #>> '{}') !~ '^https?://'
                )
        );
$$;
