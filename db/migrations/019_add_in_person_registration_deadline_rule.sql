ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS in_person_registration_deadline_rule text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_state_resources_in_person_registration_deadline_rule_text'
      AND conrelid = 'state_resources'::regclass
  ) THEN
    ALTER TABLE state_resources
      ADD CONSTRAINT chk_state_resources_in_person_registration_deadline_rule_text
      CHECK (
        in_person_registration_deadline_rule IS NULL
        OR (
          btrim(in_person_registration_deadline_rule) <> ''
          AND char_length(in_person_registration_deadline_rule) <= 1000
        )
      );
  END IF;
END
$$;

UPDATE state_resources
SET sources = jsonb_set(
  sources,
  '{in_person_registration_deadline_rule}',
  CASE
    WHEN jsonb_typeof(sources->'in_person_registration_deadline_rule') = 'array'
      AND jsonb_array_length(sources->'in_person_registration_deadline_rule') > 0
      THEN sources->'in_person_registration_deadline_rule'
    WHEN jsonb_typeof(sources->'online_registration_deadline_rule') = 'array'
      AND jsonb_array_length(sources->'online_registration_deadline_rule') > 0
      THEN sources->'online_registration_deadline_rule'
    WHEN jsonb_typeof(sources->'online_registration_available') = 'array'
      AND jsonb_array_length(sources->'online_registration_available') > 0
      THEN sources->'online_registration_available'
    ELSE '["https://vote.gov/register"]'::jsonb
  END,
  true
)
WHERE
  NOT (sources ? 'in_person_registration_deadline_rule')
  OR jsonb_typeof(sources->'in_person_registration_deadline_rule') <> 'array'
  OR jsonb_array_length(sources->'in_person_registration_deadline_rule') = 0;

UPDATE state_resources
SET in_person_registration_deadline_rule = COALESCE(
  NULLIF(btrim(online_registration_deadline_rule), ''),
  'See official state election office guidance for in-person registration deadlines.'
)
WHERE in_person_registration_deadline_rule IS NULL
  OR btrim(in_person_registration_deadline_rule) = '';

ALTER TABLE state_resources
  ALTER COLUMN in_person_registration_deadline_rule SET NOT NULL;

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_in_person_registration_deadline_rule_text;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_in_person_registration_deadline_rule_text
  CHECK (
    btrim(in_person_registration_deadline_rule) <> ''
    AND char_length(in_person_registration_deadline_rule) <= 1000
  );

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
