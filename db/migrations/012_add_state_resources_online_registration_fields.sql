ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS online_registration_available boolean,
  ADD COLUMN IF NOT EXISTS online_registration_deadline_rule text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_state_resources_online_registration_deadline_rule_text'
      AND conrelid = 'state_resources'::regclass
  ) THEN
    ALTER TABLE state_resources
      ADD CONSTRAINT chk_state_resources_online_registration_deadline_rule_text
      CHECK (
        online_registration_deadline_rule IS NULL
        OR (btrim(online_registration_deadline_rule) <> '' AND char_length(online_registration_deadline_rule) <= 1000)
      );
  END IF;

  ALTER TABLE state_resources
    DROP CONSTRAINT IF EXISTS chk_state_resources_online_registration_consistency;

  ALTER TABLE state_resources
    ADD CONSTRAINT chk_state_resources_online_registration_consistency
    CHECK (
      (
        online_registration_available IS NULL
        AND online_registration_deadline_rule IS NULL
      )
      OR (
        online_registration_available = true
        AND online_registration_deadline_rule IS NOT NULL
      )
      OR (
        online_registration_available = false
        AND online_registration_deadline_rule IS NULL
      )
    );
END
$$;

UPDATE state_resources
SET sources = jsonb_set(
  jsonb_set(
    sources,
    '{online_registration_available}',
    CASE
      WHEN jsonb_typeof(sources->'online_registration_available') = 'array'
        AND jsonb_array_length(sources->'online_registration_available') > 0
        THEN sources->'online_registration_available'
      ELSE sources->'voter_registration_url'
    END,
    true
  ),
  '{online_registration_deadline_rule}',
  CASE
    WHEN jsonb_typeof(sources->'online_registration_deadline_rule') = 'array'
      AND jsonb_array_length(sources->'online_registration_deadline_rule') > 0
      THEN sources->'online_registration_deadline_rule'
    ELSE sources->'voter_registration_url'
  END,
  true
)
WHERE
  jsonb_typeof(sources) = 'object'
  AND (
    NOT (sources ? 'online_registration_available')
    OR NOT (sources ? 'online_registration_deadline_rule')
    OR jsonb_typeof(sources->'online_registration_available') <> 'array'
    OR jsonb_typeof(sources->'online_registration_deadline_rule') <> 'array'
    OR jsonb_array_length(sources->'online_registration_available') = 0
    OR jsonb_array_length(sources->'online_registration_deadline_rule') = 0
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
                'voter_registration_url',
                'vote_by_mail_info',
                'polling_hours',
                'id_requirements',
                'online_registration_available',
                'online_registration_deadline_rule'
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
                    'voter_registration_url',
                    'vote_by_mail_info',
                    'polling_hours',
                    'id_requirements',
                    'online_registration_available',
                    'online_registration_deadline_rule'
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
