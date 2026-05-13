ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS same_day_registration_available boolean;

UPDATE state_resources
SET same_day_registration_available = false
WHERE same_day_registration_available IS NULL;

ALTER TABLE state_resources
  ALTER COLUMN same_day_registration_available SET NOT NULL;

UPDATE state_resources
SET sources = jsonb_set(
  sources,
  '{same_day_registration_available}',
  CASE
    WHEN jsonb_typeof(sources->'same_day_registration_available') = 'array'
      AND jsonb_array_length(sources->'same_day_registration_available') > 0
      THEN sources->'same_day_registration_available'
    WHEN jsonb_typeof(sources->'online_registration_available') = 'array'
      AND jsonb_array_length(sources->'online_registration_available') > 0
      THEN sources->'online_registration_available'
    ELSE sources->'id_requirements'
  END,
  true
)
WHERE
  jsonb_typeof(sources) = 'object'
  AND (
    NOT (sources ? 'same_day_registration_available')
    OR jsonb_typeof(sources->'same_day_registration_available') <> 'array'
    OR jsonb_array_length(sources->'same_day_registration_available') = 0
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
                'same_day_registration_available',
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
                    'same_day_registration_available',
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
