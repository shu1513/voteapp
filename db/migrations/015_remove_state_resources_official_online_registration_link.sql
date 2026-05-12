ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_official_online_registration_link_url;

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

UPDATE state_resources
SET voter_registration_url = 'https://vote.gov/register'
WHERE voter_registration_url IS DISTINCT FROM 'https://vote.gov/register';

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_voter_registration_url_fixed;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_voter_registration_url_fixed
  CHECK (voter_registration_url = 'https://vote.gov/register');

UPDATE state_resources
SET sources = sources - 'official_online_registration_link'
WHERE jsonb_typeof(sources) = 'object' AND sources ? 'official_online_registration_link';

ALTER TABLE state_resources
  DROP COLUMN IF EXISTS official_online_registration_link;

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
