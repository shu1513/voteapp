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
                    'mail_voting_available',
                    'mail_ballot_request_deadline_rule',
                    'mail_ballot_return_deadline_rule',
                    'mail_ballot_return_deadline_type',
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

UPDATE state_resources
SET sources = sources - 'voter_registration_url'
WHERE jsonb_typeof(sources) = 'object'
  AND sources ? 'voter_registration_url';
