CREATE OR REPLACE FUNCTION normalize_state_resource_source_bucket(bucket jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(
    (
      SELECT jsonb_agg(to_jsonb(url_text))
      FROM (
        SELECT DISTINCT btrim(
          CASE
            WHEN jsonb_typeof(item) = 'string' THEN item #>> '{}'
            WHEN jsonb_typeof(item) = 'object' AND item ? 'source_url' THEN item->>'source_url'
            ELSE ''
          END
        ) AS url_text
        FROM jsonb_array_elements(COALESCE(bucket, '[]'::jsonb)) AS item
      ) normalized
      WHERE url_text <> ''
        AND url_text ~ '^https?://'
    ),
    '[]'::jsonb
  );
$$;

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
                'id_requirements'
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
                OR jsonb_array_length(value) = 0
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(value) AS item
                    WHERE
                        NOT (
                          (jsonb_typeof(item) = 'string' AND btrim(item #>> '{}') <> '' AND (item #>> '{}') ~ '^https?://')
                          OR (
                            jsonb_typeof(item) = 'object'
                            AND item ? 'source_url'
                            AND jsonb_typeof(item->'source_url') = 'string'
                            AND btrim(item->>'source_url') <> ''
                            AND (item->>'source_url') ~ '^https?://'
                          )
                        )
                )
        );
$$;

WITH base AS (
  SELECT
    id,
    normalize_state_resource_source_bucket(sources->'voter_registration_url') AS reg_urls,
    normalize_state_resource_source_bucket(sources->'polling_place_url') AS polling_place_urls,
    normalize_state_resource_source_bucket(sources->'voter_registration_url') AS voter_registration_urls,
    normalize_state_resource_source_bucket(sources->'vote_by_mail_info') AS vote_by_mail_urls,
    normalize_state_resource_source_bucket(sources->'polling_hours') AS polling_hours_urls,
    normalize_state_resource_source_bucket(sources->'id_requirements') AS id_requirement_urls,
    normalize_state_resource_source_bucket(sources->'online_registration_available') AS online_available_urls,
    normalize_state_resource_source_bucket(sources->'online_registration_deadline_rule') AS online_deadline_urls
  FROM state_resources
), normalized AS (
  SELECT
    id,
    jsonb_build_object(
      'polling_place_url', CASE WHEN polling_place_urls = '[]'::jsonb THEN reg_urls ELSE polling_place_urls END,
      'voter_registration_url', CASE WHEN voter_registration_urls = '[]'::jsonb THEN reg_urls ELSE voter_registration_urls END,
      'vote_by_mail_info', CASE WHEN vote_by_mail_urls = '[]'::jsonb THEN reg_urls ELSE vote_by_mail_urls END,
      'polling_hours', CASE WHEN polling_hours_urls = '[]'::jsonb THEN reg_urls ELSE polling_hours_urls END,
      'id_requirements', CASE WHEN id_requirement_urls = '[]'::jsonb THEN reg_urls ELSE id_requirement_urls END,
      'online_registration_available', CASE WHEN online_available_urls = '[]'::jsonb THEN reg_urls ELSE online_available_urls END,
      'online_registration_deadline_rule', CASE WHEN online_deadline_urls = '[]'::jsonb THEN reg_urls ELSE online_deadline_urls END
    ) AS next_sources
  FROM base
)
UPDATE state_resources AS sr
SET sources = normalized.next_sources
FROM normalized
WHERE sr.id = normalized.id;

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

DROP FUNCTION IF EXISTS normalize_state_resource_source_bucket(jsonb);
