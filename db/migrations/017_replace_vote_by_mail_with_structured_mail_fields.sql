ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS mail_voting_available boolean;

ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS mail_ballot_request_deadline_rule text;

ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS mail_ballot_return_deadline_rule text;

ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS mail_ballot_return_deadline_type text;

UPDATE state_resources
SET mail_voting_available = COALESCE(mail_voting_available, true);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'state_resources'
      AND column_name = 'vote_by_mail_info'
  ) THEN
    UPDATE state_resources
    SET
      mail_ballot_request_deadline_rule = CASE
        WHEN mail_voting_available = false THEN NULL
        ELSE mail_ballot_request_deadline_rule
      END,
      mail_ballot_return_deadline_rule = CASE
        WHEN mail_voting_available = false THEN NULL
        ELSE COALESCE(mail_ballot_return_deadline_rule, vote_by_mail_info)
      END,
      mail_ballot_return_deadline_type = CASE
        WHEN mail_voting_available = false THEN NULL
        WHEN mail_ballot_return_deadline_type IN ('postmarked_by', 'received_by') THEN mail_ballot_return_deadline_type
        WHEN COALESCE(mail_ballot_return_deadline_rule, vote_by_mail_info, '') ~* 'postmark' THEN 'postmarked_by'
        WHEN COALESCE(mail_ballot_return_deadline_rule, vote_by_mail_info, '') ~* '(receiv|arriv|deliver|delivery)' THEN 'received_by'
        ELSE NULL
      END;
  ELSE
    UPDATE state_resources
    SET
      mail_ballot_request_deadline_rule = CASE
        WHEN mail_voting_available = false THEN NULL
        ELSE mail_ballot_request_deadline_rule
      END,
      mail_ballot_return_deadline_rule = CASE
        WHEN mail_voting_available = false THEN NULL
        ELSE mail_ballot_return_deadline_rule
      END,
      mail_ballot_return_deadline_type = CASE
        WHEN mail_voting_available = false THEN NULL
        WHEN mail_ballot_return_deadline_type IN ('postmarked_by', 'received_by') THEN mail_ballot_return_deadline_type
        WHEN COALESCE(mail_ballot_return_deadline_rule, '') ~* 'postmark' THEN 'postmarked_by'
        WHEN COALESCE(mail_ballot_return_deadline_rule, '') ~* '(receiv|arriv|deliver|delivery)' THEN 'received_by'
        ELSE NULL
      END;
  END IF;
END
$$;

UPDATE state_resources
SET sources = jsonb_build_object(
  'polling_place_url', COALESCE(sources->'polling_place_url', sources->'voter_registration_url', '[]'::jsonb),
  'voter_registration_url', COALESCE(sources->'voter_registration_url', '[]'::jsonb),
  'mail_voting_available', COALESCE(sources->'vote_by_mail_info', sources->'voter_registration_url', '[]'::jsonb),
  'mail_ballot_request_deadline_rule', COALESCE(sources->'vote_by_mail_info', sources->'voter_registration_url', '[]'::jsonb),
  'mail_ballot_return_deadline_rule', COALESCE(sources->'vote_by_mail_info', sources->'voter_registration_url', '[]'::jsonb),
  'mail_ballot_return_deadline_type', COALESCE(sources->'vote_by_mail_info', sources->'voter_registration_url', '[]'::jsonb),
  'polling_hours', COALESCE(sources->'polling_hours', sources->'voter_registration_url', '[]'::jsonb),
  'id_requirements', COALESCE(sources->'id_requirements', sources->'voter_registration_url', '[]'::jsonb),
  'same_day_registration_available', COALESCE(sources->'same_day_registration_available', sources->'voter_registration_url', '[]'::jsonb),
  'online_registration_available', COALESCE(sources->'online_registration_available', sources->'voter_registration_url', '[]'::jsonb),
  'online_registration_deadline_rule', COALESCE(sources->'online_registration_deadline_rule', sources->'voter_registration_url', '[]'::jsonb)
);

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_vote_by_mail_info_text;

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_mail_ballot_request_deadline_rule_text;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_mail_ballot_request_deadline_rule_text
  CHECK (
    mail_ballot_request_deadline_rule IS NULL
    OR (btrim(mail_ballot_request_deadline_rule) <> '' AND char_length(mail_ballot_request_deadline_rule) <= 1000)
  );

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_mail_ballot_return_deadline_rule_text;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_mail_ballot_return_deadline_rule_text
  CHECK (
    mail_ballot_return_deadline_rule IS NULL
    OR (btrim(mail_ballot_return_deadline_rule) <> '' AND char_length(mail_ballot_return_deadline_rule) <= 1000)
  );

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_mail_ballot_return_deadline_type;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_mail_ballot_return_deadline_type
  CHECK (
    mail_ballot_return_deadline_type IS NULL
    OR mail_ballot_return_deadline_type IN ('postmarked_by', 'received_by')
  );

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_mail_voting_consistency;

ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_mail_voting_consistency
  CHECK (
    (
      mail_voting_available = true
      AND mail_ballot_return_deadline_rule IS NOT NULL
    )
    OR (
      mail_voting_available = false
      AND mail_ballot_request_deadline_rule IS NULL
      AND mail_ballot_return_deadline_rule IS NULL
      AND mail_ballot_return_deadline_type IS NULL
    )
  );

ALTER TABLE state_resources
  ALTER COLUMN mail_voting_available SET NOT NULL;

ALTER TABLE state_resources
  DROP COLUMN IF EXISTS vote_by_mail_info;

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
                    'voter_registration_url',
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
