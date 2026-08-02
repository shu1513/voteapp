-- Adds a dedicated official mail/absentee-ballot request destination to state_resources.
-- mail_ballot_request_type distinguishes how a voter requests a mail ballot:
--   online_portal  -> official online request portal
--   form           -> official application form / PDF
--   instructions   -> official instructions page (requests go through a local office)
--   not_required   -> automatic vote-by-mail jurisdiction; URL is an official explanatory page
ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS mail_ballot_request_url text,
  ADD COLUMN IF NOT EXISTS mail_ballot_request_type text;

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_mail_ballot_request_url_format;
ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_mail_ballot_request_url_format
  CHECK (
    mail_ballot_request_url IS NULL
    OR (
      btrim(mail_ballot_request_url) <> ''
      AND mail_ballot_request_url ~ '^https?://'
      AND char_length(mail_ballot_request_url) <= 2048
    )
  );

ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_mail_ballot_request_type;
ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_mail_ballot_request_type
  CHECK (
    mail_ballot_request_type IS NULL
    OR mail_ballot_request_type IN ('online_portal', 'form', 'instructions', 'not_required')
  );

-- Mail-voting consistency now covers the request destination:
-- available -> return rule + request URL + request type all present;
-- unavailable -> every mail-request/return field is null.
ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_mail_voting_consistency;
ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_mail_voting_consistency
  CHECK (
    (
      mail_voting_available = true
      AND mail_ballot_return_deadline_rule IS NOT NULL
      AND mail_ballot_request_url IS NOT NULL
      AND mail_ballot_request_type IS NOT NULL
    )
    OR (
      mail_voting_available = false
      AND mail_ballot_request_deadline_rule IS NULL
      AND mail_ballot_return_deadline_rule IS NULL
      AND mail_ballot_return_deadline_type IS NULL
      AND mail_ballot_request_url IS NULL
      AND mail_ballot_request_type IS NULL
    )
  );

-- Automatic vote-by-mail: there is no request, so there is no request deadline.
ALTER TABLE state_resources
  DROP CONSTRAINT IF EXISTS chk_state_resources_mail_request_not_required_consistency;
ALTER TABLE state_resources
  ADD CONSTRAINT chk_state_resources_mail_request_not_required_consistency
  CHECK (
    mail_ballot_request_type IS DISTINCT FROM 'not_required'
    OR mail_ballot_request_deadline_rule IS NULL
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
                'mail_ballot_request_url',
                'mail_ballot_request_type',
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
                    'mail_ballot_request_url',
                    'mail_ballot_request_type',
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
