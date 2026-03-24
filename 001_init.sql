BEGIN;

CREATE EXTENSION IF NOT EXISTS citext;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION is_valid_notable_entities(data jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT
        data IS NULL
        OR (
            jsonb_typeof(data) = 'array'
            AND NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements(data) AS item
                WHERE
                    jsonb_typeof(item) <> 'object'
                    OR NOT (item ? 'name' AND item ? 'type' AND item ? 'source_url' AND item ? 'source_name')
                    OR jsonb_typeof(item->'name') <> 'string'
                    OR jsonb_typeof(item->'type') <> 'string'
                    OR jsonb_typeof(item->'source_url') <> 'string'
                    OR jsonb_typeof(item->'source_name') <> 'string'
                    OR btrim(item->>'name') = ''
                    OR btrim(item->>'source_url') = ''
                    OR btrim(item->>'source_name') = ''
                    OR (item->>'type') NOT IN ('organization', 'individual')
            )
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
                    'id_requirements'
                )
                OR jsonb_typeof(value) <> 'array'
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(value) AS item
                    WHERE
                        jsonb_typeof(item) <> 'object'
                        OR NOT (item ? 'source_url' AND item ? 'source_name')
                        OR jsonb_typeof(item->'source_url') <> 'string'
                        OR jsonb_typeof(item->'source_name') <> 'string'
                        OR btrim(item->>'source_url') = ''
                        OR btrim(item->>'source_name') = ''
                )
        );
$$;

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name text NOT NULL,
    email citext NOT NULL,
    password_hash text NOT NULL,
    email_verified boolean NOT NULL DEFAULT false,
    email_digest boolean NOT NULL DEFAULT true,
    email_election_reminders boolean NOT NULL DEFAULT true,
    email_new_election_alerts boolean NOT NULL DEFAULT true,
    last_logged_in timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz
);

CREATE UNIQUE INDEX uq_users_email_active ON users (email) WHERE deleted_at IS NULL;
CREATE INDEX idx_users_deleted_at ON users (deleted_at);

CREATE TABLE districts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    fips_code text NOT NULL UNIQUE,
    name text NOT NULL,
    state text NOT NULL,
    state_fips text NOT NULL,
    district_type text NOT NULL,
    population integer NOT NULL CHECK (population >= 0),
    registered_voters integer CHECK (registered_voters IS NULL OR registered_voters >= 0),
    vote_power_score numeric(5,2) CHECK (vote_power_score IS NULL OR (vote_power_score >= 0 AND vote_power_score <= 100)),
    last_researched timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_district_type
        CHECK (district_type IN ('us_senate', 'us_house', 'state_upper', 'state_lower', 'county', 'city', 'school')),
    CONSTRAINT uq_districts_id_district_type
        UNIQUE (id, district_type)
);

CREATE INDEX idx_districts_state ON districts (state);
CREATE INDEX idx_districts_district_type ON districts (district_type);

CREATE TABLE user_districts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL,
    district_id uuid NOT NULL,
    district_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_user_districts_user
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_user_districts_district
        FOREIGN KEY (district_id, district_type)
        REFERENCES districts(id, district_type)
        ON DELETE CASCADE,
    CONSTRAINT uq_user_districts_user_id_district_id
        UNIQUE (user_id, district_id),
    CONSTRAINT chk_user_districts_type
        CHECK (district_type IN ('us_senate', 'us_house', 'state_upper', 'state_lower', 'county', 'city', 'school'))
);

CREATE INDEX idx_user_districts_user_id ON user_districts (user_id);
CREATE INDEX idx_user_districts_district_id ON user_districts (district_id);
CREATE INDEX idx_user_districts_district_type ON user_districts (district_type);

CREATE TABLE staging_items (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    ingest_key text NOT NULL UNIQUE,
    item_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    reason text,
    run_id text,
    model text,
    prompt_version text,
    created_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    written_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_staging_items_type
        CHECK (item_type IN ('district', 'candidate', 'election', 'proposition', 'candidate_record', 'state_resources')),
    CONSTRAINT chk_staging_items_status
        CHECK (status IN ('pending', 'validated', 'rejected', 'written', 'failed')),
    CONSTRAINT chk_staging_items_payload_json
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT chk_staging_items_reason_for_failures
        CHECK (
            (status IN ('rejected', 'failed') AND reason IS NOT NULL)
            OR status NOT IN ('rejected', 'failed')
        )
);

CREATE INDEX idx_staging_items_status ON staging_items (status);
CREATE INDEX idx_staging_items_item_type ON staging_items (item_type);
CREATE INDEX idx_staging_items_created_at ON staging_items (created_at);
CREATE INDEX idx_staging_items_run_id ON staging_items (run_id);

CREATE TABLE elections (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    district_id uuid NOT NULL,
    title text NOT NULL,
    description text,
    election_type text NOT NULL,
    election_date date NOT NULL,
    registration_deadline date,
    early_voting_start date,
    early_voting_end date,
    results_status text,
    last_researched timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_elections_district
        FOREIGN KEY (district_id)
        REFERENCES districts(id)
        ON DELETE RESTRICT,
    CONSTRAINT chk_election_type
        CHECK (election_type IN ('general', 'primary', 'special', 'runoff')),
    CONSTRAINT chk_results_status
        CHECK (results_status IS NULL OR results_status IN ('preliminary', 'updated', 'final')),
    CONSTRAINT chk_early_voting_window
        CHECK (
            early_voting_start IS NULL
            OR early_voting_end IS NULL
            OR early_voting_end >= early_voting_start
        )
);

CREATE INDEX idx_elections_district_id ON elections (district_id);
CREATE INDEX idx_elections_election_date ON elections (election_date);

CREATE TABLE candidates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name text NOT NULL,
    last_name text NOT NULL,
    date_of_birth date,
    party text NOT NULL,
    summary text,
    photo_url text,
    twitter_handle text,
    linkedin_url text,
    fec_ids jsonb,
    state_filing_ids jsonb,
    current_office text,
    state text NOT NULL,
    city text,
    merged_into_candidate_id uuid,
    last_researched timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    CONSTRAINT fk_candidates_merged_into
        FOREIGN KEY (merged_into_candidate_id)
        REFERENCES candidates(id)
        ON DELETE RESTRICT,
    CONSTRAINT chk_candidates_not_self_merged
        CHECK (merged_into_candidate_id IS NULL OR merged_into_candidate_id <> id),
    CONSTRAINT chk_candidates_merged_implies_deleted
        CHECK (merged_into_candidate_id IS NULL OR deleted_at IS NOT NULL),
    CONSTRAINT chk_fec_ids_json
        CHECK (fec_ids IS NULL OR jsonb_typeof(fec_ids) = 'array'),
    CONSTRAINT chk_state_filing_ids_json
        CHECK (state_filing_ids IS NULL OR jsonb_typeof(state_filing_ids) = 'array')
);

CREATE INDEX idx_candidates_last_name ON candidates (last_name);
CREATE INDEX idx_candidates_state ON candidates (state);
CREATE INDEX idx_candidates_deleted_at ON candidates (deleted_at);
CREATE INDEX idx_candidates_merged_into_candidate_id ON candidates (merged_into_candidate_id);

CREATE TABLE candidate_elections (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id uuid NOT NULL,
    election_id uuid NOT NULL,
    is_incumbent boolean NOT NULL DEFAULT false,
    status text NOT NULL DEFAULT 'declared',
    votes_received integer CHECK (votes_received IS NULL OR votes_received >= 0),
    vote_percentage numeric(5,2) CHECK (vote_percentage IS NULL OR (vote_percentage >= 0 AND vote_percentage <= 100)),
    total_raised numeric(14,2) CHECK (total_raised IS NULL OR total_raised >= 0),
    total_spent numeric(14,2) CHECK (total_spent IS NULL OR total_spent >= 0),
    cash_on_hand numeric(14,2) CHECK (cash_on_hand IS NULL OR cash_on_hand >= 0),
    small_donor_percentage numeric(5,2) CHECK (small_donor_percentage IS NULL OR (small_donor_percentage >= 0 AND small_donor_percentage <= 100)),
    top_donors jsonb,
    fec_filing_url text,
    finance_sources jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_candidate_elections_candidate
        FOREIGN KEY (candidate_id)
        REFERENCES candidates(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_candidate_elections_election
        FOREIGN KEY (election_id)
        REFERENCES elections(id)
        ON DELETE CASCADE,
    CONSTRAINT uq_candidate_elections_candidate_id_election_id
        UNIQUE (candidate_id, election_id),
    CONSTRAINT chk_candidate_elections_status
        CHECK (status IN ('declared', 'withdrawn', 'won', 'lost')),
    CONSTRAINT chk_top_donors_json
        CHECK (top_donors IS NULL OR jsonb_typeof(top_donors) = 'array'),
    CONSTRAINT chk_finance_sources_json
        CHECK (finance_sources IS NULL OR jsonb_typeof(finance_sources) = 'array')
);

CREATE INDEX idx_candidate_elections_candidate_id ON candidate_elections (candidate_id);
CREATE INDEX idx_candidate_elections_election_id ON candidate_elections (election_id);

CREATE TABLE candidate_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id uuid NOT NULL,
    record_type text NOT NULL,
    title text NOT NULL,
    description text NOT NULL,
    source_url text NOT NULL,
    source_name text NOT NULL,
    event_date date NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_candidate_records_candidate
        FOREIGN KEY (candidate_id)
        REFERENCES candidates(id)
        ON DELETE CASCADE,
    CONSTRAINT chk_candidate_records_type
        CHECK (record_type IN ('vote', 'bill_sponsored', 'bill_cosponsored', 'action', 'controversy', 'attendance', 'financial'))
);

CREATE INDEX idx_candidate_records_candidate_id ON candidate_records (candidate_id);
CREATE INDEX idx_candidate_records_event_date ON candidate_records (event_date);

CREATE TABLE propositions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    district_id uuid NOT NULL,
    election_id uuid NOT NULL,
    title text NOT NULL,
    summary text NOT NULL,
    what_yes_means text NOT NULL,
    what_no_means text NOT NULL,
    result text,
    yes_percentage numeric(5,2) CHECK (yes_percentage IS NULL OR (yes_percentage >= 0 AND yes_percentage <= 100)),
    no_percentage numeric(5,2) CHECK (no_percentage IS NULL OR (no_percentage >= 0 AND no_percentage <= 100)),
    source_url text NOT NULL,
    notable_supporters jsonb,
    notable_opponents jsonb,
    last_researched timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_propositions_district
        FOREIGN KEY (district_id)
        REFERENCES districts(id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_propositions_election
        FOREIGN KEY (election_id)
        REFERENCES elections(id)
        ON DELETE RESTRICT,
    CONSTRAINT chk_propositions_result
        CHECK (result IS NULL OR result IN ('passed', 'failed')),
    CONSTRAINT chk_propositions_notable_supporters_json
        CHECK (is_valid_notable_entities(notable_supporters)),
    CONSTRAINT chk_propositions_notable_opponents_json
        CHECK (is_valid_notable_entities(notable_opponents))
);

CREATE INDEX idx_propositions_election_id ON propositions (election_id);
CREATE INDEX idx_propositions_district_id ON propositions (district_id);

CREATE TABLE user_candidate_follows (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    notify_elections boolean NOT NULL DEFAULT true,
    notify_updates boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_user_candidate_follows_user
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_user_candidate_follows_candidate
        FOREIGN KEY (candidate_id)
        REFERENCES candidates(id)
        ON DELETE CASCADE,
    CONSTRAINT uq_user_candidate_follows_user_id_candidate_id
        UNIQUE (user_id, candidate_id)
);

CREATE INDEX idx_user_candidate_follows_user_id ON user_candidate_follows (user_id);
CREATE INDEX idx_user_candidate_follows_candidate_id ON user_candidate_follows (candidate_id);

CREATE TABLE user_election_follows (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL,
    election_id uuid NOT NULL,
    reminder_preferences jsonb NOT NULL DEFAULT '["registration", "early_voting", "one_week", "three_days", "election_day"]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_user_election_follows_user
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_user_election_follows_election
        FOREIGN KEY (election_id)
        REFERENCES elections(id)
        ON DELETE CASCADE,
    CONSTRAINT uq_user_election_follows_user_id_election_id
        UNIQUE (user_id, election_id),
    CONSTRAINT chk_user_election_follows_preferences_json
        CHECK (jsonb_typeof(reminder_preferences) = 'array')
);

CREATE INDEX idx_user_election_follows_user_id ON user_election_follows (user_id);
CREATE INDEX idx_user_election_follows_election_id ON user_election_follows (election_id);

CREATE TABLE state_resources (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    state_fips text NOT NULL UNIQUE,
    state_abbreviation text NOT NULL UNIQUE,
    state_name text NOT NULL UNIQUE,
    polling_place_url text NOT NULL,
    voter_registration_url text NOT NULL,
    vote_by_mail_info text NOT NULL,
    polling_hours text NOT NULL,
    id_requirements text NOT NULL,
    sources jsonb NOT NULL,
    CONSTRAINT chk_state_fips_format CHECK (state_fips ~ '^[0-9]{2}$'),
    CONSTRAINT chk_state_abbreviation_format CHECK (state_abbreviation ~ '^[A-Z]{2}$'),
    CONSTRAINT chk_state_resources_vote_by_mail_info_text
        CHECK (btrim(vote_by_mail_info) <> '' AND char_length(vote_by_mail_info) <= 4000),
    CONSTRAINT chk_state_resources_polling_hours_text
        CHECK (btrim(polling_hours) <> '' AND char_length(polling_hours) <= 1000),
    CONSTRAINT chk_state_resources_sources
        CHECK (is_valid_state_resource_sources(sources))
);

CREATE TRIGGER trg_users_set_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_candidates_set_updated_at
BEFORE UPDATE ON candidates
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_elections_set_updated_at
BEFORE UPDATE ON elections
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_candidate_elections_set_updated_at
BEFORE UPDATE ON candidate_elections
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_propositions_set_updated_at
BEFORE UPDATE ON propositions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_user_election_follows_set_updated_at
BEFORE UPDATE ON user_election_follows
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_user_districts_set_updated_at
BEFORE UPDATE ON user_districts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_staging_items_set_updated_at
BEFORE UPDATE ON staging_items
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
