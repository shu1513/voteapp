BEGIN;

-- One row per roll-call vote (federal House/Senate first; state chambers
-- later). This is the review queue for the roll-call import
-- (docs/plans/roll-call-vote-import.md): the fetcher stores every roll call
-- it sees, the question-class filter and per-source committee detection set
-- is_floor_vote, the guarded AI run fills the two sentences + labels, a human
-- flips review_status, and only approved rows fan out into candidate_records.
--
-- Member-level votes are NOT stored here; the fetched XML lives under
-- backend/evidence/rollcall/<run-id>/ and is re-read at fan-out time.
CREATE TABLE public.legislative_votes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- 'US' for Congress, USPS code for a state legislature.
  jurisdiction text NOT NULL,
  chamber text NOT NULL,
  -- Federal: '<congress>-<session>' (e.g. '119-1'); state: the source's
  -- session key.
  session text NOT NULL,
  roll_number integer NOT NULL,
  vote_date date NOT NULL,
  -- Measure as the source prints it (e.g. 'H R 1', 'S.J.Res. 3'); NULL for
  -- votes with no measure (quorum calls, Speaker election). Normalization for
  -- duplicate detection happens in code, not here.
  measure_id text,
  exact_question text NOT NULL,
  -- Text version the chamber voted on (e.g. 'eh', 'rh'), when the source
  -- says.
  voted_text_version text,
  -- true = floor vote of a kept question class, eligible for the queue;
  -- false = known non-floor or excluded class; NULL = unknown. Anything not
  -- true is never queued.
  is_floor_vote boolean,
  result text NOT NULL,
  yeas integer NOT NULL,
  nays integer NOT NULL,
  -- Human page, machine-readable file (the record's source_url), and the bill
  -- page kept for audit only.
  display_url text NOT NULL,
  machine_url text NOT NULL,
  bill_url text,
  source_sha256 text NOT NULL,
  fetched_at timestamptz NOT NULL,
  -- Filled by the guarded AI judgment pass, then reviewed by a human. The
  -- sentences carry no member name; labels_json is an array of
  -- {"slug": <research area>, "yea": "for" | "against" | null}.
  yea_description text,
  nay_description text,
  labels_json jsonb,
  review_status text NOT NULL DEFAULT 'pending',
  reviewed_at timestamptz,
  importer_version text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT legislative_votes_roll_key
    UNIQUE (jurisdiction, chamber, session, roll_number),
  CONSTRAINT legislative_votes_jurisdiction_check
    CHECK (jurisdiction ~ '^[A-Z]{2}$'),
  CONSTRAINT legislative_votes_chamber_check
    CHECK (chamber IN ('house', 'senate')),
  CONSTRAINT legislative_votes_session_check
    CHECK (btrim(session) <> ''),
  CONSTRAINT legislative_votes_roll_number_check
    CHECK (roll_number > 0),
  CONSTRAINT legislative_votes_tally_check
    CHECK (yeas >= 0 AND nays >= 0),
  CONSTRAINT legislative_votes_source_sha256_check
    CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT legislative_votes_review_status_check
    CHECK (review_status IN ('pending', 'approved', 'rejected')),
  CONSTRAINT legislative_votes_labels_json_check
    CHECK (labels_json IS NULL OR jsonb_typeof(labels_json) = 'array'),
  -- A bad template replicates x435, so approval is only possible once the
  -- row is a floor vote with both sentences and labels in place.
  CONSTRAINT legislative_votes_approved_fields_check
    CHECK (
      review_status <> 'approved'
      OR (
        is_floor_vote = true
        AND yea_description IS NOT NULL
        AND nay_description IS NOT NULL
        AND labels_json IS NOT NULL
        AND reviewed_at IS NOT NULL
      )
    )
);

CREATE TRIGGER trg_legislative_votes_set_updated_at
BEFORE UPDATE ON public.legislative_votes
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
