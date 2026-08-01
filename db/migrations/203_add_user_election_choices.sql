BEGIN;

-- A user's planned vote per election ("my choice"): candidate pick(s) for
-- office races, a yes/no position for ballot-measure races. One row per pick
-- so multi-seat contests ("vote for up to 3") can hold several candidate
-- rows; the write path caps pick count at COALESCE(seats_to_fill, 1),
-- matching the display convention that NULL seats renders as one seat.
CREATE TABLE public.user_election_choices (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
    candidate_id uuid,
    measure_position text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    -- Exactly one of candidate pick / measure position per row.
    CONSTRAINT chk_user_election_choices_one_kind
        CHECK ((candidate_id IS NULL) <> (measure_position IS NULL)),
    CONSTRAINT chk_user_election_choices_measure_position
        CHECK (measure_position IS NULL OR measure_position IN ('yes', 'no')),
    -- The composite FK targets uq_candidate_elections (candidate_id,
    -- election_id), so a candidate pick can only reference a real candidacy
    -- in that same election — a mismatched pair is unrepresentable.
    --
    -- ON UPDATE CASCADE exists for the candidate-merge repair script, whose
    -- rehome step rewrites candidate_elections.candidate_id in place; the
    -- pick follows to the surviving candidate row automatically. Collision-
    -- free by construction: rehome only happens when the survivor has no
    -- link in that election, and a pick for the survivor there cannot exist
    -- without one (this same FK). The link-move script never relies on this
    -- cascade — it refuses to move a link that picks reference, because a
    -- pick silently teleporting to a different race would be wrong.
    CONSTRAINT fk_user_election_choices_candidacy
        FOREIGN KEY (candidate_id, election_id)
        REFERENCES public.candidate_elections(candidate_id, election_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX uq_user_election_choices_candidate
    ON public.user_election_choices (user_id, election_id, candidate_id)
    WHERE candidate_id IS NOT NULL;

-- A measure race takes a single yes/no, never several rows.
CREATE UNIQUE INDEX uq_user_election_choices_measure
    ON public.user_election_choices (user_id, election_id)
    WHERE measure_position IS NOT NULL;

CREATE INDEX idx_user_election_choices_user_id
    ON public.user_election_choices (user_id);
CREATE INDEX idx_user_election_choices_election_id
    ON public.user_election_choices (election_id);
CREATE INDEX idx_user_election_choices_candidate_id
    ON public.user_election_choices (candidate_id)
    WHERE candidate_id IS NOT NULL;

COMMIT;
