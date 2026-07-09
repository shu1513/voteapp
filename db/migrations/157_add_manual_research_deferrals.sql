BEGIN;

-- A deferral row that names an election also stores that election's district.
-- Without a composite key on the parent, nothing stops a row from pairing
-- district A with an election from district B, and the `due` worklist would
-- then surface a self-contradictory research unit. elections.id is already
-- unique, so this only adds the (id, district_id) pair the composite FK below
-- needs (same pattern as migration 096 for ballot_measure_results).
ALTER TABLE public.elections
  DROP CONSTRAINT IF EXISTS uq_elections_id_district;

ALTER TABLE public.elections
  ADD CONSTRAINT uq_elections_id_district
  UNIQUE (id, district_id);

-- Deferral ledger for manual research. When a research pass finds that a
-- stage cannot be completed until a known future date (general roster not
-- certified until September, filing closes in July, ballot questions not yet
-- posted), the agent records the deferral here instead of losing it in a run
-- report. A later session lists rows whose blocked_until has passed and
-- re-runs just those units. Postgres is the source of truth; CLI wrappers in
-- src/scripts/manualResearchDeferrals.ts are the whole interface.
CREATE TABLE public.manual_research_deferrals (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    district_id uuid NOT NULL,
    -- NULL when the deferral is district-wide (e.g. election discovery itself
    -- is blocked); set when a specific written election's downstream stage is
    -- blocked (e.g. its roster awaits certification).
    election_id uuid,
    stage text NOT NULL,
    reason text NOT NULL,
    -- Earliest date a retry can succeed: the certification/filing/publication
    -- date from the official calendar, not a guess.
    blocked_until date NOT NULL,
    -- Where the blocked_until date came from (official calendar page).
    source_url text,
    -- Snapshot so listings are self-describing without a join.
    district_name_snapshot text NOT NULL,
    status text NOT NULL DEFAULT 'deferred',
    resolved_at timestamptz,
    resolution_note text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_manual_research_deferrals_district
        FOREIGN KEY (district_id)
        REFERENCES public.districts(id)
        ON DELETE CASCADE,
    -- Composite FK, not a plain elections(id) reference: it pins the named
    -- election to THIS row's district. Default MATCH SIMPLE means the
    -- constraint is not enforced when election_id IS NULL, which is exactly
    -- the district-wide deferral case.
    CONSTRAINT fk_manual_research_deferrals_election_district
        FOREIGN KEY (election_id, district_id)
        REFERENCES public.elections(id, district_id)
        ON DELETE CASCADE,
    CONSTRAINT chk_manual_research_deferrals_stage
        CHECK (stage IN ('elections', 'candidate_roster', 'candidate_profile', 'candidate_records', 'ballot_measure')),
    CONSTRAINT chk_manual_research_deferrals_status
        CHECK (status IN ('deferred', 'resolved', 'cancelled')),
    -- Lifecycle fields must agree with status, so `due`/`status` scans can
    -- never read a half-closed row (deferred with a close timestamp, or
    -- resolved/cancelled without one).
    CONSTRAINT chk_manual_research_deferrals_resolution_state
        CHECK (
            (status = 'deferred' AND resolved_at IS NULL)
            OR (status IN ('resolved', 'cancelled') AND resolved_at IS NOT NULL)
        )
);

-- Dedupe open deferrals: one per election+stage, and one district-wide per
-- district+stage. Re-recording bumps blocked_until/reason instead of piling
-- duplicates; terminal statuses allow a fresh deferral later.
CREATE UNIQUE INDEX uq_manual_research_deferrals_open_election
    ON public.manual_research_deferrals (election_id, stage)
    WHERE status = 'deferred' AND election_id IS NOT NULL;

CREATE UNIQUE INDEX uq_manual_research_deferrals_open_district
    ON public.manual_research_deferrals (district_id, stage)
    WHERE status = 'deferred' AND election_id IS NULL;

-- Due scan: `due` lists deferred rows past their date, oldest first.
CREATE INDEX idx_manual_research_deferrals_due
    ON public.manual_research_deferrals (blocked_until, created_at)
    WHERE status = 'deferred';

CREATE INDEX idx_manual_research_deferrals_district_id
    ON public.manual_research_deferrals (district_id);

COMMIT;
