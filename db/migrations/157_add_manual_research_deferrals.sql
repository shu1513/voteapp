BEGIN;

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
    CONSTRAINT fk_manual_research_deferrals_election
        FOREIGN KEY (election_id)
        REFERENCES public.elections(id)
        ON DELETE CASCADE,
    CONSTRAINT chk_manual_research_deferrals_stage
        CHECK (stage IN ('elections', 'candidate_roster', 'candidate_profile', 'candidate_records', 'ballot_measure')),
    CONSTRAINT chk_manual_research_deferrals_status
        CHECK (status IN ('deferred', 'resolved', 'cancelled'))
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
