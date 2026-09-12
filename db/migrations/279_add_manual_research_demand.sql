BEGIN;

-- Demand ledger for research gaps finer than a district. The district queue
-- (migration 150) goes quiet once a district's elections were searched; this
-- table keeps counting user address lookups that land on an election with no
-- roster, a candidate with no profile or records, a measure never researched,
-- a past race with no result, or a primary whose general was never imported.
-- Rows are counters, not claims: the gap itself is recomputed from live data
-- whenever the ledger is read (manual:demand:status), so a gap closed through
-- any research path drops off with no status to drift. request_count is the
-- priority signal for the existing due lists.
CREATE TABLE public.manual_research_demand (
    stage text NOT NULL,
    -- election id (candidate_roster, ballot_measure, election_results,
    -- missing_general) or candidate id (candidate_profile,
    -- candidate_records); no FK because the target is polymorphic.
    target_id uuid NOT NULL,
    district_id uuid NOT NULL,
    -- The election that put the target on the user's ballot (the target
    -- itself for election-keyed stages).
    election_id uuid,
    state text NOT NULL,
    -- Snapshot so the ledger reads on its own: ballot title, candidate name
    -- or measure title at the last bump.
    label text NOT NULL,
    election_date date,
    request_count integer NOT NULL DEFAULT 1,
    first_requested_at timestamptz NOT NULL DEFAULT now(),
    last_requested_at timestamptz NOT NULL DEFAULT now(),
    last_trigger_source text NOT NULL,
    PRIMARY KEY (stage, target_id),
    CONSTRAINT fk_manual_research_demand_district
        FOREIGN KEY (district_id) REFERENCES public.districts(id) ON DELETE CASCADE,
    CONSTRAINT fk_manual_research_demand_election
        FOREIGN KEY (election_id) REFERENCES public.elections(id) ON DELETE CASCADE,
    CONSTRAINT chk_manual_research_demand_stage
        CHECK (stage IN ('candidate_roster', 'candidate_profile', 'candidate_records', 'ballot_measure', 'election_results', 'missing_general')),
    CONSTRAINT chk_manual_research_demand_trigger_source
        CHECK (last_trigger_source IN ('address_resolve', 'me_address_update')),
    CONSTRAINT chk_manual_research_demand_request_count
        CHECK (request_count >= 1)
);

-- Hottest first within a stage (manual:demand:status).
CREATE INDEX idx_manual_research_demand_hot
    ON public.manual_research_demand (stage, request_count DESC, last_requested_at DESC);

CREATE INDEX idx_manual_research_demand_district_id
    ON public.manual_research_demand (district_id);

-- The API role records demand from address lookups (docs/postgres-api-role.md);
-- guarded because the role does not exist in local dev.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT INSERT, UPDATE ON public.manual_research_demand TO voteapp_api;
  END IF;
END $$;

COMMIT;
