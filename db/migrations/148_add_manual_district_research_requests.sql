BEGIN;

-- Demand-driven manual research queue. When an address lookup resolves a
-- district that was never researched or is stale (>cooldown days), a row is
-- enqueued here. An interchangeable agent (Claude/Codex/human) claims one via
-- CLI wrappers and runs the existing manual research flow against district_id.
-- Postgres is the source of truth; any Redis/BullMQ nudge is a later add-on.
CREATE TABLE public.manual_district_research_requests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    district_id uuid NOT NULL,
    -- Snapshots taken at enqueue time so the queue is self-describing even if
    -- the district row later changes; district_id remains the source of truth.
    district_name_snapshot text NOT NULL,
    district_type_snapshot text NOT NULL,
    state_snapshot text NOT NULL,
    trigger_source text NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    -- Why the request was enqueued: the district's freshness at request time.
    last_elections_searched_at_at_request timestamptz,
    requested_at timestamptz NOT NULL DEFAULT now(),
    -- Repeat lookups for a district with an open request bump these instead of
    -- inserting a duplicate row (see the partial unique index below). The
    -- count is the demand signal: claim orders hottest districts first.
    last_requested_at timestamptz NOT NULL DEFAULT now(),
    request_count integer NOT NULL DEFAULT 1,
    claimed_at timestamptz,
    claimed_by text,
    agent_kind text,
    started_at timestamptz,
    finished_at timestamptz,
    manifest_path text,
    summary text,
    last_error text,
    attempt_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_manual_district_research_requests_district
        FOREIGN KEY (district_id)
        REFERENCES public.districts(id)
        ON DELETE CASCADE,
    CONSTRAINT chk_manual_district_research_requests_status
        CHECK (status IN ('queued', 'claimed', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')),
    CONSTRAINT chk_manual_district_research_requests_trigger_source
        CHECK (trigger_source IN ('address_resolve', 'me_address_update', 'manual_seed')),
    CONSTRAINT chk_manual_district_research_requests_agent_kind
        CHECK (agent_kind IS NULL OR agent_kind IN ('claude', 'codex', 'human', 'other')),
    CONSTRAINT chk_manual_district_research_requests_request_count
        CHECK (request_count >= 1),
    CONSTRAINT chk_manual_district_research_requests_attempt_count
        CHECK (attempt_count >= 0)
);

-- Dedupe: at most one open request per district. Repeat triggers bump the
-- existing open row instead of inserting. Terminal statuses (succeeded,
-- failed, skipped, cancelled) are excluded so a district can be re-enqueued
-- after a prior request closes.
CREATE UNIQUE INDEX uq_manual_district_research_requests_open_district
    ON public.manual_district_research_requests (district_id)
    WHERE status IN ('queued', 'claimed', 'running');

-- Claim scan: filter by status, oldest first (the store additionally orders
-- by request_count for demand priority).
CREATE INDEX idx_manual_district_research_requests_status_requested_at
    ON public.manual_district_research_requests (status, requested_at);

CREATE INDEX idx_manual_district_research_requests_district_id
    ON public.manual_district_research_requests (district_id);

COMMIT;
