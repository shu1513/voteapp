BEGIN;

-- Migration 157 keys open deferrals as (election_id, stage) when an election is
-- named and (district_id, stage) when it is not. The election-scoped half works:
-- districts routinely carry a dozen-plus concurrent per-election roster
-- deferrals (St. Tammany Parish holds 32) and none collide.
--
-- The district-wide half is the problem. The `elections` and `ballot_measure`
-- stages are district-wide BY CONSTRUCTION -- when election discovery itself is
-- blocked there is no election row to point `--election-id` at -- so every
-- distinct blocker on those stages lands on the same (district_id, stage) key
-- and the second `record` silently replaces the first. This has cost real work
-- (2026-07-10: a judicial-retention deferral clobbered a runoff-generals one),
-- and the standing workaround is for researchers to hand-merge unrelated
-- blockers into a single `reason` paragraph at the earliest date -- which buries
-- the later blocker in prose the due list cannot itemize.
--
-- blocker_key is the discriminator that lets those coexist as separate rows:
-- a short caller-supplied slug naming WHICH blocker the row tracks
-- ('ballot_measure_family', 'office_matcher'), NULL for the ordinary
-- one-blocker-per-stage case.
ALTER TABLE public.manual_research_deferrals
  ADD COLUMN blocker_key text;

-- Slug shape, so the discriminator stays a stable machine key across the
-- sessions that re-record a row rather than drifting into free text (which
-- would silently mint a new row every retry instead of updating the old one).
ALTER TABLE public.manual_research_deferrals
  ADD CONSTRAINT chk_manual_research_deferrals_blocker_key
  CHECK (blocker_key IS NULL OR blocker_key ~ '^[a-z0-9][a-z0-9_-]{0,39}$');

-- Rebuild both open-row unique indexes with the discriminator folded in.
--
-- coalesce(blocker_key, ''), not a bare column: Postgres treats NULLs as
-- DISTINCT in a unique index by default, so a bare blocker_key would let
-- unlimited NULL-keyed rows pile up per (district_id, stage) and destroy the
-- dedupe that is this index's whole purpose. PG15+ could say NULLS NOT
-- DISTINCT; the expression form behaves identically and does not pin a version.
--
-- Every existing row has blocker_key NULL, so both indexes cover exactly the
-- same tuples they did before and this rebuild cannot fail on current data.
DROP INDEX public.uq_manual_research_deferrals_open_election;
DROP INDEX public.uq_manual_research_deferrals_open_district;

CREATE UNIQUE INDEX uq_manual_research_deferrals_open_election
    ON public.manual_research_deferrals (election_id, stage, coalesce(blocker_key, ''))
    WHERE status = 'deferred' AND election_id IS NOT NULL;

CREATE UNIQUE INDEX uq_manual_research_deferrals_open_district
    ON public.manual_research_deferrals (district_id, stage, coalesce(blocker_key, ''))
    WHERE status = 'deferred' AND election_id IS NULL;

COMMIT;
