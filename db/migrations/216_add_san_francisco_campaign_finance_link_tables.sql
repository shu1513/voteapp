-- San Francisco campaign-finance identity tables (plan-san-francisco-finance.md
-- Phase 3). Only the two link tables ship here; summaries/breakdowns/outside
-- group amounts follow in Phase 5. Modeled on the Los Angeles City tables
-- (migration 173), with the committee identity swapped for the SFEC dashboard
-- manifest's: contest code + FPPC id + filer_nid.

BEGIN;

CREATE TABLE public.sfc_candidate_finance_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  candidate_name_normalized text NOT NULL CHECK (btrim(candidate_name_normalized) <> ''),
  -- SFEC dashboard contest code ("myr", "bos04", ...); doubles as the manifest
  -- file locator together with the election date.
  contest_code text NOT NULL CHECK (contest_code ~ '^[a-z0-9]{2,20}$'),
  fppc_id text NOT NULL CHECK (btrim(fppc_id) <> ''),
  filer_nid text NOT NULL CHECK (btrim(filer_nid) <> ''),
  committee_name text NOT NULL CHECK (btrim(committee_name) <> ''),
  link_status text NOT NULL DEFAULT 'active' CHECK (link_status IN ('active', 'needs_review', 'inactive')),
  link_source text NOT NULL DEFAULT 'manual' CHECK (link_source IN ('manual', 'sfec_dashboard')),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (candidate_id, election_id, fppc_id),
  UNIQUE (id, election_year)
);
CREATE UNIQUE INDEX sfc_candidate_finance_links_active_candidate_election_idx
  ON public.sfc_candidate_finance_links (candidate_id, election_id) WHERE link_status = 'active';
CREATE INDEX sfc_candidate_finance_links_election_candidate_idx
  ON public.sfc_candidate_finance_links (election_id, candidate_id);
CREATE TRIGGER sfc_candidate_finance_links_set_updated_at
  BEFORE UPDATE ON public.sfc_candidate_finance_links
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- One row per manifest outside-spending relation: (candidate, election,
-- spender committee, direction). Direction is per relation, not per committee
-- (one committee legitimately supports one candidate while opposing another in
-- the same contest). Identity only — per-cycle amounts live in the Phase 5
-- outside-groups table. spender_fppc_id carries a synthetic
-- "name:<normalized committee name>" identity when the manifest entry has no
-- committee id, so id-less money is never silently dropped. The manifest names
-- outside spenders with filer_id only, so no filer_nid column exists here.
CREATE TABLE public.sfc_candidate_finance_outside_committee_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  election_id uuid NOT NULL REFERENCES public.elections(id) ON DELETE CASCADE,
  election_year integer NOT NULL CHECK (election_year BETWEEN 2000 AND 2100),
  spender_fppc_id text NOT NULL CHECK (btrim(spender_fppc_id) <> ''),
  spender_name text NOT NULL CHECK (btrim(spender_name) <> ''),
  support_oppose text NOT NULL CHECK (support_oppose IN ('support', 'oppose')),
  relation_source text NOT NULL DEFAULT 'sfec_dashboard' CHECK (relation_source IN ('sfec_dashboard')),
  source_url text CHECK (source_url IS NULL OR btrim(source_url) <> ''),
  last_verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (candidate_id, election_id, spender_fppc_id, support_oppose)
);
CREATE INDEX sfc_candidate_finance_outside_committee_links_election_idx
  ON public.sfc_candidate_finance_outside_committee_links (election_id, candidate_id);
CREATE TRIGGER sfc_candidate_finance_outside_committee_links_set_updated_at
  BEFORE UPDATE ON public.sfc_candidate_finance_outside_committee_links
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
