BEGIN;

-- Georgia cross-system filer identity map (georgia_plan.md D3, shape pinned
-- by spike result A6). One row per (source system, registration): a canonical
-- PeachFile entity maps to the SAME registration chain re-keyed across the
-- two portals (archive 757274 <-> PeachFile 100035), never to a legally
-- separate committee — Carr's terminated legacy committee stays its own
-- entity. Candidate-total inclusion is an explicit per-row property, not a
-- consequence of sharing a canonical id, and outside spenders can never be
-- inside candidate totals (the CHECK makes that unrepresentable).
CREATE TABLE IF NOT EXISTS public.ga_finance_filer_identity_map (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- PeachFile filerEntityId; matches ga_candidate_finance_links.committee_id
  -- for candidate committees and outside-group committee_id for spenders.
  canonical_committee_id text NOT NULL,
  canonical_committee_name text NOT NULL,
  entity_role text NOT NULL,
  source_system text NOT NULL,
  source_filer_entity_id text NOT NULL,
  source_registration_guid uuid NOT NULL,
  -- The name form this host actually keys searches on (spike result A3):
  -- archive = person display name ("Carr, Christopher Michael"),
  -- PeachFile = committee name ("Carr for Georgia, Inc.").
  source_filer_name text NOT NULL,
  source_committee_name text,
  source_filing_cycle_name text,
  include_in_candidate_totals boolean NOT NULL,
  map_provenance text NOT NULL,
  notes text,
  last_verified_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ga_ffim_canonical_committee_id_check
    CHECK (btrim(canonical_committee_id) <> ''),
  CONSTRAINT ga_ffim_canonical_committee_name_check
    CHECK (btrim(canonical_committee_name) <> ''),
  CONSTRAINT ga_ffim_entity_role_check
    CHECK (entity_role IN ('candidate_committee', 'outside_spender')),
  CONSTRAINT ga_ffim_source_system_check
    CHECK (source_system IN ('peachfile', 'efile_archive')),
  CONSTRAINT ga_ffim_source_filer_entity_id_check
    CHECK (btrim(source_filer_entity_id) <> ''),
  CONSTRAINT ga_ffim_source_filer_name_check
    CHECK (btrim(source_filer_name) <> ''),
  CONSTRAINT ga_ffim_source_committee_name_check
    CHECK (source_committee_name IS NULL OR btrim(source_committee_name) <> ''),
  CONSTRAINT ga_ffim_source_filing_cycle_name_check
    CHECK (source_filing_cycle_name IS NULL OR btrim(source_filing_cycle_name) <> ''),
  CONSTRAINT ga_ffim_provenance_check
    CHECK (map_provenance IN ('reconciled', 'manual')),
  CONSTRAINT ga_ffim_outside_spender_totals_check
    CHECK (entity_role = 'candidate_committee' OR include_in_candidate_totals = false),
  -- A registration belongs to exactly one canonical entity.
  CONSTRAINT ga_ffim_source_registration_unique
    UNIQUE (source_system, source_registration_guid)
);

CREATE INDEX IF NOT EXISTS ga_ffim_canonical_committee_idx
  ON public.ga_finance_filer_identity_map (canonical_committee_id);

DROP TRIGGER IF EXISTS ga_finance_filer_identity_map_set_updated_at
  ON public.ga_finance_filer_identity_map;
CREATE TRIGGER ga_finance_filer_identity_map_set_updated_at
BEFORE UPDATE ON public.ga_finance_filer_identity_map
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
