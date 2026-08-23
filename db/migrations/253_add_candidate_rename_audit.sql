BEGIN;

-- Audit ledger for guarded candidate display-name corrections
-- (npm run manual:candidates:rename). The profile writer fills blanks only
-- and --replace-profile-fields deliberately excludes identity fields, so a
-- wrong stored ballot name (live case: a candidate stored under a former
-- legal name) had no supported correction path. The rename wrapper writes
-- one row here in the same transaction as the candidates update so every
-- name change stays traceable to its official source and operator reason.
CREATE TABLE IF NOT EXISTS public.candidate_rename_audit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
  old_display_name text,
  new_display_name text NOT NULL,
  -- Old first/last are always captured for context; the new_* columns stay
  -- NULL when the rename left that column unchanged.
  old_first_name text NOT NULL,
  new_first_name text,
  old_last_name text NOT NULL,
  new_last_name text,
  source_url text NOT NULL,
  reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_candidate_rename_audit_reason
    CHECK (char_length(reason) >= 20),
  CONSTRAINT chk_candidate_rename_audit_source_https
    CHECK (source_url LIKE 'https://%')
);

CREATE INDEX IF NOT EXISTS idx_candidate_rename_audit_candidate
  ON public.candidate_rename_audit (candidate_id);

COMMIT;
