BEGIN;

-- Canonical archive index for every document in a legal bundle. The source
-- documents themselves remain versioned in docs/legal/ and git; these hashes
-- let an acceptance record prove exactly which bytes were in force without
-- relying on mutable "current terms" content.
CREATE TABLE public.legal_document_bundles (
  version text PRIMARY KEY,
  terms_version text NOT NULL,
  terms_sha256 char(64) NOT NULL CHECK (terms_sha256 ~ '^[0-9a-f]{64}$'),
  privacy_version text NOT NULL,
  privacy_sha256 char(64) NOT NULL CHECK (privacy_sha256 ~ '^[0-9a-f]{64}$'),
  disclaimer_version text NOT NULL,
  disclaimer_sha256 char(64) NOT NULL CHECK (disclaimer_sha256 ~ '^[0-9a-f]{64}$'),
  checkbox_copy_sha256 char(64) NOT NULL CHECK (checkbox_copy_sha256 ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.legal_document_bundles (
  version,
  terms_version,
  terms_sha256,
  privacy_version,
  privacy_sha256,
  disclaimer_version,
  disclaimer_sha256,
  checkbox_copy_sha256
)
VALUES (
  '1.2',
  '1.2',
  '94799b6a579e8ac1787283d4754d0552e8b078178827644fa3fff46201f7529c',
  '1.2',
  '673567507ba0f8e6551f9857145fd191cb85c89c6402b6e11b9ac7ce57533e72',
  '1.2',
  '6395006dab7d1cc01b683698b3ad07d27010d95a638f750cf0278c229a7b4e55',
  'ef2d03354602368e14251739ac046f91686d0eb1762dc0bb6f38bb3279ac3457'
);

-- Append-only evidence ledger. account_user_id deliberately is not an FK:
-- account deletion must still delete the users row as promised while this
-- pseudonymous contract evidence survives for legal-claims retention.
-- account_email_sha256 proves a known email corresponded to the accepting
-- account without retaining the plain-text address here.
CREATE TABLE public.legal_acceptance_events (
  id uuid PRIMARY KEY,
  account_user_id uuid,
  account_email_sha256 char(64) CHECK (
    account_email_sha256 IS NULL OR account_email_sha256 ~ '^[0-9a-f]{64}$'
  ),
  anonymous_subject_id uuid NOT NULL,
  context text NOT NULL CHECK (context IN ('anonymous_search', 'registration', 'terms_renewal')),
  document_bundle_version text NOT NULL REFERENCES public.legal_document_bundles (version),
  presentation_version text NOT NULL,
  acceptance_text text NOT NULL CHECK (btrim(acceptance_text) <> ''),
  action_text text NOT NULL CHECK (btrim(action_text) <> ''),
  accepted_at timestamptz NOT NULL DEFAULT now(),
  client_ip text,
  user_agent text,
  origin text,
  CONSTRAINT chk_legal_acceptance_account_evidence CHECK (
    context = 'anonymous_search'
    OR (account_user_id IS NOT NULL AND account_email_sha256 IS NOT NULL)
  )
);

CREATE INDEX idx_legal_acceptance_events_account
  ON public.legal_acceptance_events (account_user_id, accepted_at DESC)
  WHERE account_user_id IS NOT NULL;

CREATE INDEX idx_legal_acceptance_events_subject
  ON public.legal_acceptance_events (anonymous_subject_id, accepted_at DESC);

CREATE INDEX idx_legal_acceptance_events_bundle
  ON public.legal_acceptance_events (document_bundle_version, accepted_at DESC);

-- Evidence must never be rewritten or removed in place. Corrections and new
-- legal versions are additional rows. The application role also receives
-- INSERT only, but this trigger protects against accidental owner-role edits.
CREATE FUNCTION public.reject_legal_evidence_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'legal acceptance evidence is append-only';
END;
$$;

CREATE TRIGGER trg_legal_document_bundles_immutable
BEFORE UPDATE OR DELETE ON public.legal_document_bundles
FOR EACH ROW EXECUTE FUNCTION public.reject_legal_evidence_mutation();

CREATE TRIGGER trg_legal_acceptance_events_immutable
BEFORE UPDATE OR DELETE ON public.legal_acceptance_events
FOR EACH ROW EXECUTE FUNCTION public.reject_legal_evidence_mutation();

-- Production may use the optional least-privilege API role. Local databases
-- do not necessarily define it, so grant conditionally.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT SELECT, INSERT ON public.legal_acceptance_events TO voteapp_api;
  END IF;
END;
$$;

COMMIT;
