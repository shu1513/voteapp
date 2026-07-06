BEGIN;

CREATE TABLE IF NOT EXISTS public.content_reports (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type text NOT NULL,
  entity_id uuid NOT NULL,
  entity_label_snapshot text NOT NULL,
  message text NOT NULL,
  suggested_source_url text,
  reporter_email text,
  user_id uuid REFERENCES public.users(id) ON DELETE SET NULL,
  status text NOT NULL DEFAULT 'new',
  claimed_at timestamptz,
  claimed_by text,
  agent_kind text,
  attempt_count integer NOT NULL DEFAULT 0,
  investigation_summary text,
  resolution text,
  finished_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_content_reports_entity_type
    CHECK (entity_type IN ('candidate', 'candidate_record', 'election', 'ballot_measure')),
  CONSTRAINT chk_content_reports_status
    CHECK (status IN ('new', 'investigating', 'resolved', 'dismissed')),
  CONSTRAINT chk_content_reports_agent_kind
    CHECK (agent_kind IS NULL OR agent_kind IN ('claude', 'codex', 'human', 'other')),
  CONSTRAINT chk_content_reports_resolution
    CHECK (resolution IS NULL OR resolution IN ('fixed', 'no_change_needed', 'unverifiable', 'duplicate', 'spam')),
  CONSTRAINT chk_content_reports_resolution_status
    CHECK (
      (resolution IS NULL AND status IN ('new', 'investigating'))
      OR (resolution IN ('fixed', 'no_change_needed', 'duplicate') AND status = 'resolved')
      OR (resolution IN ('unverifiable', 'spam') AND status = 'dismissed')
    ),
  CONSTRAINT chk_content_reports_message_not_blank
    CHECK (btrim(message) <> '' AND char_length(message) <= 2000),
  CONSTRAINT chk_content_reports_label_not_blank
    CHECK (btrim(entity_label_snapshot) <> '' AND char_length(entity_label_snapshot) <= 500),
  CONSTRAINT chk_content_reports_suggested_source_url_length
    CHECK (suggested_source_url IS NULL OR char_length(suggested_source_url) <= 2048),
  CONSTRAINT chk_content_reports_reporter_email_length
    CHECK (reporter_email IS NULL OR char_length(reporter_email) <= 320),
  CONSTRAINT chk_content_reports_attempt_count
    CHECK (attempt_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_content_reports_status_created_at
  ON public.content_reports (status, created_at);

CREATE INDEX IF NOT EXISTS idx_content_reports_open_entity
  ON public.content_reports (entity_type, entity_id)
  WHERE status IN ('new', 'investigating');

CREATE INDEX IF NOT EXISTS idx_content_reports_user_id
  ON public.content_reports (user_id)
  WHERE user_id IS NOT NULL;

DROP TRIGGER IF EXISTS trg_content_reports_set_updated_at ON public.content_reports;
CREATE TRIGGER trg_content_reports_set_updated_at
BEFORE UPDATE ON public.content_reports
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMIT;
