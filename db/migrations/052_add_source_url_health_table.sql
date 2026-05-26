BEGIN;

CREATE TABLE IF NOT EXISTS public.source_url_health (
  url text PRIMARY KEY,
  last_checked_at timestamptz,
  last_http_status integer,
  last_error text,
  consecutive_hard_failures integer NOT NULL DEFAULT 0,
  first_hard_failed_at timestamptz,
  last_hard_failed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_source_url_health_http_status
    CHECK (last_http_status IS NULL OR (last_http_status BETWEEN 100 AND 599)),
  CONSTRAINT chk_source_url_health_consecutive_hard_failures
    CHECK (consecutive_hard_failures >= 0)
);

CREATE INDEX IF NOT EXISTS idx_source_url_health_last_checked_at
  ON public.source_url_health (last_checked_at ASC NULLS FIRST);

CREATE INDEX IF NOT EXISTS idx_source_url_health_cleanup
  ON public.source_url_health (consecutive_hard_failures DESC, first_hard_failed_at ASC NULLS FIRST);

DROP TRIGGER IF EXISTS trg_source_url_health_set_updated_at ON public.source_url_health;
CREATE TRIGGER trg_source_url_health_set_updated_at
BEFORE UPDATE ON public.source_url_health
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;

