BEGIN;

ALTER TABLE state_resources
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

DROP TRIGGER IF EXISTS trg_state_resources_set_updated_at ON state_resources;

CREATE TRIGGER trg_state_resources_set_updated_at
BEFORE UPDATE ON state_resources
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
