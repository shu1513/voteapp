BEGIN;

-- This guard is intentionally installed after historical office consolidation
-- migrations. Future migrations that deliberately rehome aliases must handle
-- that explicitly instead of using ON CONFLICT ... DO UPDATE SET office_id.
CREATE OR REPLACE FUNCTION public.prevent_office_title_alias_reassignment()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.office_id IS DISTINCT FROM NEW.office_id THEN
    RAISE EXCEPTION
      'Cannot reassign office title alias %.% from office % to office %',
      OLD.scope,
      OLD.normalized_alias,
      OLD.office_id,
      NEW.office_id;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_prevent_office_title_alias_reassignment
  ON public.office_title_aliases;

CREATE TRIGGER trg_prevent_office_title_alias_reassignment
BEFORE UPDATE OF office_id ON public.office_title_aliases
FOR EACH ROW
EXECUTE FUNCTION public.prevent_office_title_alias_reassignment();

COMMIT;
