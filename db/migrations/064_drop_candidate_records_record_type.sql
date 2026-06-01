BEGIN;

ALTER TABLE public.candidate_records
  DROP COLUMN IF EXISTS record_type;

COMMIT;
