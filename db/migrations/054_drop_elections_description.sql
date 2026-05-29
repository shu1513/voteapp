BEGIN;

ALTER TABLE public.elections
  DROP COLUMN IF EXISTS description;

COMMIT;

