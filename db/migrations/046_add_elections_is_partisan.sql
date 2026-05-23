BEGIN;

ALTER TABLE public.elections
  ADD COLUMN IF NOT EXISTS is_partisan boolean;

COMMIT;
