BEGIN;

UPDATE public.presidential_cycles
SET status = 'active'
WHERE status = 'upcoming';

ALTER TABLE public.presidential_cycles
  ALTER COLUMN status SET DEFAULT 'active';

ALTER TABLE public.presidential_cycles
  DROP CONSTRAINT IF EXISTS chk_presidential_cycles_status;

ALTER TABLE public.presidential_cycles
  ADD CONSTRAINT chk_presidential_cycles_status
  CHECK (status IN ('active', 'completed'));

COMMIT;
