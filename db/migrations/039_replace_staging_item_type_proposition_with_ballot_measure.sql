BEGIN;

-- Replace legacy wire value with current ballot-measure naming.
UPDATE public.staging_items
SET item_type = 'ballot_measure'
WHERE item_type = 'proposition';

ALTER TABLE public.staging_items
DROP CONSTRAINT IF EXISTS chk_staging_items_type;

ALTER TABLE public.staging_items
ADD CONSTRAINT chk_staging_items_type
CHECK (
  item_type IN (
    'district',
    'candidate',
    'election',
    'ballot_measure',
    'candidate_record',
    'state_resources'
  )
);

COMMIT;
