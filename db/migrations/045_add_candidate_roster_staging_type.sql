BEGIN;

ALTER TABLE public.staging_items
DROP CONSTRAINT IF EXISTS chk_staging_items_type;

ALTER TABLE public.staging_items
ADD CONSTRAINT chk_staging_items_type
CHECK (
  item_type IN (
    'district',
    'candidate',
    'candidate_roster',
    'election',
    'ballot_measure',
    'candidate_record',
    'state_resources'
  )
);

COMMIT;
