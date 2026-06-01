BEGIN;

ALTER TABLE public.candidates
  ADD COLUMN IF NOT EXISTS last_records_researched_through date,
  ADD COLUMN IF NOT EXISTS records_search_claimed_at timestamptz;

ALTER TABLE public.candidate_records
  ADD COLUMN IF NOT EXISTS record_identity_key text;

UPDATE public.candidate_records
SET record_identity_key = 'v1_' || md5(
  concat_ws(
    '|',
    'v1',
    regexp_replace(lower(btrim(source_url)), '/+$', ''),
    event_date::text,
    regexp_replace(regexp_replace(lower(btrim(title)), '[^a-z0-9]+', ' ', 'g'), '\s+', ' ', 'g'),
    regexp_replace(regexp_replace(lower(btrim(source_name)), '[^a-z0-9]+', ' ', 'g'), '\s+', ' ', 'g')
  )
)
WHERE record_identity_key IS NULL;

WITH ranked AS (
  SELECT
    id,
    row_number() OVER (
      PARTITION BY candidate_id, record_identity_key
      ORDER BY created_at ASC, id ASC
    ) AS rn
  FROM public.candidate_records
  WHERE record_identity_key IS NOT NULL
)
DELETE FROM public.candidate_records AS cr
USING ranked
WHERE cr.id = ranked.id
  AND ranked.rn > 1;

ALTER TABLE public.candidate_records
  ALTER COLUMN record_identity_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_records_candidate_identity_key
  ON public.candidate_records (candidate_id, record_identity_key);

COMMIT;
