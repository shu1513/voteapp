BEGIN;

UPDATE public.candidate_records
SET record_identity_key = 'v2_' || md5(
  concat_ws(
    '|',
    'v2',
    regexp_replace(lower(btrim(source_url)), '/+$', ''),
    event_date::text,
    regexp_replace(regexp_replace(lower(btrim(title)), '[^a-z0-9]+', ' ', 'g'), '\s+', ' ', 'g')
  )
);

WITH ranked AS (
  SELECT
    id,
    row_number() OVER (
      PARTITION BY candidate_id, record_identity_key
      ORDER BY created_at ASC, id ASC
    ) AS rn
  FROM public.candidate_records
)
DELETE FROM public.candidate_records AS cr
USING ranked
WHERE cr.id = ranked.id
  AND ranked.rn > 1;

ALTER TABLE public.candidate_records
  DROP COLUMN IF EXISTS source_name;

COMMIT;
