BEGIN;

ALTER TABLE public.candidate_records
  ADD COLUMN IF NOT EXISTS record_identity_key_v2_fix text;

UPDATE public.candidate_records
SET record_identity_key_v2_fix = 'v2_' || md5(
  concat_ws(
    '|',
    'v2',
    regexp_replace(lower(btrim(source_url)), '/+$', ''),
    event_date::text,
    btrim(
      regexp_replace(
        regexp_replace(lower(btrim(title)), '[^a-z0-9]+', ' ', 'g'),
        '\s+',
        ' ',
        'g'
      )
    )
  )
);

WITH ranked AS (
  SELECT
    id,
    candidate_id,
    record_identity_key_v2_fix,
    row_number() OVER (
      PARTITION BY candidate_id, record_identity_key_v2_fix
      ORDER BY created_at ASC, id ASC
    ) AS rn
  FROM public.candidate_records
),
keepers AS (
  SELECT
    candidate_id,
    record_identity_key_v2_fix,
    id AS kept_id
  FROM ranked
  WHERE rn = 1
),
mapping AS (
  SELECT
    k.kept_id,
    r.id AS deleted_id
  FROM ranked r
  JOIN keepers k
    ON k.candidate_id = r.candidate_id
   AND k.record_identity_key_v2_fix = r.record_identity_key_v2_fix
  WHERE r.rn > 1
)
INSERT INTO public.candidate_record_area_tags (
  candidate_record_id,
  research_area_id,
  stance,
  created_at,
  updated_at
)
SELECT
  m.kept_id,
  t.research_area_id,
  t.stance,
  t.created_at,
  t.updated_at
FROM mapping m
JOIN public.candidate_record_area_tags t
  ON t.candidate_record_id = m.deleted_id
ON CONFLICT (candidate_record_id, research_area_id) DO NOTHING;

WITH ranked AS (
  SELECT
    id,
    candidate_id,
    record_identity_key_v2_fix,
    row_number() OVER (
      PARTITION BY candidate_id, record_identity_key_v2_fix
      ORDER BY created_at ASC, id ASC
    ) AS rn
  FROM public.candidate_records
)
DELETE FROM public.candidate_records cr
USING ranked
WHERE cr.id = ranked.id
  AND ranked.rn > 1;

UPDATE public.candidate_records
SET record_identity_key = record_identity_key_v2_fix
WHERE record_identity_key IS DISTINCT FROM record_identity_key_v2_fix;

ALTER TABLE public.candidate_records
  DROP COLUMN IF EXISTS record_identity_key_v2_fix;

COMMIT;
