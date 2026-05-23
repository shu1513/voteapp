BEGIN;

ALTER TABLE public.elections
ADD COLUMN IF NOT EXISTS official_ballot_title_key text;

UPDATE public.elections
SET official_ballot_title_key = trim(
  regexp_replace(
    trim(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            lower(official_ballot_title),
            '\moffice[[:space:]]*no\.?[[:space:]]*',
            'office no ',
            'g'
          ),
          '[^a-z0-9[:space:]]+',
          ' ',
          'g'
        ),
        '[[:space:]]+',
        ' ',
        'g'
      )
    ),
    '\moffice no 0+([0-9]+)\M',
    'office no \1',
    'g'
  )
)
WHERE official_ballot_title_key IS NULL
   OR official_ballot_title_key = '';

WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY district_id, official_ballot_title_key, election_date
      ORDER BY updated_at DESC, created_at DESC, id DESC
    ) AS rn
  FROM public.elections
)
DELETE FROM public.elections e
USING ranked r
WHERE e.id = r.id
  AND r.rn > 1;

DROP INDEX IF EXISTS uq_elections_district_title_date;

CREATE UNIQUE INDEX IF NOT EXISTS uq_elections_district_title_key_date
  ON public.elections (district_id, official_ballot_title_key, election_date);

ALTER TABLE public.elections
ALTER COLUMN official_ballot_title_key SET NOT NULL;

COMMIT;
