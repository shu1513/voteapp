BEGIN;

CREATE TABLE IF NOT EXISTS public.election_senate_metadata (
  election_id uuid PRIMARY KEY REFERENCES public.elections(id) ON DELETE CASCADE,
  senate_class text NULL,
  term_end_year text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_election_senate_metadata_class
    CHECK (senate_class IS NULL OR senate_class IN ('class_i', 'class_ii', 'class_iii')),
  CONSTRAINT chk_election_senate_metadata_term_end_year
    CHECK (term_end_year IS NULL OR term_end_year ~ '^[0-9]{4}$')
);

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

DROP INDEX IF EXISTS uq_elections_district_title_key_date_variant;

CREATE UNIQUE INDEX IF NOT EXISTS uq_elections_district_title_key_date
  ON public.elections (district_id, official_ballot_title_key, election_date);

ALTER TABLE public.elections
  DROP COLUMN IF EXISTS contest_variant_key;

COMMIT;
