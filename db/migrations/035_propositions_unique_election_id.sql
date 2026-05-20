BEGIN;

-- Data model decision (intentional): one ballot-measure detail row per election row.
-- This migration enforces 1:1 by keeping a single row per election_id
-- (latest updated_at/created_at/id) before adding the UNIQUE constraint.
WITH ranked AS (
  SELECT
    id,
    election_id,
    ROW_NUMBER() OVER (
      PARTITION BY election_id
      ORDER BY updated_at DESC, created_at DESC, id DESC
    ) AS rn
  FROM public.propositions
)
DELETE FROM public.propositions p
USING ranked r
WHERE p.id = r.id
  AND r.rn > 1;

ALTER TABLE public.propositions
  DROP CONSTRAINT IF EXISTS uq_propositions_election_id;

ALTER TABLE public.propositions
  ADD CONSTRAINT uq_propositions_election_id UNIQUE (election_id);

COMMIT;
