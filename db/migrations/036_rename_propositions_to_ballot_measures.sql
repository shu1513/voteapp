BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'propositions'
  ) THEN
    ALTER TABLE public.propositions RENAME TO ballot_measures;
  END IF;
END $$;

-- Rename indexes (safe for mixed states).
ALTER INDEX IF EXISTS public.idx_propositions_election_id
  RENAME TO idx_ballot_measures_election_id;

ALTER INDEX IF EXISTS public.idx_propositions_district_id
  RENAME TO idx_ballot_measures_district_id;

-- Rename trigger if present.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'ballot_measures'
      AND t.tgname = 'trg_propositions_set_updated_at'
      AND NOT t.tgisinternal
  ) THEN
    ALTER TRIGGER trg_propositions_set_updated_at
      ON public.ballot_measures
      RENAME TO trg_ballot_measures_set_updated_at;
  END IF;
END $$;

-- Rename named constraints for clarity if they still use old table prefix.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_propositions_district'
  ) THEN
    ALTER TABLE public.ballot_measures
      RENAME CONSTRAINT fk_propositions_district TO fk_ballot_measures_district;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_propositions_election'
  ) THEN
    ALTER TABLE public.ballot_measures
      RENAME CONSTRAINT fk_propositions_election TO fk_ballot_measures_election;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_propositions_result'
  ) THEN
    ALTER TABLE public.ballot_measures
      RENAME CONSTRAINT chk_propositions_result TO chk_ballot_measures_result;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_propositions_notable_supporters_json'
  ) THEN
    ALTER TABLE public.ballot_measures
      RENAME CONSTRAINT chk_propositions_notable_supporters_json TO chk_ballot_measures_notable_supporters_json;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_propositions_notable_opponents_json'
  ) THEN
    ALTER TABLE public.ballot_measures
      RENAME CONSTRAINT chk_propositions_notable_opponents_json TO chk_ballot_measures_notable_opponents_json;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_propositions_election_id'
  ) THEN
    ALTER TABLE public.ballot_measures
      RENAME CONSTRAINT uq_propositions_election_id TO uq_ballot_measures_election_id;
  END IF;
END $$;

COMMIT;
