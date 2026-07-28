-- Reconcile schema drift between databases built from this migration set and
-- the long-lived databases (local dev, production seeded from a local pg_dump).
--
-- Two independent causes, both invisible until you migrate an empty database:
--
--   1. Nine migrations were applied locally and then renamed/renumbered on disk
--      (e.g. 195_add_county_council_chairman_alias.sql ->
--      196_add_county_council_chair_alias.sql). schema_migrations still carries
--      the old filenames, so the long-lived databases hold changes that no
--      current file reproduces. candidate_records.updated_at and its trigger
--      came in that way: candidateRecordStore.ts writes `updated_at = now()`,
--      so a fresh database built via the "Fresh-DB alternative" path in
--      docs/deploy-render.md breaks on the first candidate-record write.
--
--   2. Three finance migrations were edited in place after they had already
--      been applied locally (the "Harden ..." review-fix commits). The fix
--      reached databases created afterwards but never the long-lived ones,
--      which are therefore running the pre-review constraints. The Tennessee
--      one is a live defect: tennesseeCandidateFinanceAutoLink.ts writes
--      link_status = 'ambiguous', which the stale CHECK rejects.
--
-- Everything below is idempotent and converges both directions: on a database
-- built from files it is a no-op except for the drift the edited files left
-- behind; on local/production it applies the missing changes.
--
-- Office-catalog rows from the removed alias migrations are deliberately NOT
-- replayed here. A migration cannot restore them: `offices` is populated by
-- elections:offices:seed, which runs AFTER migrations, so an alias INSERT that
-- selects its office_id from `offices` matches nothing on a fresh database and
-- silently inserts zero rows (196_add_county_council_chair_alias.sql has
-- exactly that problem — it is a no-op on a fresh database, and the alias only
-- exists because seedOffices.ts also carries it). The two alias rows that were
-- genuinely lost with the removed migrations, "County Council Chairman" and
-- "Soil & Water Commission", are therefore restored in
-- backend/src/scripts/seedOffices.ts instead.

-- 1. candidate_records.updated_at (+ trigger) — missing from the file set.
-- Added nullable and backfilled from created_at rather than defaulted straight
-- to now(): stamping pre-existing rows with the migration timestamp would
-- assert a "last updated" event that never happened. The SET DEFAULT/SET NOT
-- NULL pair also converges a database where the column exists but is nullable
-- or defaultless, which a bare ADD COLUMN IF NOT EXISTS would skip silently.
--
-- Order matters: set_updated_at() is a BEFORE UPDATE trigger that overwrites
-- NEW.updated_at with now(), so the backfill has to run while the trigger is
-- absent or it silently degrades to now() on any replay.
DROP TRIGGER IF EXISTS trg_candidate_records_set_updated_at ON public.candidate_records;

ALTER TABLE public.candidate_records
  ADD COLUMN IF NOT EXISTS updated_at timestamptz;

UPDATE public.candidate_records
  SET updated_at = created_at
  WHERE updated_at IS NULL;

ALTER TABLE public.candidate_records
  ALTER COLUMN updated_at SET DEFAULT now(),
  ALTER COLUMN updated_at SET NOT NULL;

CREATE TRIGGER trg_candidate_records_set_updated_at
  BEFORE UPDATE ON public.candidate_records
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 2. Tennessee link_status — allow 'ambiguous'. Widening, so no row can fail.
ALTER TABLE public.tn_candidate_finance_links
  DROP CONSTRAINT IF EXISTS tn_candidate_finance_links_status_check;
ALTER TABLE public.tn_candidate_finance_links
  ADD CONSTRAINT tn_candidate_finance_links_status_check
    CHECK (link_status IN ('active', 'inactive', 'ambiguous'));

-- 3. Tennessee summaries lookup index — dropped from 125 during review because
--    it duplicates the (link_id, election_year) unique index (btree scans
--    backwards, so the DESC ordering buys nothing). Still present locally.
DROP INDEX IF EXISTS public.tn_candidate_finance_summaries_lookup_idx;

-- 4. Louisiana summary amounts — cash_on_hand may legitimately be negative
--    (committees carrying debt), so the review dropped it from the CHECK.
--    Also widening.
ALTER TABLE public.la_candidate_finance_summaries
  DROP CONSTRAINT IF EXISTS la_candidate_finance_summaries_amounts_check;
ALTER TABLE public.la_candidate_finance_summaries
  ADD CONSTRAINT la_candidate_finance_summaries_amounts_check
    CHECK (
      (total_receipts IS NULL OR total_receipts >= 0)
      AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
      AND (total_disbursements IS NULL OR total_disbursements >= 0)
      AND (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    );

-- 5. Minnesota breakdown constraint names — 134 was edited to use the mn_cff_
--    prefix so the identifiers stop hitting Postgres' 63-char truncation
--    (..._committee_id_chec, ..._support_oppose_ch). Rename rather than
--    drop/add so the constraints are never absent mid-migration.
-- to_regclass rather than a ::regclass cast: the cast raises undefined_table
-- instead of yielding a false EXISTS if the table is ever absent.
DO $$
DECLARE
  breakdowns regclass :=
    to_regclass('public.mn_candidate_finance_outside_group_breakdowns');
BEGIN
  IF breakdowns IS NULL THEN
    RETURN;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = breakdowns
      AND conname = 'mn_candidate_finance_outside_group_breakdowns_committee_id_chec'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = breakdowns
      AND conname = 'mn_cff_outside_group_breakdowns_committee_id_check'
  ) THEN
    ALTER TABLE public.mn_candidate_finance_outside_group_breakdowns
      RENAME CONSTRAINT mn_candidate_finance_outside_group_breakdowns_committee_id_chec
      TO mn_cff_outside_group_breakdowns_committee_id_check;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = breakdowns
      AND conname = 'mn_candidate_finance_outside_group_breakdowns_support_oppose_ch'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = breakdowns
      AND conname = 'mn_cff_outside_group_breakdowns_support_oppose_check'
  ) THEN
    ALTER TABLE public.mn_candidate_finance_outside_group_breakdowns
      RENAME CONSTRAINT mn_candidate_finance_outside_group_breakdowns_support_oppose_ch
      TO mn_cff_outside_group_breakdowns_support_oppose_check;
  END IF;
END
$$;
