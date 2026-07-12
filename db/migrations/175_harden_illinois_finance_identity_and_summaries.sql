BEGIN;

ALTER TABLE public.il_candidate_finance_links
  ADD COLUMN IF NOT EXISTS sbe_candidate_id text,
  ADD COLUMN IF NOT EXISTS sbe_district_type text,
  ADD COLUMN IF NOT EXISTS sbe_office text,
  ADD COLUMN IF NOT EXISTS is_at_large boolean;

ALTER TABLE public.il_candidate_finance_links
  DROP CONSTRAINT IF EXISTS il_candidate_finance_links_sbe_candidate_id_check,
  ADD CONSTRAINT il_candidate_finance_links_sbe_candidate_id_check
    CHECK (sbe_candidate_id IS NULL OR btrim(sbe_candidate_id) <> ''),
  DROP CONSTRAINT IF EXISTS il_candidate_finance_links_sbe_district_type_check,
  ADD CONSTRAINT il_candidate_finance_links_sbe_district_type_check
    CHECK (sbe_district_type IS NULL OR btrim(sbe_district_type) <> ''),
  DROP CONSTRAINT IF EXISTS il_candidate_finance_links_sbe_office_check,
  ADD CONSTRAINT il_candidate_finance_links_sbe_office_check
    CHECK (sbe_office IS NULL OR btrim(sbe_office) <> '');

ALTER TABLE public.il_candidate_finance_summaries
  ADD COLUMN IF NOT EXISTS debts_owed numeric(16,2);

ALTER TABLE public.il_candidate_finance_summaries
  DROP CONSTRAINT IF EXISTS il_candidate_finance_summaries_amounts_check,
  ADD CONSTRAINT il_candidate_finance_summaries_amounts_check
    CHECK (
      (total_receipts IS NULL OR total_receipts >= 0)
      AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
      AND (total_disbursements IS NULL OR total_disbursements >= 0)
      AND (cash_on_hand IS NULL OR cash_on_hand >= 0)
      AND (debts_owed IS NULL OR debts_owed >= 0)
      AND (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    );

COMMIT;
