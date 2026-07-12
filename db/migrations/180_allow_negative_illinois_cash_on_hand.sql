BEGIN;

-- SBE D-2 Ending Funds Available can legitimately be negative. Keep flow,
-- debt, and outside-spending totals nonnegative while preserving that signed
-- official balance.
ALTER TABLE public.il_candidate_finance_summaries
  DROP CONSTRAINT IF EXISTS il_candidate_finance_summaries_amounts_check,
  ADD CONSTRAINT il_candidate_finance_summaries_amounts_check
    CHECK (
      (total_receipts IS NULL OR total_receipts >= 0)
      AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
      AND (total_disbursements IS NULL OR total_disbursements >= 0)
      AND (debts_owed IS NULL OR debts_owed >= 0)
      AND (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    );

COMMIT;
