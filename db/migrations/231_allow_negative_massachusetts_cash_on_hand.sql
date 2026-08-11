BEGIN;

-- OCPF bank-report YTD rows carry legitimately negative (overdrawn) current
-- cash on hand for live committees. Preserve that signed official balance
-- while keeping flow and outside-spending totals nonnegative (same pattern
-- as 180_allow_negative_illinois_cash_on_hand.sql).
ALTER TABLE public.ma_candidate_finance_summaries
  DROP CONSTRAINT IF EXISTS ma_candidate_finance_summaries_amounts_check,
  ADD CONSTRAINT ma_candidate_finance_summaries_amounts_check
    CHECK (
      (total_receipts IS NULL OR total_receipts >= 0)
      AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
      AND (total_disbursements IS NULL OR total_disbursements >= 0)
      AND (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    );

COMMIT;
