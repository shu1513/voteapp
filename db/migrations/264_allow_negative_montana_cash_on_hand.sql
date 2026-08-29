-- Cash on hand is a signed BALANCE, not a flow: an indebted Montana campaign
-- legitimately reports negative cash (live-hit on two 2026 filers in the
-- Phase 3 run: derived ending balances of -$18.52 and -$173.80, both from
-- clean cent-exact chains), and the snapshot writer accepts it as of the
-- same change set. This rebuilds the amounts check with cash_on_hand
-- unconstrained below zero; every flow column stays nonnegative — a
-- negative flow still indicates corrupted source data. GA/MA/IL precedent
-- (migrations 180, 231, 232).
ALTER TABLE mt_candidate_finance_summaries
  DROP CONSTRAINT mt_candidate_finance_summaries_amounts_check;

ALTER TABLE mt_candidate_finance_summaries
  ADD CONSTRAINT mt_candidate_finance_summaries_amounts_check CHECK (
    (total_receipts IS NULL OR total_receipts >= 0)
    AND (direct_contribution_total IS NULL OR direct_contribution_total >= 0)
    AND (total_disbursements IS NULL OR total_disbursements >= 0)
    AND (outside_support_total IS NULL OR outside_support_total >= 0)
    AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
  );
