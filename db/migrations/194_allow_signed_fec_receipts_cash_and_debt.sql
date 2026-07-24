BEGIN;

-- OpenFEC candidate aggregates can report negative receipts, cash on hand, and
-- debt after amendments or other corrections. Preserve those official signed
-- values while keeping the remaining finance totals constrained.
ALTER TABLE public.candidate_finance_summaries
  DROP CONSTRAINT IF EXISTS chk_candidate_finance_summaries_nonnegative_amounts,
  ADD CONSTRAINT chk_candidate_finance_summaries_nonnegative_amounts
    CHECK (
      (total_disbursements IS NULL OR total_disbursements >= 0)
      AND (individual_itemized_total IS NULL OR individual_itemized_total >= 0)
      AND (individual_unitemized_total IS NULL OR individual_unitemized_total >= 0)
      AND (other_committee_contributions IS NULL OR other_committee_contributions >= 0)
      AND (transfers_from_affiliated_committees IS NULL OR transfers_from_affiliated_committees >= 0)
      AND (outside_support_total IS NULL OR outside_support_total >= 0)
      AND (outside_oppose_total IS NULL OR outside_oppose_total >= 0)
    );

COMMIT;
