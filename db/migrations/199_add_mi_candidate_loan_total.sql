-- Self-funded Michigan candidates (e.g. $30.75M of candidate loans against
-- $28.7K of donations) are invisible on the finance card because "Raised"
-- deliberately excludes loans. Store the loan total explicitly so the card
-- can show it as its own stat. NULL means "not computed" (pre-backfill rows);
-- 0 means "source covers loans and this candidate has none".
ALTER TABLE public.mi_candidate_finance_summaries
  ADD COLUMN candidate_loan_total numeric(16, 2);

ALTER TABLE public.mi_candidate_finance_summaries
  ADD CONSTRAINT mi_candidate_finance_summaries_loan_total_check
    CHECK (candidate_loan_total IS NULL OR candidate_loan_total >= 0);
