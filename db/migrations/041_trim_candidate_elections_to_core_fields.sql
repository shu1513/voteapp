BEGIN;

ALTER TABLE public.candidate_elections
  DROP COLUMN IF EXISTS votes_received,
  DROP COLUMN IF EXISTS vote_percentage,
  DROP COLUMN IF EXISTS total_raised,
  DROP COLUMN IF EXISTS total_spent,
  DROP COLUMN IF EXISTS cash_on_hand,
  DROP COLUMN IF EXISTS small_donor_percentage,
  DROP COLUMN IF EXISTS top_donors,
  DROP COLUMN IF EXISTS fec_filing_url,
  DROP COLUMN IF EXISTS finance_sources;

COMMIT;
