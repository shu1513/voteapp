-- Denver Phase 4: publish the two funding sources the card already renders as
-- their own stats.
--
-- Both are money the committee can spend that is deliberately NOT counted in
-- the published "raised" figure (direct_contribution_total = private donor
-- money only):
--   * public_funds_received — Denver Fair Elections Fund matching, the
--     `Fair Elections Payments` transaction subtype. It is a large share of a
--     qualified Denver campaign's funding (Padgett, cycle 26: $36,855.00 of
--     $53,782.99 in receipts), so leaving it out of the payload hid most of
--     the money behind a sentence of prose.
--   * loans_received — the `Loan` transaction subtype, candidate self-funding
--     (verified live 2026-08-13: the overview's private figure includes loans
--     while getContributionsTotalByCommittee excludes them).
--
-- Nullable, like every other money column here: null means "not reported",
-- 0.00 means "reported as none".

ALTER TABLE public.denver_candidate_finance_summaries
  ADD COLUMN loans_received numeric(16,2),
  ADD COLUMN public_funds_received numeric(16,2);
