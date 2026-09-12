BEGIN;

-- The "soonest" ballot sort is retired: election date is now the fixed outer
-- order of every sort (earliest date first), and the chosen sort only orders
-- the races within a date — so "soonest first" no longer changes anything.
-- Saved rows move to the default. Keep this list in sync with
-- SAVEABLE_BALLOT_PREFERENCE_SORTS in
-- backend/src/pipeline/address/ballotElectionOrdering.ts.
UPDATE public.user_ballot_preferences
SET sort = 'vote_power'
WHERE sort = 'soonest';

ALTER TABLE public.user_ballot_preferences
  DROP CONSTRAINT user_ballot_preferences_sort_check;

ALTER TABLE public.user_ballot_preferences
  ADD CONSTRAINT user_ballot_preferences_sort_check
  CHECK (sort IN ('vote_power', 'district_size', 'district_size_smallest', 'my_areas'));

COMMIT;
