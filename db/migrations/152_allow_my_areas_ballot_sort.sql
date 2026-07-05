BEGIN;

-- New ballot sort mode "my_areas": orders elections by how strongly their
-- research areas (office links + ballot-measure tags) match the user's saved
-- research-area preferences. Keep this list in sync with
-- BALLOT_SUMMARY_SORTS in backend/src/pipeline/address/ballotElectionOrdering.ts.
ALTER TABLE public.user_ballot_preferences
  DROP CONSTRAINT user_ballot_preferences_sort_check;

ALTER TABLE public.user_ballot_preferences
  ADD CONSTRAINT user_ballot_preferences_sort_check
  CHECK (sort IN ('vote_power', 'soonest', 'district_size', 'district_size_smallest', 'my_areas'));

COMMIT;
