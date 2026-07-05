BEGIN;

-- Migration 152 made the absent-row default personalized: a user with saved
-- research areas but no row here defaults to the my_areas sort (see
-- getUserBallotPreferences). The table comment from migration 144 still
-- described the old unconditional vote_power default; applied migrations are
-- checksummed, so the correction lands as its own migration.
COMMENT ON TABLE public.user_ballot_preferences IS
  'Per-user default ordering for the elections list; absent row means the application defaults: sort my_areas when the user has saved research-area preferences, vote_power otherwise, followed_first=true either way.';

COMMIT;
