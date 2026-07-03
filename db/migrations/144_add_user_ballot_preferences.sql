BEGIN;

CREATE TABLE IF NOT EXISTS public.user_ballot_preferences (
  user_id uuid PRIMARY KEY,
  sort text NOT NULL DEFAULT 'vote_power'
    CHECK (sort IN ('vote_power', 'soonest', 'district_size', 'district_size_smallest')),
  followed_first boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_user_ballot_preferences_user
    FOREIGN KEY (user_id)
    REFERENCES public.users (id)
    ON DELETE CASCADE
);

COMMENT ON TABLE public.user_ballot_preferences IS
  'Per-user default ordering for the elections list; absent row means the application defaults (vote_power, followed_first=true).';

COMMIT;
