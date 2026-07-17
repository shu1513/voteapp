BEGIN;

-- Election-result alerts: one row per (user, election) meaning "an election
-- in one of this user's districts has a decisive result and the user has the
-- email digest on". Created by the election-result writer in the same
-- transaction as the result rows, only for decisive outcomes
-- (won/advanced/runoff, passed/failed); delivered by the election-result
-- alert sender, which stamps notified_at after a successful send. The
-- (user, election) pair is unique for the row's lifetime, so a certified
-- write after an election-night write never re-notifies. Channel-agnostic by
-- design, mirroring user_district_notification_events.

CREATE TABLE IF NOT EXISTS public.user_election_result_notification_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  election_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  notified_at timestamptz,
  CONSTRAINT fk_user_election_result_notification_events_user
    FOREIGN KEY (user_id)
    REFERENCES public.users (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_user_election_result_notification_events_election
    FOREIGN KEY (election_id)
    REFERENCES public.elections (id)
    ON DELETE CASCADE,
  -- Dedupe: result upserts (corrections, the certified pass after election
  -- night) retry the same pair; ON CONFLICT DO NOTHING relies on this.
  CONSTRAINT uq_user_election_result_notification_events_user_election
    UNIQUE (user_id, election_id)
);

CREATE INDEX IF NOT EXISTS idx_uern_events_unnotified
  ON public.user_election_result_notification_events (user_id, created_at)
  WHERE notified_at IS NULL;

-- FK cascade support: election deletions must not seq-scan the events table.
CREATE INDEX IF NOT EXISTS idx_uern_events_election
  ON public.user_election_result_notification_events (election_id);

COMMIT;
