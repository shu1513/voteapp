BEGIN;

-- New-election alerts: one row per (user, election) meaning "a new election
-- appeared in one of this user's districts and the user has
-- email_new_election_alerts on". Created by the elections writer on fresh
-- inserts of future-dated elections; delivered by the new-election alert
-- sender, which stamps notified_at after a successful send. Channel-agnostic
-- by design: a later mobile-push consumer reads the same rows.

CREATE TABLE IF NOT EXISTS public.user_district_notification_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  election_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  notified_at timestamptz,
  CONSTRAINT fk_user_district_notification_events_user
    FOREIGN KEY (user_id)
    REFERENCES public.users (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_user_district_notification_events_election
    FOREIGN KEY (election_id)
    REFERENCES public.elections (id)
    ON DELETE CASCADE,
  -- Dedupe: re-running the writer (upserts) or re-arming after a prune may
  -- try the same pair again; ON CONFLICT DO NOTHING relies on this.
  CONSTRAINT uq_user_district_notification_events_user_election
    UNIQUE (user_id, election_id)
);

CREATE INDEX IF NOT EXISTS idx_udn_events_unnotified
  ON public.user_district_notification_events (user_id, created_at)
  WHERE notified_at IS NULL;

-- FK cascade support: election deletions must not seq-scan the events table.
CREATE INDEX IF NOT EXISTS idx_udn_events_election
  ON public.user_district_notification_events (election_id);

COMMIT;
