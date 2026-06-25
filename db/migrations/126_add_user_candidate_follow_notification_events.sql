BEGIN;

CREATE TABLE IF NOT EXISTS public.user_candidate_follow_notification_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  event_type text NOT NULL,
  candidate_record_id uuid,
  election_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_user_candidate_follow_notification_events_user
    FOREIGN KEY (user_id)
    REFERENCES public.users (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_user_candidate_follow_notification_events_candidate
    FOREIGN KEY (candidate_id)
    REFERENCES public.candidates (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_user_candidate_follow_notification_events_candidate_record
    FOREIGN KEY (candidate_record_id)
    REFERENCES public.candidate_records (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_user_candidate_follow_notification_events_election
    FOREIGN KEY (election_id)
    REFERENCES public.elections (id)
    ON DELETE CASCADE,
  CONSTRAINT chk_user_candidate_follow_notification_events_type
    CHECK (event_type IN ('candidate_record_update', 'candidate_future_election')),
  CONSTRAINT chk_user_candidate_follow_notification_events_payload
    CHECK (
      (
        event_type = 'candidate_record_update'
        AND candidate_record_id IS NOT NULL
        AND election_id IS NULL
      )
      OR (
        event_type = 'candidate_future_election'
        AND candidate_record_id IS NULL
        AND election_id IS NOT NULL
      )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ucf_notification_events_record
  ON public.user_candidate_follow_notification_events (user_id, candidate_record_id)
  WHERE event_type = 'candidate_record_update';

CREATE UNIQUE INDEX IF NOT EXISTS uq_ucf_notification_events_election
  ON public.user_candidate_follow_notification_events (user_id, candidate_id, election_id)
  WHERE event_type = 'candidate_future_election';

CREATE INDEX IF NOT EXISTS idx_ucf_notification_events_user_created
  ON public.user_candidate_follow_notification_events (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ucf_notification_events_candidate_created
  ON public.user_candidate_follow_notification_events (candidate_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ucf_notification_events_record
  ON public.user_candidate_follow_notification_events (candidate_record_id)
  WHERE candidate_record_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ucf_notification_events_election
  ON public.user_candidate_follow_notification_events (election_id)
  WHERE election_id IS NOT NULL;

COMMIT;
