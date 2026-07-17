-- Candidate withdrawal notifications: a real withdrawal keeps the
-- candidate_elections row (status = 'withdrawn') and notifies followers who
-- opted into election alerts, through the existing digest pipeline. The new
-- event type carries the same payload shape as candidate_future_election
-- (an election_id, no candidate_record_id).

BEGIN;

ALTER TABLE public.user_candidate_follow_notification_events
  DROP CONSTRAINT IF EXISTS chk_user_candidate_follow_notification_events_type;

ALTER TABLE public.user_candidate_follow_notification_events
  ADD CONSTRAINT chk_user_candidate_follow_notification_events_type
  CHECK (event_type IN ('candidate_record_update', 'candidate_future_election', 'candidate_election_withdrawal'));

ALTER TABLE public.user_candidate_follow_notification_events
  DROP CONSTRAINT IF EXISTS chk_user_candidate_follow_notification_events_payload;

ALTER TABLE public.user_candidate_follow_notification_events
  ADD CONSTRAINT chk_user_candidate_follow_notification_events_payload
  CHECK (
    (
      event_type = 'candidate_record_update'
      AND candidate_record_id IS NOT NULL
      AND election_id IS NULL
    )
    OR (
      event_type IN ('candidate_future_election', 'candidate_election_withdrawal')
      AND candidate_record_id IS NULL
      AND election_id IS NOT NULL
    )
  );

CREATE UNIQUE INDEX IF NOT EXISTS uq_ucf_notification_events_withdrawal
  ON public.user_candidate_follow_notification_events (user_id, candidate_id, election_id)
  WHERE event_type = 'candidate_election_withdrawal';

COMMIT;
