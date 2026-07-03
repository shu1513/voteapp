BEGIN;

-- Delivery marker for the candidate-follow digest sender. NULL means the
-- event has not been included in a sent digest email yet; the sender sets it
-- after a successful send (or when it resolves an event whose follow no
-- longer exists). The pruner continues to delete by created_at age
-- regardless of delivery state.
ALTER TABLE public.user_candidate_follow_notification_events
  ADD COLUMN IF NOT EXISTS notified_at timestamptz;

CREATE INDEX IF NOT EXISTS idx_ucf_notification_events_unnotified
  ON public.user_candidate_follow_notification_events (user_id, created_at)
  WHERE notified_at IS NULL;

COMMIT;
