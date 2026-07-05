BEGIN;

-- Operator-sent issue broadcasts: targeted one-off emails to users who saved
-- specific research areas (e.g. an environmental nonprofit announcement to
-- everyone with the environment area saved). Sent by CLI today; a future
-- admin page reuses the same pipeline function.

-- Opt-in, alongside the other email preference columns from 001_init.
ALTER TABLE public.users
  ADD COLUMN email_issue_updates boolean NOT NULL DEFAULT true;

-- Dedupe log: one row per (broadcast, user) means "this user got this
-- broadcast". broadcast_id is an operator-chosen slug (the message body lives
-- outside the database), so a re-run of the same broadcast resumes instead of
-- double-sending. Rows are inserted after a successful send (at-least-once).
-- Deliberately NOT in the notification prune: deleting a row re-arms the
-- dedupe, and an operator may legitimately re-run an old broadcast id months
-- later. One row per user per broadcast stays tiny.
CREATE TABLE public.issue_broadcast_sends (
  broadcast_id text NOT NULL,
  user_id uuid NOT NULL,
  sent_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pk_issue_broadcast_sends
    PRIMARY KEY (broadcast_id, user_id),
  CONSTRAINT fk_issue_broadcast_sends_user
    FOREIGN KEY (user_id)
    REFERENCES public.users (id)
    ON DELETE CASCADE,
  CONSTRAINT chk_issue_broadcast_sends_broadcast_id
    CHECK (length(trim(broadcast_id)) > 0)
);

CREATE INDEX idx_issue_broadcast_sends_user
  ON public.issue_broadcast_sends (user_id);

COMMIT;
