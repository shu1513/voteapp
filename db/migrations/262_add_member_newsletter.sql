BEGIN;

-- Operator-sent member newsletters: one-off emails to users with an active
-- monthly membership (Terms 14.5 member communications). Sent by CLI today
-- (scripts/sendMemberNewsletter.ts); a future admin page reuses the same
-- pipeline function. Mirrors the issue-broadcast shape (154).

-- Opt-out, alongside the other email preference columns from 001_init/154.
-- Default true: the newsletter is the membership benefit, and Terms 14.5
-- names it; members can turn it off in Settings or via the one-click
-- unsubscribe link without affecting the membership itself.
ALTER TABLE public.users
  ADD COLUMN email_member_newsletter boolean NOT NULL DEFAULT true;

-- Dedupe log: one row per (newsletter, user) means "this member got this
-- newsletter". newsletter_id is an operator-chosen slug (the message body
-- lives outside the database), so a re-run of the same newsletter resumes
-- instead of double-sending; an id therefore names ONE message — edited
-- content must get a new id. Rows are inserted after a successful send
-- (at-least-once). Deliberately NOT in the notification prune: deleting a
-- row re-arms the dedupe. One row per member per newsletter stays tiny.
CREATE TABLE public.member_newsletter_sends (
  newsletter_id text NOT NULL,
  user_id uuid NOT NULL,
  sent_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pk_member_newsletter_sends
    PRIMARY KEY (newsletter_id, user_id),
  CONSTRAINT fk_member_newsletter_sends_user
    FOREIGN KEY (user_id)
    REFERENCES public.users (id)
    ON DELETE CASCADE,
  CONSTRAINT chk_member_newsletter_sends_newsletter_id
    CHECK (length(trim(newsletter_id)) > 0)
);

CREATE INDEX idx_member_newsletter_sends_user
  ON public.member_newsletter_sends (user_id);

COMMIT;
