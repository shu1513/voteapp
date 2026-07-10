BEGIN;

-- Database-backed session revocation. Redis sessions capture the user's
-- session_epoch at creation; every authenticated request compares the
-- session's epoch against this column. Password reset/change and logout-all
-- bump the epoch inside the same transaction as the credential change, so
-- old-password sessions die even when the best-effort Redis destroy fails
-- and a concurrent old-password login cannot outlive the reset.
ALTER TABLE public.users
    ADD COLUMN session_epoch integer NOT NULL DEFAULT 1;

COMMENT ON COLUMN public.users.session_epoch IS
    'Session generation counter. Bumped on password reset/change and logout-all; sessions created under an older epoch fail per-request validation.';

COMMIT;
