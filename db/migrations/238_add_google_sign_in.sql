BEGIN;

-- Google Sign-In (docs/plans/google-sign-in.md). Accounts created via Google
-- carry no password, so the NOT NULL constraint goes. Application-side,
-- login() treats a NULL hash as never-matching (see authService.ts — the
-- dummy-hash constant-time path must not become a real credential).
ALTER TABLE public.users ALTER COLUMN password_hash DROP NOT NULL;

-- Identity key for "Sign in with Google": the ID token's `sub` claim, stable
-- per Google account and never reused. Never key on the Google email — it
-- can change or be recycled while `sub` stays fixed.
ALTER TABLE public.users
    ADD COLUMN google_sub text,
    ADD CONSTRAINT users_google_sub_present CHECK (google_sub IS NULL OR btrim(google_sub) <> '');

-- Partial unique index mirrors uq_users_email_active for consistency
-- (account deletion is a hard DELETE today, so the predicate is
-- future-proofing rather than load-bearing).
CREATE UNIQUE INDEX uq_users_google_sub_active
    ON public.users (google_sub)
    WHERE deleted_at IS NULL;

COMMENT ON COLUMN public.users.google_sub IS
    'Google ID-token sub claim. Stable per Google account; identity key for Sign in with Google. NULL = no Google link.';

COMMIT;
