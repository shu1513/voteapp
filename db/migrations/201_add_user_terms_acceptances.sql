BEGIN;

-- Acceptance history for registered users.
--
-- users.accepted_terms_version / accepted_terms_at hold ONE version and ONE
-- timestamp, and re-acceptance overwrites them. So the day the terms version
-- is bumped, every user who accepts the new version destroys the evidence that
-- they ever accepted the old one — and a dispute about conduct while the old
-- version was in force has nothing left to point at. This table appends
-- instead: one row per acceptance, kept for the life of the account.
--
-- The users columns stay as they are. They answer "which version is this
-- account on right now", which is what the re-acceptance interstitial reads on
-- every request; this table answers "what has this account ever accepted".
--
-- Anonymous searches are deliberately absent. Nothing about them is stored
-- anywhere, so that people with no account are not tracked — see
-- docs/legal/checkbox-copy.md.

CREATE TABLE public.user_terms_acceptances (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    -- ON DELETE CASCADE, matching every other user-owned table: account
    -- deletion is a hard DELETE and the privacy policy promises the account
    -- and its data go away for real. Keeping acceptance rows past deletion
    -- would be a new retention practice needing its own disclosure, and it is
    -- not what this table is for — the evidence being lost today is lost to
    -- re-acceptance by ACTIVE accounts, not to deletion.
    user_id uuid NOT NULL REFERENCES public.users (id) ON DELETE CASCADE,
    terms_version text NOT NULL,
    -- backfill = reconstructed below from the users columns, where the
    -- original context was never recorded and must not be guessed at.
    context text NOT NULL CHECK (context IN ('registration', 'renewal', 'backfill')),
    accepted_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT user_terms_acceptances_version_present CHECK (btrim(terms_version) <> '')
);

CREATE INDEX idx_user_terms_acceptances_user_id
    ON public.user_terms_acceptances (user_id, accepted_at DESC);

-- "Who had accepted version X, and when" is the question a dispute starts from.
CREATE INDEX idx_user_terms_acceptances_version
    ON public.user_terms_acceptances (terms_version, accepted_at DESC);

-- Backfill before anything can overwrite it. Every acceptance currently on a
-- users row is history that exists exactly once; once that row is overwritten
-- by a version bump it cannot be reconstructed from anywhere.
INSERT INTO public.user_terms_acceptances (user_id, terms_version, context, accepted_at)
SELECT
    id,
    accepted_terms_version,
    'backfill',
    -- accepted_terms_at has been written alongside the version since migration
    -- 149, but the column is nullable for rows that predate it; fall back to
    -- the account's own timestamps rather than stamping now() on old consent.
    COALESCE(accepted_terms_at, updated_at, created_at)
FROM public.users
WHERE accepted_terms_version IS NOT NULL
  AND btrim(accepted_terms_version) <> '';

-- Append-only, with one deliberate exception. UPDATE is rejected outright:
-- evidence that can be silently rewritten is not evidence. DELETE is allowed
-- because account deletion cascades through here, and blocking it would break
-- the deletion the privacy policy promises. Corrections are new rows.
CREATE FUNCTION public.reject_terms_acceptance_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'user_terms_acceptances is append-only';
END;
$$;

CREATE TRIGGER trg_user_terms_acceptances_immutable
BEFORE UPDATE ON public.user_terms_acceptances
FOR EACH ROW EXECUTE FUNCTION public.reject_terms_acceptance_update();

COMMIT;
