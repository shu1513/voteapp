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
--
-- KNOWN GAP, stated plainly because the table must not be described as more
-- than it is: this preserves history from here on, it does not reconstruct
-- history that is already gone. CURRENT_TERMS_VERSION was '1.0' from
-- 2026-07-03 and became '1.1' on 2026-07-18, the same day the re-acceptance
-- interstitial shipped. Any account that accepted 1.0 and then re-accepted
-- 1.1 had its 1.0 acceptance overwritten in place, and the backfill below can
-- only see what survived — the current version. For those accounts this table
-- answers "what has this account accepted since 2026-07-31", not "ever". If a
-- production backup from before the bump is still within retention, the 1.0
-- acceptances can be recovered from it and inserted separately; nothing here
-- should invent them.

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
--
-- Only rows carrying a real accepted_terms_at are copied. Falling back to
-- updated_at or created_at would be worse than skipping: updated_at moves on
-- any profile edit and created_at predates the clickwrap, so either would turn
-- an unknown consent time into an exact-looking one on a row whose whole
-- purpose is to be relied on. An acceptance whose time we do not know is
-- reported below and left out.
DO $$
DECLARE
    undated bigint;
BEGIN
    SELECT count(*) INTO undated
    FROM public.users
    WHERE accepted_terms_version IS NOT NULL
      AND btrim(accepted_terms_version) <> ''
      AND accepted_terms_at IS NULL;

    IF undated > 0 THEN
        RAISE NOTICE
            'user_terms_acceptances: skipped % account(s) recording a terms version with no acceptance timestamp. Their acceptance time is unknown and was not invented; recover it from a backup if the history is needed.',
            undated;
    END IF;
END;
$$;

INSERT INTO public.user_terms_acceptances (user_id, terms_version, context, accepted_at)
SELECT
    id,
    accepted_terms_version,
    'backfill',
    accepted_terms_at
FROM public.users
WHERE accepted_terms_version IS NOT NULL
  AND btrim(accepted_terms_version) <> ''
  AND accepted_terms_at IS NOT NULL;

-- Append-only. UPDATE is rejected outright: evidence that can be silently
-- rewritten is not evidence, and corrections belong in new rows.
--
-- DELETE has to stay possible for exactly one case — account deletion hard-
-- deletes the users row and cascades through here — but a blanket allowance
-- would leave the evidence removable by any stray statement. The parent check
-- separates the two: on a cascade the users row is already gone by the time
-- this fires, while a direct DELETE against this table still sees it. So the
-- cascade passes and everything else raises.
--
-- This is a guardrail against accident and casual tampering, not tamper-proof
-- storage. Anyone able to drop the trigger can still delete rows; the point is
-- that doing so has to be deliberate rather than a stray WHERE clause.
CREATE FUNCTION public.reject_terms_acceptance_rewrite()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'user_terms_acceptances is append-only';
    END IF;

    IF EXISTS (SELECT 1 FROM public.users WHERE id = OLD.user_id) THEN
        RAISE EXCEPTION 'user_terms_acceptances rows are removed only by deleting the account';
    END IF;

    RETURN OLD;
END;
$$;

CREATE TRIGGER trg_user_terms_acceptances_append_only
BEFORE UPDATE OR DELETE ON public.user_terms_acceptances
FOR EACH ROW EXECUTE FUNCTION public.reject_terms_acceptance_rewrite();

COMMIT;
