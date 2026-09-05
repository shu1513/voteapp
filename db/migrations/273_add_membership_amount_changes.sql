BEGIN;

-- Member-initiated monthly amount changes (docs/plans/membership-manage-page.md,
-- PR 2). Stripe's hosted portal cannot take a customer-typed amount, so the
-- app swaps the subscription's ad-hoc price itself — never today, always at a
-- future renewal, after a CA BPC §17602(g)(2) advance notice sent 7–30 days
-- before the first charge at the new amount.

-- Stale-write guard for billing_subscriptions: the instant the written state
-- was known to be current at Stripe (a retrieve's start, a mutation's
-- return). Every writer skips its write when the row already carries a later
-- instant, so an older webhook poke landing after a manage-page update can no
-- longer overwrite the newer state. NULL on rows written before this column
-- existed = unknown, always overwritable.
ALTER TABLE public.billing_subscriptions
  ADD COLUMN IF NOT EXISTS stripe_synced_at timestamptz;

-- One row per amount-change request. Rows are never deleted: together with
-- notice_sent_at they are the retained notice/consent evidence §17602(a)(6)
-- wants kept for 3 years, and they hang off billing_subscriptions, which
-- already survives account deletion.
CREATE TABLE IF NOT EXISTS public.billing_subscription_amount_changes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  stripe_subscription_id text NOT NULL
    REFERENCES public.billing_subscriptions(stripe_subscription_id),
  -- Stripe's price at request time (what the next renewal would have billed).
  previous_amount_cents integer NOT NULL,
  new_amount_cents integer NOT NULL CHECK (new_amount_cents >= 500),
  requested_at timestamptz NOT NULL DEFAULT now(),
  -- Set when the Stripe price was swapped (proration_behavior none).
  applied_at timestamptz,
  -- The renewal that first bills the new amount; set together with applied_at.
  effective_at timestamptz,
  -- §17602(g)(2) advance notice emailed. NULL after applied_at = the send
  -- failed; every subscription webhook poke retries it.
  notice_sent_at timestamptz,
  -- Replaced by a newer request, withdrawn by re-saving the current amount,
  -- or the subscription ended before it could apply.
  superseded_at timestamptz,
  CHECK (new_amount_cents <> previous_amount_cents)
);

-- At most one unapplied request per subscription; a newer request supersedes
-- the older one first.
CREATE UNIQUE INDEX IF NOT EXISTS billing_subscription_amount_changes_one_pending_idx
  ON public.billing_subscription_amount_changes (stripe_subscription_id)
  WHERE applied_at IS NULL AND superseded_at IS NULL;

CREATE INDEX IF NOT EXISTS billing_subscription_amount_changes_subscription_idx
  ON public.billing_subscription_amount_changes (stripe_subscription_id, requested_at DESC);

-- Least-privilege grants (docs/postgres-api-role.md): SELECT arrives through
-- the role's default privileges; the service inserts requests and updates
-- their applied/notice/superseded stamps. No DELETE. Guarded because the
-- role does not exist in local dev.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT INSERT, UPDATE ON public.billing_subscription_amount_changes TO voteapp_api;
  END IF;
END $$;

COMMIT;
