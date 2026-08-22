BEGIN;

-- Support payments / membership billing (docs/plans/membership-contributions.md).
-- Stripe holds all card data; these tables hold references and amounts only.

-- Indirection that survives account deletion: users.id is hard-deleted with
-- ON DELETE CASCADE everywhere else, but payment records are accounting data
-- we retain (privacy policy Section 4). On deletion user_id nulls out and the
-- payment history below stays, unlinked from any account. The retained Stripe
-- ids remain resolvable through the Stripe dashboard, so these rows are
-- pseudonymous business records, not anonymous data.
CREATE TABLE IF NOT EXISTS public.billing_customers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid UNIQUE REFERENCES public.users(id) ON DELETE SET NULL,
  stripe_customer_id text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Ledger: one row per successful charge, monthly and one-time alike.
CREATE TABLE IF NOT EXISTS public.billing_payments (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  billing_customer_id uuid NOT NULL REFERENCES public.billing_customers(id),
  amount_cents integer NOT NULL CHECK (amount_cents > 0),
  currency text NOT NULL DEFAULT 'usd',
  kind text NOT NULL CHECK (kind IN ('one_time', 'monthly')),
  -- Idempotency key for webhook retries: payment_intent id (one-time) or
  -- invoice id (monthly). Inserts are ON CONFLICT DO NOTHING on this column.
  stripe_payment_ref text NOT NULL UNIQUE,
  -- Refund join key, populated for BOTH kinds: refund events identify a
  -- charge/payment_intent, never an invoice. One-time rows repeat
  -- stripe_payment_ref; monthly rows resolve it from the invoice's payments
  -- at insert time.
  stripe_payment_intent_id text NOT NULL UNIQUE,
  -- Stripe's cumulative amount_refunded, assigned absolutely (never
  -- incremented) by the charge.refunded handler.
  refunded_amount_cents integer NOT NULL DEFAULT 0
    CHECK (refunded_amount_cents >= 0),
  refunded_at timestamptz,
  paid_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (refunded_amount_cents <= amount_cents)
);

CREATE INDEX IF NOT EXISTS billing_payments_customer_paid_at_idx
  ON public.billing_payments (billing_customer_id, paid_at DESC);

-- One row PER SUBSCRIPTION, append-only across a customer's lifetime: a
-- cancel + resubscribe adds a row, never overwrites one. This preserves
-- consent evidence — CA BPC §17602 wants consent verification retained
-- (3 years / 1 year past termination), and a row-per-customer shape would
-- overwrite the prior subscription's consent pointer on resubscribe.
-- "Current membership" = the one nonterminal row.
CREATE TABLE IF NOT EXISTS public.billing_subscriptions (
  stripe_subscription_id text PRIMARY KEY,
  billing_customer_id uuid NOT NULL REFERENCES public.billing_customers(id),
  -- Consent evidence pointer (CA auto-renewal law): the Checkout Session
  -- that carried the renewal-terms consent checkbox. Stripe retains the
  -- session (with its consent record) retrievably; we keep the id.
  stripe_checkout_session_id text,
  -- §17602 post-purchase acknowledgment: set when the membership-started
  -- email (renewal terms + how to cancel) was sent. NULL = not yet sent;
  -- the next subscription webhook poke retries the send.
  acknowledgment_sent_at timestamptz,
  monthly_amount_cents integer NOT NULL CHECK (monthly_amount_cents >= 500),
  -- Raw Stripe status verbatim, so new Stripe statuses never break the
  -- schema. Member treatment = 'active' or 'past_due'.
  stripe_status text NOT NULL,
  cancel_at_period_end boolean NOT NULL DEFAULT false,
  current_period_end timestamptz,
  started_at timestamptz NOT NULL,
  canceled_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- At most one live subscription per customer, enforced by the DB itself.
-- The webhook duplicate-subscription guard cancels the loser of a
-- concurrent-checkout race; this index is the backstop that makes the
-- invariant impossible to violate.
CREATE UNIQUE INDEX IF NOT EXISTS billing_subscriptions_one_live_per_customer_idx
  ON public.billing_subscriptions (billing_customer_id)
  WHERE stripe_status NOT IN ('canceled', 'incomplete_expired');

DROP TRIGGER IF EXISTS trg_billing_subscriptions_set_updated_at
  ON public.billing_subscriptions;
CREATE TRIGGER trg_billing_subscriptions_set_updated_at
BEFORE UPDATE ON public.billing_subscriptions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
