import type { Pool } from "pg";
import type Stripe from "stripe";

import {
  MEMBERSHIP_CHECKOUT_MAX_AMOUNT_CENTS,
  MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS,
} from "../apiValidation.js";
import { RequestValidationError } from "../../utils/requestValidationError.js";

// Support payments / membership (docs/plans/membership-contributions.md).
// Stripe holds all card data; our tables hold references and amounts only.
// Every webhook handler is idempotent and treats the event as a poke: the
// fetched current object (or Stripe's cumulative figure) is the truth, never
// the event's snapshot, so out-of-order and duplicate deliveries are harmless.

// Any status outside these can still charge (even `incomplete` can activate
// within its 23-hour window), so guards and the deletion precondition treat
// everything else as live.
const TERMINAL_SUBSCRIPTION_STATUSES = ["canceled", "incomplete_expired"] as const;

export type MembershipKind = "one_time" | "monthly";

export type MembershipCheckoutInput = {
  kind: MembershipKind;
  amount_cents: number;
};

export type MembershipAmountInput = {
  amount_cents: number;
};

export type MembershipStatusResult = {
  enabled: true;
  membership: {
    stripe_status: string;
    /** What the current billing period costs. While an applied amount
     * change waits for its renewal, Stripe already carries the new price but
     * this stays the old one — the member has not been charged it yet. */
    monthly_amount_cents: number;
    cancel_at_period_end: boolean;
    current_period_end: string | null;
    started_at: string;
    /** A requested amount change that has not billed yet
     * (docs/plans/membership-manage-page.md). `applied` = the Stripe price
     * is already swapped and the notice sent, so `starts_at` is firm;
     * otherwise `starts_at` is the projected renewal (null when the period
     * end is unknown). */
    pending_amount_change: {
      new_amount_cents: number;
      starts_at: string | null;
      applied: boolean;
    } | null;
  } | null;
  total_net_cents: number;
  payments: {
    amount_cents: number;
    refunded_amount_cents: number;
    kind: MembershipKind;
    currency: string;
    paid_at: string;
  }[];
};

export type MembershipStartedEmailInput = {
  email: string;
  monthlyAmountCents: number;
};

/** Emails for member-initiated changes (docs/plans/membership-manage-page.md).
 * `canceled` / `resumed` are courtesy confirmations: best-effort, a failed
 * send logs and never fails the action. `amount_notice` is the CA BPC
 * §17602(g)(2) advance notice of a fee change, sent 7–30 days before the
 * first charge at the new amount: a failed send stays unstamped and every
 * subscription webhook poke retries it, like the start acknowledgment. */
export type MembershipChangedEmailInput =
  | { kind: "canceled"; email: string; endsAt: Date | null }
  | { kind: "resumed"; email: string; monthlyAmountCents: number; renewsAt: Date | null }
  | { kind: "amount_notice"; email: string; newAmountCents: number; startsAt: Date };

export type MembershipPortalInput = {
  flow: "payment_method_update" | null;
};

/** The slice of the Stripe SDK the service uses; injected so tests run
 * without network. The real `Stripe` instance satisfies it structurally. */
export type MembershipStripeClient = {
  checkout: {
    sessions: {
      create(params: Stripe.Checkout.SessionCreateParams): Promise<Stripe.Checkout.Session>;
    };
  };
  billingPortal: {
    sessions: {
      create(params: Stripe.BillingPortal.SessionCreateParams): Promise<Stripe.BillingPortal.Session>;
    };
  };
  customers: {
    create(params: Stripe.CustomerCreateParams): Promise<Stripe.Customer>;
    update(id: string, params: Stripe.CustomerUpdateParams): Promise<Stripe.Customer>;
  };
  subscriptions: {
    retrieve(id: string): Promise<Stripe.Subscription>;
    update(id: string, params: Stripe.SubscriptionUpdateParams): Promise<Stripe.Subscription>;
    cancel(id: string): Promise<Stripe.Subscription>;
  };
  invoicePayments: {
    list(params: Stripe.InvoicePaymentListParams): Promise<{ data: Stripe.InvoicePayment[] }>;
  };
  webhooks: {
    constructEvent(payload: string | Buffer, header: string, secret: string): Stripe.Event;
  };
};

export type MembershipServiceOptions = {
  db: Pick<Pool, "query">;
  stripe: MembershipStripeClient;
  webhookSecret: string;
  /** The one dashboard-created Product shared by both payment kinds. */
  membershipProductId: string;
  /** Site origin for Checkout redirects and the terms link, no trailing slash. */
  publicBaseUrl: string;
  /** §17602 post-purchase acknowledgment sender. null = sending disabled
   * (logged per attempt; boot fails fast before this happens in production);
   * the NULL acknowledgment_sent_at keeps retrying via later subscription
   * pokes once a sender exists. A configured sender that THROWS fails the
   * webhook retryably so Stripe redelivers. */
  sendMembershipStartedEmail: ((input: MembershipStartedEmailInput) => Promise<void>) | null;
  /** Cancel / resume confirmations and the amount-change notice. null =
   * sending disabled (the notice then stays unstamped and logged, like the
   * start acknowledgment). */
  sendMembershipChangedEmail: ((input: MembershipChangedEmailInput) => Promise<void>) | null;
};

export class MembershipServiceError extends Error {
  constructor(
    readonly code:
      | "membership_exists"
      | "no_billing_account"
      | "user_not_found"
      | "subscription_cancel_failed"
      // Manage-page actions (docs/plans/membership-manage-page.md):
      | "no_membership"
      | "membership_pending"
      | "membership_conflict"
      | "membership_update_failed",
    message: string
  ) {
    super(message);
    this.name = "MembershipServiceError";
  }
}

/** Thrown when a webhook event cannot be applied YET (e.g. a refund arriving
 * before its ledger row, an invoice caught mid-settlement). Maps to a 5xx so
 * Stripe redelivers; every such condition resolves once the earlier event
 * lands, because all charges on this account come from our checkouts. */
export class MembershipWebhookRetryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MembershipWebhookRetryError";
  }
}

export type MembershipService = {
  getMembership(userId: string): Promise<MembershipStatusResult>;
  createCheckoutSession(userId: string, input: MembershipCheckoutInput): Promise<{ url: string }>;
  /** null = the user has no billing customer yet (404). `flow` deep-links
   * into one portal flow and returns to the membership page; without it the
   * general portal opens and returns to Settings. */
  createPortalSession(userId: string, input?: MembershipPortalInput): Promise<{ url: string } | null>;
  /** Period-end cancel of the live subscription (set-this-state: already
   * canceling → sync + return). Throws no_membership / membership_pending /
   * membership_conflict. Returns the fresh status. */
  cancelMembership(userId: string): Promise<MembershipStatusResult>;
  /** Clears a scheduled period-end cancel (set-this-state: not canceling →
   * sync + return). Throws no_membership / membership_conflict /
   * membership_update_failed. Returns the fresh status. */
  resumeMembership(userId: string): Promise<MembershipStatusResult>;
  /** Records a request to bill a new monthly amount from a future renewal
   * and applies it right away when the §17602(g)(2) notice window allows
   * (set-this-state: the current amount withdraws a pending request; a
   * repeat of a pending request is a no-op). Nothing is charged today.
   * Throws no_membership / membership_pending / membership_conflict.
   * Returns the fresh status. */
  changeMonthlyAmount(userId: string, input: MembershipAmountInput): Promise<MembershipStatusResult>;
  handleWebhookEvent(input: { rawBody: Buffer; signatureHeader: string | null }): Promise<"ok" | "bad_signature">;
  /** Pushes the user's current account email onto their Stripe customer, so
   * Checkout prefills and Stripe receipts follow an email change. Called
   * best-effort from the verified email-change flow; no-op without a billing
   * customer. */
  syncCustomerEmail(userId: string): Promise<void>;
  /** Account-deletion precondition: cancels any nonterminal subscription at
   * Stripe (immediately, not period-end). Returns true when a subscription
   * was actually canceled — the caller logs an inconsistent half-state if
   * the deletion then fails. Throws a retryable subscription_cancel_failed
   * when Stripe is unreachable — the caller must fail the deletion; cancel
   * is idempotent so retrying is always safe. */
  cancelSubscriptionsForAccountDeletion(userId: string): Promise<boolean>;
};

type BillingCustomerRow = {
  id: string;
  user_id: string | null;
  stripe_customer_id: string;
};

function epochToDate(epochSeconds: number): Date {
  return new Date(epochSeconds * 1000);
}

function toIsoString(value: Date | string): string {
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}

function formatUsd(amountCents: number): string {
  return `$${(amountCents / 100).toFixed(2)}`;
}

function idOf(value: string | { id: string } | null | undefined): string | null {
  if (typeof value === "string") {
    return value;
  }
  return value?.id ?? null;
}

function isTerminalStatus(status: string): boolean {
  return (TERMINAL_SUBSCRIPTION_STATUSES as readonly string[]).includes(status);
}

/** Member treatment: the subscription renews and can be re-priced. */
function isBillableStatus(status: string): boolean {
  return status === "active" || status === "past_due";
}

/** A subscription object with the instant its state was known to be current
 * at Stripe: a retrieve's START (the state is at least that fresh), a
 * mutation's RETURN (Stripe had applied it by then). billing_subscriptions
 * .stripe_synced_at keeps the latest such instant, and every writer skips a
 * write carrying an older one — so a webhook poke that read Stripe before a
 * manage-page mutation can no longer overwrite the mutation's result. A poke
 * that read between a mutation's apply and its return loses to the mutation
 * with an equal-or-newer state; the mutation's own webhook re-syncs. */
type FetchedSubscription = { subscription: Stripe.Subscription; syncedAt: Date };

// CA BPC §17602(g)(2): notice of a fee change no less than 7 and no more
// than 30 days before it takes effect. The price swap and the notice happen
// together, so the swap itself waits for this window before a renewal
// (docs/plans/membership-manage-page.md, "Apply-if-due").
const NOTICE_WINDOW_MIN_DAYS = 7;
const NOTICE_WINDOW_MAX_DAYS = 30;
const DAY_MS = 24 * 60 * 60 * 1000;

function daysUntil(instant: Date, now: Date): number {
  return (instant.getTime() - now.getTime()) / DAY_MS;
}

function isInNoticeWindow(periodEnd: Date | null, now: Date): boolean {
  if (!periodEnd) {
    return false;
  }
  const days = daysUntil(periodEnd, now);
  return days >= NOTICE_WINDOW_MIN_DAYS && days <= NOTICE_WINDOW_MAX_DAYS;
}

/** The month after `periodEnd` on the subscription's anchor day, clamped to
 * the shorter month the way Stripe clamps (anchor 31 → Feb 28 → Mar 31, never
 * Mar 28 or Mar 3). The anchor is `started_at`'s day: Checkout subscriptions
 * bill from their start instant. UTC, like Stripe's epochs. */
function nextRenewalAfter(periodEnd: Date, anchorDay: number): Date {
  const next = new Date(periodEnd.getTime());
  next.setUTCDate(1);
  next.setUTCMonth(next.getUTCMonth() + 1);
  const lastDay = new Date(Date.UTC(next.getUTCFullYear(), next.getUTCMonth() + 1, 0)).getUTCDate();
  next.setUTCDate(Math.min(anchorDay, lastDay));
  return next;
}

/** The renewal a still-unapplied change would first bill: this period's end
 * while the notice can still go out 7 days ahead of it, else the renewal
 * after (the post-renewal poke or `invoice.upcoming` applies it then). A
 * projection for display; the firm date is recorded with the notice. */
function projectedAmountStart(periodEnd: Date | null, anchorDay: number, now: Date): Date | null {
  if (!periodEnd) {
    return null;
  }
  return daysUntil(periodEnd, now) >= NOTICE_WINDOW_MIN_DAYS ? periodEnd : nextRenewalAfter(periodEnd, anchorDay);
}

/** "Will end without renewing." Current API versions express a period-end
 * cancel as a scheduled `cancel_at` (= the period end) with
 * `cancel_at_period_end` still false; older clients set the boolean. Known
 * tradeoff: a dashboard operator could schedule cancel_at at some OTHER
 * date, and the settings page would still caption the end as the period
 * boundary — accepted, because nothing in the product sets such a date, and
 * the tempting `cancel_at === current_period_end` refinement fails the other
 * way (a second of drift shows a canceled member as renewing). */
function isScheduledToCancel(subscription: Stripe.Subscription): boolean {
  return subscription.cancel_at_period_end === true || subscription.cancel_at != null;
}

/** API versions 2025-03-31+ carry the billing period on the item, not the
 * subscription. */
function currentPeriodEndOf(subscription: Stripe.Subscription): Date | null {
  const epoch = subscription.items?.data?.[0]?.current_period_end;
  return epoch ? epochToDate(epoch) : null;
}

export function createMembershipService(options: MembershipServiceOptions): MembershipService {
  const { db, stripe } = options;
  const publicBaseUrl = options.publicBaseUrl.replace(/\/+$/, "");
  // Every portal session returns to the membership page, where the plan is
  // managed (docs/plans/membership-manage-page.md).
  const membershipPageUrl = `${publicBaseUrl}/me/membership`;
  // Checkout starts on the kind-specific support page, so it returns there.
  const termsUrl = `${publicBaseUrl}/terms`;

  async function findBillingCustomerByUserId(userId: string): Promise<BillingCustomerRow | null> {
    const result = await db.query<BillingCustomerRow>(
      `
        SELECT id::text AS id, user_id::text AS user_id, stripe_customer_id
        FROM public.billing_customers
        WHERE user_id = $1::uuid
      `,
      [userId]
    );
    return result.rows[0] ?? null;
  }

  async function findBillingCustomerByStripeId(stripeCustomerId: string): Promise<BillingCustomerRow | null> {
    const result = await db.query<BillingCustomerRow>(
      `
        SELECT id::text AS id, user_id::text AS user_id, stripe_customer_id
        FROM public.billing_customers
        WHERE stripe_customer_id = $1
      `,
      [stripeCustomerId]
    );
    return result.rows[0] ?? null;
  }

  async function getOrCreateBillingCustomer(userId: string, email: string): Promise<BillingCustomerRow> {
    const existing = await findBillingCustomerByUserId(userId);
    if (existing) {
      // Self-heal a stale customer email right where it matters most: Checkout
      // locks an existing customer's email and Stripe's receipts follow it,
      // while the email-change sync hook is best-effort one-shot. If this
      // update fails Stripe is down and the session create below would fail
      // anyway, so let it propagate rather than lock in the stale address.
      await stripe.customers.update(existing.stripe_customer_id, { email });
      return existing;
    }
    // Email on the Stripe customer drives Stripe's own receipts; the privacy
    // policy names Stripe as a processor for exactly this.
    const created = await stripe.customers.create({ email });
    const inserted = await db.query<BillingCustomerRow>(
      `
        INSERT INTO public.billing_customers (user_id, stripe_customer_id)
        VALUES ($1::uuid, $2)
        ON CONFLICT (user_id) DO NOTHING
        RETURNING id::text AS id, user_id::text AS user_id, stripe_customer_id
      `,
      [userId, created.id]
    );
    if (inserted.rows[0]) {
      return inserted.rows[0];
    }
    // Lost a concurrent-create race; the winner's row is the customer. The
    // Stripe customer created above stays orphaned, which is harmless — it
    // can never be charged.
    const winner = await findBillingCustomerByUserId(userId);
    if (!winner) {
      throw new Error(`billing customer for user ${userId} vanished after insert conflict`);
    }
    return winner;
  }

  async function findLiveSubscriptionId(
    billingCustomerId: string,
    excludeSubscriptionId?: string
  ): Promise<string | null> {
    const result = await db.query<{ stripe_subscription_id: string }>(
      `
        SELECT stripe_subscription_id
        FROM public.billing_subscriptions
        WHERE billing_customer_id = $1::uuid
          AND stripe_status NOT IN ('canceled', 'incomplete_expired')
          AND ($2::text IS NULL OR stripe_subscription_id <> $2::text)
        LIMIT 1
      `,
      [billingCustomerId, excludeSubscriptionId ?? null]
    );
    return result.rows[0]?.stripe_subscription_id ?? null;
  }

  async function retrieveSubscription(subscriptionId: string): Promise<FetchedSubscription> {
    const syncedAt = new Date();
    const subscription = await stripe.subscriptions.retrieve(subscriptionId);
    return { subscription, syncedAt };
  }

  async function updateSubscription(
    subscriptionId: string,
    params: Stripe.SubscriptionUpdateParams
  ): Promise<FetchedSubscription> {
    const subscription = await stripe.subscriptions.update(subscriptionId, params);
    return { subscription, syncedAt: new Date() };
  }

  /** Cancel that treats "already canceled" as success: retrying a cancel must
   * never fail, or the deletion precondition would wedge on a subscription a
   * previous attempt (or the portal) already ended. */
  async function cancelSubscriptionSafely(subscriptionId: string): Promise<FetchedSubscription> {
    try {
      const subscription = await stripe.subscriptions.cancel(subscriptionId);
      return { subscription, syncedAt: new Date() };
    } catch (error) {
      // Stripe rejects canceling an already-canceled subscription; confirm
      // via a retrieve instead of matching error strings.
      const current = await retrieveSubscription(subscriptionId).catch(() => null);
      if (current && isTerminalStatus(current.subscription.status)) {
        return current;
      }
      throw error;
    }
  }

  async function lookupActiveUserEmail(userId: string): Promise<string | null> {
    const result = await db.query<{ email: string }>(
      `
        SELECT email::text AS email
        FROM public.users
        WHERE id = $1::uuid
          AND deleted_at IS NULL
      `,
      [userId]
    );
    return result.rows[0]?.email ?? null;
  }

  /** Writes a subscription's current state to billing_subscriptions. Pure —
   * no Stripe call, no email — so the webhook poke and the manage-page
   * actions share one writer, and a mail failure can never be reported as
   * a failed action. Returns null (logged) for an unusable amount: our
   * checkouts always set unit_amount ≥ the minimum, so that is a foreign or
   * hand-made subscription the schema CHECK would reject anyway. */
  async function upsertSubscriptionRow(
    { subscription, syncedAt }: FetchedSubscription,
    customer: BillingCustomerRow,
    checkoutSessionId: string | null
  ): Promise<{ monthlyAmountCents: number; acknowledgmentPending: boolean } | null> {
    const monthlyAmountCents = subscription.items?.data?.[0]?.price?.unit_amount ?? null;
    if (monthlyAmountCents === null || monthlyAmountCents < MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS) {
      console.error(
        `[membership] subscription ${subscription.id} has unusable amount ${monthlyAmountCents ?? "<none>"}; not recording`
      );
      return null;
    }
    const upserted = await db.query<{ acknowledgment_sent_at: Date | null }>(
      `
        INSERT INTO public.billing_subscriptions (
          stripe_subscription_id, billing_customer_id, stripe_checkout_session_id,
          monthly_amount_cents, stripe_status, cancel_at_period_end,
          current_period_end, started_at, canceled_at, stripe_synced_at
        )
        VALUES ($1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (stripe_subscription_id) DO UPDATE SET
          monthly_amount_cents = EXCLUDED.monthly_amount_cents,
          stripe_status = EXCLUDED.stripe_status,
          cancel_at_period_end = EXCLUDED.cancel_at_period_end,
          current_period_end = EXCLUDED.current_period_end,
          canceled_at = EXCLUDED.canceled_at,
          -- Consent evidence is append-only: never overwrite a stored session
          -- pointer with NULL from a later poke.
          stripe_checkout_session_id = COALESCE(
            public.billing_subscriptions.stripe_checkout_session_id,
            EXCLUDED.stripe_checkout_session_id
          ),
          stripe_synced_at = EXCLUDED.stripe_synced_at,
          updated_at = now()
        -- Stale-write guard (see FetchedSubscription): a row already carrying
        -- a newer Stripe instant keeps it. NULL = written before the column
        -- existed.
        WHERE public.billing_subscriptions.stripe_synced_at IS NULL
           OR public.billing_subscriptions.stripe_synced_at <= EXCLUDED.stripe_synced_at
        RETURNING acknowledgment_sent_at
      `,
      [
        subscription.id,
        customer.id,
        checkoutSessionId,
        monthlyAmountCents,
        subscription.status,
        isScheduledToCancel(subscription),
        currentPeriodEndOf(subscription),
        epochToDate(subscription.start_date),
        subscription.canceled_at ? epochToDate(subscription.canceled_at) : null,
        syncedAt,
      ]
    );
    if (!upserted.rows[0]) {
      // A newer write already landed; nothing to acknowledge from this one.
      console.log(`[membership] subscription ${subscription.id}: skipped a stale write (row is newer than ${syncedAt.toISOString()})`);
      return { monthlyAmountCents, acknowledgmentPending: false };
    }
    return { monthlyAmountCents, acknowledgmentPending: upserted.rows[0].acknowledgment_sent_at === null };
  }

  /** The poke pattern: whatever subscription event arrived, fetch the current
   * subscription from Stripe and write that. Returns nothing; all failures
   * throw (→ 5xx → Stripe retries) except permanent mismatches, which log. */
  async function syncSubscription(subscriptionId: string, checkoutSessionId: string | null): Promise<void> {
    let fetched = await retrieveSubscription(subscriptionId);
    // Ownership check before anything mutates: only subscriptions on OUR
    // membership Product are recorded or auto-canceled. The account is
    // single-product today, but this keeps the guards below provably scoped
    // if a dashboard experiment or second product ever appears.
    const subscriptionProductId = idOf(fetched.subscription.items?.data?.[0]?.price?.product ?? null);
    if (subscriptionProductId !== options.membershipProductId) {
      console.error(
        `[membership] subscription ${subscriptionId} is for product ${subscriptionProductId ?? "<none>"}, not the membership product; ignoring`
      );
      return;
    }
    const stripeCustomerId = idOf(fetched.subscription.customer);
    const customer = stripeCustomerId ? await findBillingCustomerByStripeId(stripeCustomerId) : null;
    if (!customer) {
      // Not one of ours (the customer row is created before any checkout
      // session, so it always exists for our subscriptions). Permanent —
      // retrying cannot make the row appear.
      console.error(
        `[membership] subscription ${subscriptionId} references unknown Stripe customer ${stripeCustomerId ?? "<none>"}; ignoring`
      );
      return;
    }

    if (!isTerminalStatus(fetched.subscription.status)) {
      // Deleted-account guard: a checkout tab completed after the account was
      // hard-deleted. Cancel at Stripe immediately — nobody owns this
      // membership any more.
      if (customer.user_id === null) {
        // error, not warn: if a charge already landed it needs a MANUAL
        // refund from the dashboard (it stays visible in the ledger).
        console.error(
          `[membership] subscription ${subscriptionId} arrived for deleted-account billing customer ${customer.id}; canceling — refund any first charge manually`
        );
        fetched = await cancelSubscriptionSafely(subscriptionId);
      } else {
        // Duplicate-subscription guard: the customer already has a different
        // live subscription (concurrent checkout tabs). Cancel the newcomer;
        // its already-charged first month is refunded manually from the
        // dashboard (it stays visible in the ledger).
        const otherLiveId = await findLiveSubscriptionId(customer.id, subscriptionId);
        if (otherLiveId) {
          // error, not warn: the newcomer's already-charged first month needs
          // a MANUAL refund from the dashboard (visible in the ledger).
          console.error(
            `[membership] billing customer ${customer.id} already has live subscription ${otherLiveId}; canceling newly arrived ${subscriptionId} — refund its first charge manually`
          );
          fetched = await cancelSubscriptionSafely(subscriptionId);
        }
      }
    }

    const upserted = await upsertSubscriptionRow(fetched, customer, checkoutSessionId);
    if (!upserted) {
      return;
    }

    // §17602 post-purchase acknowledgment: once, when the subscription is
    // live and unacknowledged.
    if (isBillableStatus(fetched.subscription.status) && upserted.acknowledgmentPending && customer.user_id !== null) {
      await sendStartAcknowledgment(subscriptionId, customer.user_id, upserted.monthlyAmountCents);
    }
    // Every poke is also the timing signal for a requested amount change
    // (docs/plans/membership-manage-page.md, "Apply-if-due").
    await applyDueAmountChange(fetched, customer);
  }

  /** A failed send leaves acknowledgment_sent_at NULL and 5xxes (see the
   * catch below), so both Stripe's redelivery of this event and any later
   * subscription poke retry it. */
  async function sendStartAcknowledgment(subscriptionId: string, userId: string, monthlyAmountCents: number): Promise<void> {
    if (!options.sendMembershipStartedEmail) {
      console.warn(
        `[membership] no acknowledgment email sender configured; subscription ${subscriptionId} stays unacknowledged`
      );
      return;
    }
    try {
      const email = await lookupActiveUserEmail(userId);
      if (!email) {
        return;
      }
      await options.sendMembershipStartedEmail({ email, monthlyAmountCents });
      await db.query(
        `
          UPDATE public.billing_subscriptions
          SET acknowledgment_sent_at = now()
          WHERE stripe_subscription_id = $1
            AND acknowledgment_sent_at IS NULL
        `,
        [subscriptionId]
      );
    } catch (error) {
      // A failed send must not be swallowed with a 200: the checkout burst's
      // other pokes land within seconds of this one, so a mailer outage
      // spanning the burst would leave this LEGALLY REQUIRED (§17602) notice
      // unsent until some later subscription event — possibly next month's
      // renewal. 5xx instead, so Stripe redelivers THIS event with backoff
      // for days; the upsert before it is idempotent under the retry. A send
      // that succeeded but failed to stamp re-sends on retry — a rare
      // duplicate acknowledgment beats a lost one.
      console.warn(
        `[membership] acknowledgment email for subscription ${subscriptionId} failed; asking Stripe to redeliver:`,
        error instanceof Error ? error.message : String(error)
      );
      throw new MembershipWebhookRetryError(
        `acknowledgment email for subscription ${subscriptionId} failed`
      );
    }
  }

  // Amount changes (docs/plans/membership-manage-page.md). One row per
  // request in billing_subscription_amount_changes; "unbilled" = not yet in
  // effect (unapplied, or applied and waiting for its renewal).

  type AmountChangeRow = {
    id: string;
    previous_amount_cents: number;
    new_amount_cents: number;
    requested_at: Date;
    applied_at: Date | null;
    effective_at: Date | null;
    notice_sent_at: Date | null;
    superseded_at: Date | null;
  };

  /** Unbilled rows plus applied-but-superseded ones still ahead of their
   * renewal (a change replaced inside the window), oldest request first.
   * Superseded rows that never applied are history and stay out. */
  async function listUnbilledAmountChanges(subscriptionId: string): Promise<AmountChangeRow[]> {
    const result = await db.query<AmountChangeRow>(
      `
        SELECT id::text AS id, previous_amount_cents, new_amount_cents, requested_at,
               applied_at, effective_at, notice_sent_at, superseded_at
        FROM public.billing_subscription_amount_changes
        WHERE stripe_subscription_id = $1
          AND (applied_at IS NULL OR effective_at > now())
          AND (superseded_at IS NULL OR applied_at IS NOT NULL)
        ORDER BY requested_at ASC
      `,
      [subscriptionId]
    );
    return result.rows;
  }

  /** What the current billing period costs: the price the FIRST applied
   * (unbilled) swap replaced, else the row's amount. Stripe's own price
   * already reads as the new amount once a swap is applied. */
  function currentPeriodAmount(changes: AmountChangeRow[], rowAmountCents: number): number {
    const firstApplied = [...changes]
      .filter((change) => change.applied_at !== null)
      .sort((a, b) => (a.applied_at as Date).getTime() - (b.applied_at as Date).getTime())[0];
    return firstApplied ? firstApplied.previous_amount_cents : rowAmountCents;
  }

  function latestOpenAmountChange(changes: AmountChangeRow[]): AmountChangeRow | null {
    const open = changes.filter((change) => change.superseded_at === null);
    return open[open.length - 1] ?? null;
  }

  async function supersedeAmountChanges(subscriptionId: string, scope: "unapplied" | "unbilled"): Promise<void> {
    await db.query(
      `
        UPDATE public.billing_subscription_amount_changes
        SET superseded_at = now()
        WHERE stripe_subscription_id = $1
          AND superseded_at IS NULL
          AND ${scope === "unapplied" ? "applied_at IS NULL" : "(applied_at IS NULL OR effective_at > now())"}
      `,
      [subscriptionId]
    );
  }

  /** The same ad-hoc-price mechanism Checkout uses; no proration in either
   * direction, and the billing-cycle anchor is untouched, so the renewal at
   * current_period_end is the first invoice at the new amount. */
  async function swapSubscriptionPrice(
    subscriptionId: string,
    itemId: string,
    amountCents: number
  ): Promise<FetchedSubscription> {
    return updateSubscription(subscriptionId, {
      items: [
        {
          id: itemId,
          price_data: {
            currency: "usd",
            product: options.membershipProductId,
            unit_amount: amountCents,
            recurring: { interval: "month" },
          },
        },
      ],
      proration_behavior: "none",
    });
  }

  /** The latest open (un-superseded, unbilled) change. Runs on every
   * subscription poke and after every request. NOTICE FIRST, then the swap:
   * the §17602(g)(2) notice is the legal requirement, so the Stripe price
   * never moves until a notice naming this renewal has actually gone out —
   * a mail outage leaves the price alone and the request pending, never a
   * changed price with a late notice. A failed send throws
   * MembershipWebhookRetryError so the webhook 5xxes and Stripe redelivers.
   * Once applied, each poke also reconciles Stripe's price to the noticed
   * amount, so a slower, older swap landing after a newer one is repaired
   * by its own webhook. */
  async function applyDueAmountChange(fetched: FetchedSubscription, customer: BillingCustomerRow): Promise<void> {
    const { subscription } = fetched;
    const change = latestOpenAmountChange(await listUnbilledAmountChanges(subscription.id));
    if (!change) {
      return;
    }
    if (isTerminalStatus(subscription.status)) {
      // Nothing will bill again; the row stays as the record of the request.
      await db.query(
        `
          UPDATE public.billing_subscription_amount_changes
          SET superseded_at = now()
          WHERE id = $1::uuid AND superseded_at IS NULL
        `,
        [change.id]
      );
      return;
    }
    // A scheduled cancel leaves the request parked: resume brings it back
    // into play, and a canceled member must not be re-priced.
    if (!isBillableStatus(subscription.status) || isScheduledToCancel(subscription)) {
      return;
    }
    const item = subscription.items?.data?.[0];
    if (!item?.id) {
      console.error(`[membership] subscription ${subscription.id} has no item to re-price; leaving the amount change alone`);
      return;
    }

    if (change.applied_at !== null) {
      // Reconcile: the member was noticed for this amount, so it is what the
      // renewal must bill. Drift here means an older swap (a poke that read
      // the previous request before it was replaced) landed after this one.
      if (item.price?.unit_amount !== change.new_amount_cents) {
        console.error(
          `[membership] subscription ${subscription.id} bills ${item.price?.unit_amount ?? "<none>"} but the noticed amount is ${change.new_amount_cents}; restoring`
        );
        const restored = await swapSubscriptionPrice(subscription.id, item.id, change.new_amount_cents);
        await upsertSubscriptionRow(restored, customer, null);
      }
      return;
    }

    const periodEnd = currentPeriodEndOf(subscription);
    if (!periodEnd || !isInNoticeWindow(periodEnd, new Date())) {
      return;
    }
    // The notice names the renewal it precedes. One already sent for THIS
    // renewal (the swap failed after it) is not repeated; one sent for an
    // earlier renewal that has since passed is stale and goes out again.
    if (change.notice_sent_at === null || change.effective_at?.getTime() !== periodEnd.getTime()) {
      if (customer.user_id === null) {
        // Deleted account: nobody to notice, so nothing may change.
        return;
      }
      if (!options.sendMembershipChangedEmail) {
        console.warn(
          `[membership] no email sender configured; amount change for subscription ${subscription.id} stays pending unnoticed`
        );
        return;
      }
      try {
        const email = await lookupActiveUserEmail(customer.user_id);
        if (!email) {
          return;
        }
        await options.sendMembershipChangedEmail({
          kind: "amount_notice",
          email,
          newAmountCents: change.new_amount_cents,
          startsAt: periodEnd,
        });
        await db.query(
          `
            UPDATE public.billing_subscription_amount_changes
            SET notice_sent_at = now(), effective_at = $2
            WHERE id = $1::uuid AND applied_at IS NULL
          `,
          [change.id, periodEnd]
        );
      } catch (error) {
        // Same reasoning as the start acknowledgment: the notice is the legal
        // requirement, so a failed send must keep retrying, not 200. Nothing
        // has changed at Stripe.
        console.warn(
          `[membership] amount-change notice for subscription ${subscription.id} failed; asking Stripe to redeliver:`,
          error instanceof Error ? error.message : String(error)
        );
        throw new MembershipWebhookRetryError(`amount-change notice for subscription ${subscription.id} failed`);
      }
    }

    const updated = await swapSubscriptionPrice(subscription.id, item.id, change.new_amount_cents);
    // A request replaced while this swap was in flight stays unapplied
    // history; the reconcile above puts Stripe back on the replacement.
    await db.query(
      `
        UPDATE public.billing_subscription_amount_changes
        SET applied_at = now()
        WHERE id = $1::uuid AND applied_at IS NULL AND superseded_at IS NULL
      `,
      [change.id]
    );
    await upsertSubscriptionRow(updated, customer, null);
  }

  async function handleCheckoutSessionCompleted(event: Stripe.Event): Promise<void> {
    const session = event.data.object as Stripe.Checkout.Session;
    if (session.mode === "payment") {
      const billingCustomerId = session.client_reference_id;
      const paymentIntentId = idOf(session.payment_intent);
      const amountCents = session.amount_total;
      if (!billingCustomerId || !paymentIntentId || !amountCents || amountCents <= 0) {
        // Not a session we created (ours always carry all three). Permanent.
        console.error(
          `[membership] payment-mode checkout session ${session.id} missing reference/intent/amount; ignoring`
        );
        return;
      }
      const customer = await db.query<{ id: string }>(
        `SELECT id::text AS id FROM public.billing_customers WHERE id = $1::uuid`,
        [billingCustomerId]
      );
      if (!customer.rows[0]) {
        // Would otherwise FK-fail and retry for 3 days against a row that can
        // never appear.
        console.error(
          `[membership] checkout session ${session.id} references unknown billing customer ${billingCustomerId}; ignoring`
        );
        return;
      }
      await db.query(
        `
          INSERT INTO public.billing_payments (
            billing_customer_id, amount_cents, currency, kind,
            stripe_payment_ref, stripe_payment_intent_id, paid_at
          )
          VALUES ($1::uuid, $2, $3, 'one_time', $4, $4, $5)
          ON CONFLICT (stripe_payment_ref) DO NOTHING
        `,
        [billingCustomerId, amountCents, session.currency ?? "usd", paymentIntentId, epochToDate(event.created)]
      );
      return;
    }

    if (session.mode === "subscription") {
      const subscriptionId = idOf(session.subscription);
      if (!subscriptionId) {
        console.error(`[membership] subscription-mode checkout session ${session.id} carries no subscription; ignoring`);
        return;
      }
      // CA BPC §17602: the session carried a required renewal-terms consent
      // checkbox; its id is our retained pointer to Stripe's consent record.
      // A completion without consent should be impossible (Stripe enforces
      // `required`), so an absence FAILS CLOSED: continuing to bill monthly
      // without retained consent evidence is exactly what §17602 forbids.
      // Cancel the subscription (idempotent; a throw → 5xx → Stripe retries)
      // and record its canceled state; the already-charged first month is
      // refunded manually from the dashboard, like the guard cancels.
      const consented = session.consent?.terms_of_service === "accepted";
      if (!consented) {
        // Ownership check BEFORE the cancel, mirroring syncSubscription's: a
        // consent-less subscription session for some other product on this
        // account (which never configures our consent collection) must not
        // get that product's subscription canceled by our fail-closed rule.
        const subscription = await stripe.subscriptions.retrieve(subscriptionId);
        const subscriptionProductId = idOf(subscription.items?.data?.[0]?.price?.product ?? null);
        if (subscriptionProductId !== options.membershipProductId) {
          console.error(
            `[membership] consent-less checkout session ${session.id} is for product ${subscriptionProductId ?? "<none>"}, not the membership product; ignoring`
          );
          return;
        }
        console.error(
          `[membership] checkout session ${session.id} completed WITHOUT terms-of-service consent; canceling subscription ${subscriptionId} — refund its first charge manually`
        );
        await cancelSubscriptionSafely(subscriptionId);
        await syncSubscription(subscriptionId, null);
        return;
      }
      // No ledger row here: the first month's row comes from invoice.paid;
      // writing it in both places would double-count.
      await syncSubscription(subscriptionId, session.id);
    }
  }

  async function handleInvoicePaid(event: Stripe.Event): Promise<void> {
    const invoice = event.data.object as Stripe.Invoice;
    const invoiceId = invoice.id;
    if (!invoiceId || !invoice.amount_paid || invoice.amount_paid <= 0) {
      return;
    }
    const stripeCustomerId = idOf(invoice.customer);
    const customer = stripeCustomerId ? await findBillingCustomerByStripeId(stripeCustomerId) : null;
    if (!customer) {
      console.error(
        `[membership] invoice ${invoiceId} references unknown Stripe customer ${stripeCustomerId ?? "<none>"}; ignoring`
      );
      return;
    }
    // Idempotency fast-path: a retry of an already-recorded invoice skips the
    // payments lookup entirely.
    const existing = await db.query(`SELECT 1 FROM public.billing_payments WHERE stripe_payment_ref = $1`, [invoiceId]);
    if ((existing.rowCount ?? 0) > 0) {
      return;
    }
    // Refunds identify a payment_intent, never an invoice, so resolve the
    // intent now. invoice.payment_intent itself was removed in 2025-03-31+
    // API versions; the Invoice Payments list is the supported route. Our
    // invoices are Checkout card subscriptions paid in full by one payment.
    const payments = await stripe.invoicePayments.list({ invoice: invoiceId });
    const paidIntentIds = payments.data
      .filter((payment) => payment.status === "paid")
      .map((payment) => (payment.payment.type === "payment_intent" ? idOf(payment.payment.payment_intent) : null))
      .filter((value): value is string => value !== null);
    if (paidIntentIds.length === 0) {
      // Caught Stripe mid-settlement; the retry will find it.
      console.warn(`[membership] invoice ${invoiceId} has no succeeded payment yet; asking Stripe to redeliver`);
      throw new MembershipWebhookRetryError(`invoice ${invoiceId} has no succeeded payment yet`);
    }
    if (paidIntentIds.length > 1) {
      console.warn(`[membership] invoice ${invoiceId} has ${paidIntentIds.length} paid payments; recording the first`);
    }
    const paidAt = invoice.status_transitions?.paid_at
      ? epochToDate(invoice.status_transitions.paid_at)
      : epochToDate(event.created);
    await db.query(
      `
        INSERT INTO public.billing_payments (
          billing_customer_id, amount_cents, currency, kind,
          stripe_payment_ref, stripe_payment_intent_id, paid_at
        )
        VALUES ($1::uuid, $2, $3, 'monthly', $4, $5, $6)
        ON CONFLICT (stripe_payment_ref) DO NOTHING
      `,
      [customer.id, invoice.amount_paid, invoice.currency ?? "usd", invoiceId, paidIntentIds[0], paidAt]
    );
  }

  async function handleChargeRefunded(event: Stripe.Event): Promise<void> {
    const charge = event.data.object as Stripe.Charge;
    const paymentIntentId = idOf(charge.payment_intent);
    if (!paymentIntentId) {
      console.error(`[membership] refunded charge ${charge.id} carries no payment_intent; ignoring`);
      return;
    }
    // SET Stripe's cumulative amount_refunded — an absolute assignment, never
    // an increment. That makes this idempotent under retries, correct for
    // partial refunds, and self-correcting when a pending refund later fails.
    const updated = await db.query(
      `
        UPDATE public.billing_payments
        SET refunded_amount_cents = $2,
            refunded_at = CASE
              WHEN refunded_at IS NULL AND $2 > 0 THEN now()
              ELSE refunded_at
            END
        WHERE stripe_payment_intent_id = $1
      `,
      [paymentIntentId, charge.amount_refunded]
    );
    if ((updated.rowCount ?? 0) === 0) {
      // Delivery order isn't guaranteed: the refund can arrive while the
      // invoice.paid that creates the ledger row is still retrying. 5xx so
      // Stripe redelivers until the row exists — every charge on this account
      // comes from our checkouts, so it always eventually matches.
      console.warn(
        `[membership] refund for payment_intent ${paymentIntentId} has no ledger row yet; asking Stripe to redeliver`
      );
      throw new MembershipWebhookRetryError(
        `refund for payment_intent ${paymentIntentId} has no ledger row yet`
      );
    }
  }

  /** Apply-if-due from a member's HTTP request. A failed NOTICE means
   * nothing changed at Stripe and the request is recorded as pending, which
   * is what the answered status says; the next subscription poke retries
   * it, so the member is not shown an error for a saved request. */
  async function applyDueAmountChangeLogged(fetched: FetchedSubscription, customer: BillingCustomerRow): Promise<void> {
    try {
      await applyDueAmountChange(fetched, customer);
    } catch (error) {
      if (!(error instanceof MembershipWebhookRetryError)) {
        throw error;
      }
      console.warn(`[membership] ${error.message}; the request stays pending for the next subscription poke`);
    }
  }

  async function sendChangedEmailBestEffort(input: MembershipChangedEmailInput): Promise<void> {
    if (!options.sendMembershipChangedEmail) {
      return;
    }
    try {
      await options.sendMembershipChangedEmail(input);
    } catch (error) {
      // The action already happened at Stripe and in our row; a lost
      // courtesy email must not turn that into an error for the member.
      console.warn(
        `[membership] ${input.kind} confirmation email failed:`,
        error instanceof Error ? error.message : String(error)
      );
    }
  }

  /** The member's live subscription, fetched fresh from Stripe and checked
   * against our records before any manage-page mutation. A terminal state
   * found here (canceled between reads) is recorded, then reported as no
   * membership. */
  async function loadOwnedLiveSubscription(
    userId: string
  ): Promise<{ customer: BillingCustomerRow; fetched: FetchedSubscription }> {
    const customer = await findBillingCustomerByUserId(userId);
    const liveSubscriptionId = customer ? await findLiveSubscriptionId(customer.id) : null;
    if (!customer || !liveSubscriptionId) {
      throw new MembershipServiceError("no_membership", "You don't have a monthly membership.");
    }
    const fetched = await retrieveSubscription(liveSubscriptionId);
    const { subscription } = fetched;
    const productId = idOf(subscription.items?.data?.[0]?.price?.product ?? null);
    const stripeCustomerId = idOf(subscription.customer);
    if (productId !== options.membershipProductId || stripeCustomerId !== customer.stripe_customer_id) {
      console.error(
        `[membership] subscription ${liveSubscriptionId} for user ${userId} does not match our records (product ${productId ?? "<none>"}, customer ${stripeCustomerId ?? "<none>"}); refusing to change it`
      );
      throw new MembershipServiceError(
        "membership_conflict",
        "Your membership record doesn't match our payment processor. Contact us and we'll sort it out."
      );
    }
    if (isTerminalStatus(subscription.status)) {
      await upsertSubscriptionRow(fetched, customer, null);
      throw new MembershipServiceError("no_membership", "You don't have a monthly membership.");
    }
    return { customer, fetched };
  }

  const service: MembershipService = {
    async getMembership(userId) {
      const customer = await findBillingCustomerByUserId(userId);
      if (!customer) {
        return { enabled: true, membership: null, total_net_cents: 0, payments: [] };
      }
      const subscriptionResult = await db.query<{
        stripe_subscription_id: string;
        stripe_status: string;
        monthly_amount_cents: number;
        cancel_at_period_end: boolean;
        current_period_end: Date | null;
        started_at: Date;
      }>(
        `
          SELECT stripe_status, monthly_amount_cents, cancel_at_period_end,
                 current_period_end, started_at, stripe_subscription_id
          FROM public.billing_subscriptions
          WHERE billing_customer_id = $1::uuid
            AND stripe_status NOT IN ('canceled', 'incomplete_expired')
          LIMIT 1
        `,
        [customer.id]
      );
      const paymentsResult = await db.query<{
        amount_cents: number;
        refunded_amount_cents: number;
        kind: MembershipKind;
        currency: string;
        paid_at: Date;
      }>(
        `
          SELECT amount_cents, refunded_amount_cents, kind, currency, paid_at
          FROM public.billing_payments
          WHERE billing_customer_id = $1::uuid
          ORDER BY paid_at DESC
          LIMIT 50
        `,
        [customer.id]
      );
      const totalResult = await db.query<{ total_net_cents: number }>(
        `
          SELECT COALESCE(SUM(amount_cents - refunded_amount_cents), 0)::int AS total_net_cents
          FROM public.billing_payments
          WHERE billing_customer_id = $1::uuid
        `,
        [customer.id]
      );
      const subscription = subscriptionResult.rows[0] ?? null;
      const changes = subscription ? await listUnbilledAmountChanges(subscription.stripe_subscription_id) : [];
      const periodAmountCents = subscription ? currentPeriodAmount(changes, subscription.monthly_amount_cents) : 0;
      const pendingChange = latestOpenAmountChange(changes);
      const pendingStartsAt =
        pendingChange && subscription
          ? pendingChange.applied_at
            ? pendingChange.effective_at
            : projectedAmountStart(subscription.current_period_end, subscription.started_at.getUTCDate(), new Date())
          : null;
      return {
        enabled: true,
        membership: subscription
          ? {
              stripe_status: subscription.stripe_status,
              monthly_amount_cents: periodAmountCents,
              cancel_at_period_end: subscription.cancel_at_period_end,
              current_period_end: subscription.current_period_end
                ? toIsoString(subscription.current_period_end)
                : null,
              started_at: toIsoString(subscription.started_at),
              // A request back to this period's amount (reverting an applied
              // change inside the window) leaves nothing to announce.
              pending_amount_change:
                pendingChange && pendingChange.new_amount_cents !== periodAmountCents
                  ? {
                      new_amount_cents: pendingChange.new_amount_cents,
                      starts_at: pendingStartsAt ? toIsoString(pendingStartsAt) : null,
                      applied: pendingChange.applied_at !== null,
                    }
                  : null,
            }
          : null,
        total_net_cents: totalResult.rows[0]?.total_net_cents ?? 0,
        payments: paymentsResult.rows.map((row) => ({
          amount_cents: row.amount_cents,
          refunded_amount_cents: row.refunded_amount_cents,
          kind: row.kind,
          currency: row.currency,
          paid_at: toIsoString(row.paid_at),
        })),
      };
    },

    async createCheckoutSession(userId, input) {
      // The API layer validated the body; re-assert the money constraints at
      // the service boundary anyway — this is the last stop before Stripe.
      if (
        !Number.isInteger(input.amount_cents) ||
        input.amount_cents < MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS ||
        input.amount_cents > MEMBERSHIP_CHECKOUT_MAX_AMOUNT_CENTS
      ) {
        throw new RequestValidationError(
          `amount_cents must be an integer between ${MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS} and ${MEMBERSHIP_CHECKOUT_MAX_AMOUNT_CENTS}`
        );
      }
      if (input.kind !== "one_time" && input.kind !== "monthly") {
        throw new RequestValidationError("kind must be one_time or monthly");
      }
      const email = await lookupActiveUserEmail(userId);
      if (!email) {
        throw new MembershipServiceError("user_not_found", "Authentication is required");
      }
      const customer = await getOrCreateBillingCustomer(userId, email);
      if (input.kind === "monthly") {
        const liveSubscriptionId = await findLiveSubscriptionId(customer.id);
        if (liveSubscriptionId) {
          throw new MembershipServiceError(
            "membership_exists",
            "You already have a monthly membership. Change the amount under Manage membership."
          );
        }
      }

      const metadata = { billing_customer_id: customer.id, kind: input.kind };
      const monthly = input.kind === "monthly";
      const session = await stripe.checkout.sessions.create({
        mode: monthly ? "subscription" : "payment",
        customer: customer.stripe_customer_id,
        client_reference_id: customer.id,
        // Cards only in v1: no async payment methods, so no
        // checkout.session.async_payment_* events to handle.
        payment_method_types: ["card"],
        line_items: [
          {
            quantity: 1,
            price_data: {
              currency: "usd",
              product: options.membershipProductId,
              unit_amount: input.amount_cents,
              ...(monthly ? { recurring: { interval: "month" } } : {}),
            },
          },
        ],
        success_url: `${publicBaseUrl}/support/${monthly ? "member" : "once"}?membership=success`,
        cancel_url: `${publicBaseUrl}/support/${monthly ? "member" : "once"}?membership=canceled`,
        // Checkout's minimum expiry: a stale tab cannot complete a forgotten
        // session hours later.
        expires_at: Math.floor(Date.now() / 1000) + 30 * 60,
        metadata,
        ...(monthly
          ? {
              subscription_data: { metadata },
              // CA BPC §17602: affirmative consent to the renewal terms,
              // collected next to an unchecked box. The completed session id
              // is stored as the consent-evidence pointer.
              consent_collection: { terms_of_service: "required" },
              custom_text: {
                terms_of_service_acceptance: {
                  message:
                    `You authorize Elections Simplified to charge ${formatUsd(input.amount_cents)} to your payment method ` +
                    `each month until you cancel. Cancel anytime in account settings (Manage membership). ` +
                    `See the [Terms of Use](${termsUrl}).`,
                },
              },
            }
          : {
              payment_intent_data: { metadata },
            }),
      });
      if (!session.url) {
        throw new Error(`Stripe checkout session ${session.id} has no redirect URL`);
      }
      return { url: session.url };
    },

    async createPortalSession(userId, input) {
      const customer = await findBillingCustomerByUserId(userId);
      if (!customer) {
        return null;
      }
      const session = await stripe.billingPortal.sessions.create(
        input?.flow === "payment_method_update"
          ? {
              customer: customer.stripe_customer_id,
              return_url: membershipPageUrl,
              flow_data: {
                type: "payment_method_update",
                // Without after_completion the flow ends on Stripe's own
                // confirmation page instead of coming back to us.
                after_completion: { type: "redirect", redirect: { return_url: membershipPageUrl } },
              },
            }
          : {
              customer: customer.stripe_customer_id,
              return_url: membershipPageUrl,
            }
      );
      return { url: session.url };
    },

    async cancelMembership(userId) {
      const { customer, fetched } = await loadOwnedLiveSubscription(userId);
      const { subscription } = fetched;
      if (subscription.status === "incomplete") {
        // A seconds-wide race by design (cards-only Checkout confirms the
        // payment before the session completes) that auto-expires in 23h;
        // an immediate cancel racing the activation would eat a paid month.
        throw new MembershipServiceError(
          "membership_pending",
          "Your first payment is still being confirmed. Try again in a moment."
        );
      }
      if (isScheduledToCancel(subscription)) {
        // Already in the requested state (a portal cancel, or a retry after a
        // lost response): record it — no second mutation, no second email.
        await upsertSubscriptionRow(fetched, customer, null);
        return service.getMembership(userId);
      }
      const updated = await updateSubscription(subscription.id, { cancel_at_period_end: true });
      await upsertSubscriptionRow(updated, customer, null);
      const email = await lookupActiveUserEmail(userId);
      if (email) {
        await sendChangedEmailBestEffort({ kind: "canceled", email, endsAt: currentPeriodEndOf(updated.subscription) });
      }
      return service.getMembership(userId);
    },

    async resumeMembership(userId) {
      const { customer, fetched } = await loadOwnedLiveSubscription(userId);
      if (!isScheduledToCancel(fetched.subscription)) {
        await upsertSubscriptionRow(fetched, customer, null);
        return service.getMembership(userId);
      }
      // Clear whichever form the schedule took (see isScheduledToCancel): the
      // scheduled cancel_at first — how current API versions store a
      // period-end cancel, including the portal's — then the legacy boolean
      // if it is still set. Verify on the returned object; never report a
      // resume Stripe did not perform.
      let updated = fetched;
      if (updated.subscription.cancel_at != null) {
        updated = await updateSubscription(updated.subscription.id, { cancel_at: "" });
      }
      if (updated.subscription.cancel_at_period_end === true) {
        updated = await updateSubscription(updated.subscription.id, { cancel_at_period_end: false });
      }
      if (isScheduledToCancel(updated.subscription)) {
        console.error(
          `[membership] subscription ${updated.subscription.id} still scheduled to cancel after clearing (cancel_at ${updated.subscription.cancel_at ?? "null"}, cancel_at_period_end ${updated.subscription.cancel_at_period_end})`
        );
        throw new MembershipServiceError(
          "membership_update_failed",
          "We couldn't resume your membership just now. Try again in a few minutes."
        );
      }
      const row = await upsertSubscriptionRow(updated, customer, null);
      const email = await lookupActiveUserEmail(userId);
      if (email && row) {
        await sendChangedEmailBestEffort({
          kind: "resumed",
          email,
          monthlyAmountCents: row.monthlyAmountCents,
          renewsAt: currentPeriodEndOf(updated.subscription),
        });
      }
      // A parked amount change comes back into play with the membership.
      await applyDueAmountChangeLogged(updated, customer);
      return service.getMembership(userId);
    },

    async changeMonthlyAmount(userId, input) {
      // Same defense-in-depth as checkout: last stop before Stripe.
      if (
        !Number.isInteger(input.amount_cents) ||
        input.amount_cents < MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS ||
        input.amount_cents > MEMBERSHIP_CHECKOUT_MAX_AMOUNT_CENTS
      ) {
        throw new RequestValidationError(
          `amount_cents must be an integer between ${MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS} and ${MEMBERSHIP_CHECKOUT_MAX_AMOUNT_CENTS}`
        );
      }
      const { customer, fetched } = await loadOwnedLiveSubscription(userId);
      const { subscription } = fetched;
      if (subscription.status === "incomplete") {
        throw new MembershipServiceError(
          "membership_pending",
          "Your first payment is still being confirmed. Try again in a moment."
        );
      }
      if (!isBillableStatus(subscription.status)) {
        throw new MembershipServiceError(
          "membership_pending",
          "Your last payment didn't go through. Update your payment method first, then change the amount."
        );
      }
      if (isScheduledToCancel(subscription)) {
        throw new MembershipServiceError(
          "membership_pending",
          "Your membership is set to end. Choose Keep membership first, then change the amount."
        );
      }
      // Our checkouts create exactly one monthly USD item; anything else is
      // not a subscription this code knows how to re-price.
      const item = subscription.items?.data?.[0];
      if (
        subscription.items?.data?.length !== 1 ||
        !item?.id ||
        item.price?.currency !== "usd" ||
        item.price?.recurring?.interval !== "month" ||
        typeof item.price?.unit_amount !== "number"
      ) {
        console.error(`[membership] subscription ${subscription.id} for user ${userId} is not a single monthly USD item; refusing to re-price it`);
        throw new MembershipServiceError(
          "membership_conflict",
          "Your membership record doesn't match our payment processor. Contact us and we'll sort it out."
        );
      }
      const stripeAmountCents = item.price.unit_amount;
      await upsertSubscriptionRow(fetched, customer, null);

      const unbilled = latestOpenAmountChange(await listUnbilledAmountChanges(subscription.id));
      if (input.amount_cents === stripeAmountCents) {
        // Re-saving what Stripe already bills withdraws a pending request;
        // with an applied one this IS the requested amount — nothing to do.
        if (unbilled && unbilled.applied_at === null) {
          await supersedeAmountChanges(subscription.id, "unapplied");
        }
      } else {
        if (unbilled && unbilled.applied_at !== null && !isInNoticeWindow(currentPeriodEndOf(subscription), new Date())) {
          // The applied change bills at the coming renewal and there is no
          // longer time to notice a replacement 7 days ahead of it.
          throw new MembershipServiceError(
            "membership_pending",
            `Your new amount of ${formatUsd(unbilled.new_amount_cents)} is already set for your next renewal. You can change it again after that renewal.`
          );
        }
        await supersedeAmountChanges(subscription.id, "unbilled");
        // ON CONFLICT: two concurrent requests both superseded and both
        // insert; the loser's row is dropped and the answer below shows the
        // winner's request.
        await db.query(
          `
            INSERT INTO public.billing_subscription_amount_changes (
              stripe_subscription_id, previous_amount_cents, new_amount_cents
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (stripe_subscription_id) WHERE applied_at IS NULL AND superseded_at IS NULL DO NOTHING
          `,
          [subscription.id, stripeAmountCents, input.amount_cents]
        );
      }
      await applyDueAmountChangeLogged(fetched, customer);
      return service.getMembership(userId);
    },

    async handleWebhookEvent({ rawBody, signatureHeader }) {
      if (!signatureHeader) {
        return "bad_signature";
      }
      let event: Stripe.Event;
      try {
        event = stripe.webhooks.constructEvent(rawBody, signatureHeader, options.webhookSecret);
      } catch (error) {
        console.warn(
          "[membership] webhook signature verification failed:",
          error instanceof Error ? error.message : String(error)
        );
        return "bad_signature";
      }

      switch (event.type) {
        case "checkout.session.completed":
          await handleCheckoutSessionCompleted(event);
          break;
        case "customer.subscription.created":
        case "customer.subscription.updated":
        case "customer.subscription.deleted": {
          // The event is a poke; the fetched subscription is the truth.
          const subscription = event.data.object as Stripe.Subscription;
          await syncSubscription(subscription.id, null);
          break;
        }
        case "invoice.paid":
          await handleInvoicePaid(event);
          break;
        case "charge.refunded":
          await handleChargeRefunded(event);
          break;
        case "invoice.upcoming": {
          // Fires N days before a renewal (Billing settings → "Upcoming
          // renewal events", set to 14): the timing poke that applies a
          // requested amount change inside the §17602(g)(2) window. The
          // preview invoice has no id; the subscription rides on `parent`.
          const invoice = event.data.object as Stripe.Invoice;
          const subscriptionId = idOf(invoice.parent?.subscription_details?.subscription ?? null);
          if (!subscriptionId) {
            console.log(`[membership] invoice.upcoming without a subscription; ignoring`);
            break;
          }
          await syncSubscription(subscriptionId, null);
          break;
        }
        default:
          // The endpoint subscribes to exactly seven event types; anything
          // else is a configuration drift worth a log line, not an error.
          console.log(`[membership] ignoring webhook event type ${event.type}`);
      }
      return "ok";
    },

    async syncCustomerEmail(userId) {
      const customer = await findBillingCustomerByUserId(userId);
      if (!customer) {
        return;
      }
      const email = await lookupActiveUserEmail(userId);
      if (!email) {
        return;
      }
      // Stripe locks an existing customer's email in Checkout and sends its
      // receipts there, so the customer object must track the account email.
      await stripe.customers.update(customer.stripe_customer_id, { email });
    },

    async cancelSubscriptionsForAccountDeletion(userId) {
      const customer = await findBillingCustomerByUserId(userId);
      if (!customer) {
        return false;
      }
      const liveSubscriptionId = await findLiveSubscriptionId(customer.id);
      if (!liveSubscriptionId) {
        return false;
      }
      try {
        await cancelSubscriptionSafely(liveSubscriptionId);
      } catch (error) {
        // Precondition semantics: the caller fails the deletion with a
        // retryable error and deletes nothing. Cancel is idempotent, so the
        // cancel-succeeded-but-delete-failed half-state is harmless.
        console.error(
          `[membership] could not cancel subscription ${liveSubscriptionId} for account deletion:`,
          error instanceof Error ? error.message : String(error)
        );
        throw new MembershipServiceError(
          "subscription_cancel_failed",
          "We couldn't cancel your membership just now. Nothing was deleted — try again in a few minutes."
        );
      }
      // The DB row is updated by the subscription.deleted webhook; Stripe is
      // the authority on status.
      return true;
    },
  };
  return service;
}
