import type { Pool } from "pg";
import type Stripe from "stripe";

import {
  MEMBERSHIP_CHECKOUT_MAX_AMOUNT_CENTS,
  MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS,
} from "../apiValidation.js";

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

export type MembershipStatusResult = {
  enabled: true;
  membership: {
    stripe_status: string;
    monthly_amount_cents: number;
    cancel_at_period_end: boolean;
    current_period_end: string | null;
    started_at: string;
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
};

export class MembershipServiceError extends Error {
  constructor(
    readonly code: "membership_exists" | "no_billing_account" | "user_not_found" | "subscription_cancel_failed",
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
  /** null = the user has no billing customer yet (404). */
  createPortalSession(userId: string): Promise<{ url: string } | null>;
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

export function createMembershipService(options: MembershipServiceOptions): MembershipService {
  const { db, stripe } = options;
  const publicBaseUrl = options.publicBaseUrl.replace(/\/+$/, "");
  const settingsUrl = `${publicBaseUrl}/me/settings`;
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

  /** Cancel that treats "already canceled" as success: retrying a cancel must
   * never fail, or the deletion precondition would wedge on a subscription a
   * previous attempt (or the portal) already ended. */
  async function cancelSubscriptionSafely(subscriptionId: string): Promise<Stripe.Subscription> {
    try {
      return await stripe.subscriptions.cancel(subscriptionId);
    } catch (error) {
      // Stripe rejects canceling an already-canceled subscription; confirm
      // via a retrieve instead of matching error strings.
      const current = await stripe.subscriptions.retrieve(subscriptionId).catch(() => null);
      if (current && isTerminalStatus(current.status)) {
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

  /** The poke pattern: whatever subscription event arrived, fetch the current
   * subscription from Stripe and write that. Returns nothing; all failures
   * throw (→ 5xx → Stripe retries) except permanent mismatches, which log. */
  async function syncSubscription(subscriptionId: string, checkoutSessionId: string | null): Promise<void> {
    let subscription = await stripe.subscriptions.retrieve(subscriptionId);
    // Ownership check before anything mutates: only subscriptions on OUR
    // membership Product are recorded or auto-canceled. The account is
    // single-product today, but this keeps the guards below provably scoped
    // if a dashboard experiment or second product ever appears.
    const subscriptionProductId = idOf(subscription.items?.data?.[0]?.price?.product ?? null);
    if (subscriptionProductId !== options.membershipProductId) {
      console.error(
        `[membership] subscription ${subscriptionId} is for product ${subscriptionProductId ?? "<none>"}, not the membership product; ignoring`
      );
      return;
    }
    const stripeCustomerId = idOf(subscription.customer);
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

    if (!isTerminalStatus(subscription.status)) {
      // Deleted-account guard: a checkout tab completed after the account was
      // hard-deleted. Cancel at Stripe immediately — nobody owns this
      // membership any more.
      if (customer.user_id === null) {
        // error, not warn: if a charge already landed it needs a MANUAL
        // refund from the dashboard (it stays visible in the ledger).
        console.error(
          `[membership] subscription ${subscriptionId} arrived for deleted-account billing customer ${customer.id}; canceling — refund any first charge manually`
        );
        subscription = await cancelSubscriptionSafely(subscriptionId);
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
          subscription = await cancelSubscriptionSafely(subscriptionId);
        }
      }
    }

    const item = subscription.items?.data?.[0];
    const monthlyAmountCents = item?.price?.unit_amount ?? null;
    if (monthlyAmountCents === null || monthlyAmountCents < MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS) {
      // Our checkouts always set unit_amount ≥ the minimum, so this is a
      // foreign or hand-made subscription; the schema CHECK would reject it.
      console.error(
        `[membership] subscription ${subscriptionId} has unusable amount ${monthlyAmountCents ?? "<none>"}; not recording`
      );
      return;
    }
    // API versions 2025-03-31+ carry the billing period on the item, not the
    // subscription.
    const currentPeriodEnd = item?.current_period_end ? epochToDate(item.current_period_end) : null;

    const upserted = await db.query<{ acknowledgment_sent_at: Date | null }>(
      `
        INSERT INTO public.billing_subscriptions (
          stripe_subscription_id, billing_customer_id, stripe_checkout_session_id,
          monthly_amount_cents, stripe_status, cancel_at_period_end,
          current_period_end, started_at, canceled_at
        )
        VALUES ($1, $2::uuid, $3, $4, $5, $6, $7, $8, $9)
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
          updated_at = now()
        RETURNING acknowledgment_sent_at
      `,
      [
        subscriptionId,
        customer.id,
        checkoutSessionId,
        monthlyAmountCents,
        subscription.status,
        subscription.cancel_at_period_end ?? false,
        currentPeriodEnd,
        epochToDate(subscription.start_date),
        subscription.canceled_at ? epochToDate(subscription.canceled_at) : null,
      ]
    );

    // §17602 post-purchase acknowledgment: once, when the subscription is
    // live and unacknowledged. A failed send leaves the column NULL and 5xxes
    // (see the catch below), so both Stripe's redelivery of this event and
    // any later subscription poke retry it.
    const acknowledgmentUserId =
      (subscription.status === "active" || subscription.status === "past_due") &&
      upserted.rows[0]?.acknowledgment_sent_at === null
        ? customer.user_id
        : null;
    if (acknowledgmentUserId !== null) {
      if (!options.sendMembershipStartedEmail) {
        console.warn(
          `[membership] no acknowledgment email sender configured; subscription ${subscriptionId} stays unacknowledged`
        );
        return;
      }
      try {
        const email = await lookupActiveUserEmail(acknowledgmentUserId);
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
        // for days; the upsert above is idempotent under the retry. A send
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

  return {
    async getMembership(userId) {
      const customer = await findBillingCustomerByUserId(userId);
      if (!customer) {
        return { enabled: true, membership: null, total_net_cents: 0, payments: [] };
      }
      const subscriptionResult = await db.query<{
        stripe_status: string;
        monthly_amount_cents: number;
        cancel_at_period_end: boolean;
        current_period_end: Date | null;
        started_at: Date;
      }>(
        `
          SELECT stripe_status, monthly_amount_cents, cancel_at_period_end,
                 current_period_end, started_at
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
      return {
        enabled: true,
        membership: subscription
          ? {
              stripe_status: subscription.stripe_status,
              monthly_amount_cents: subscription.monthly_amount_cents,
              cancel_at_period_end: subscription.cancel_at_period_end,
              current_period_end: subscription.current_period_end
                ? toIsoString(subscription.current_period_end)
                : null,
              started_at: toIsoString(subscription.started_at),
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
        throw new TypeError(
          `amount_cents must be an integer between ${MEMBERSHIP_CHECKOUT_MIN_AMOUNT_CENTS} and ${MEMBERSHIP_CHECKOUT_MAX_AMOUNT_CENTS}`
        );
      }
      if (input.kind !== "one_time" && input.kind !== "monthly") {
        throw new TypeError("kind must be one_time or monthly");
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
            "You already have a monthly membership. To change the amount, cancel it in Manage membership and subscribe again."
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
        success_url: `${settingsUrl}?membership=success`,
        cancel_url: `${settingsUrl}?membership=canceled`,
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

    async createPortalSession(userId) {
      const customer = await findBillingCustomerByUserId(userId);
      if (!customer) {
        return null;
      }
      const session = await stripe.billingPortal.sessions.create({
        customer: customer.stripe_customer_id,
        return_url: settingsUrl,
      });
      return { url: session.url };
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
        default:
          // The endpoint subscribes to exactly six event types; anything else
          // is a configuration drift worth a log line, not an error.
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
}
