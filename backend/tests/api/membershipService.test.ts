import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  createMembershipService,
  MembershipServiceError,
  MembershipWebhookRetryError,
  type MembershipChangedEmailInput,
  type MembershipStripeClient,
} from "../../src/api/membership/membershipService.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const BILLING_CUSTOMER_ID = "22222222-2222-4222-8222-222222222222";
const STRIPE_CUSTOMER_ID = "cus_test1";

// Routed query mock: the service issues plain single statements, so each
// test wires responses by SQL substring instead of call order (order is an
// implementation detail; the statements are not).
type DbRoute = [substring: string, respond: (params: unknown[]) => { rows?: unknown[]; rowCount?: number }];

// Every status read and subscription poke now also reads the amount-change
// table (docs/plans/membership-manage-page.md); it is empty for most tests,
// so that read answers no rows unless a test routes it first (earlier
// routes win).
const NO_AMOUNT_CHANGES: DbRoute = ["FROM public.billing_subscription_amount_changes", () => ({ rows: [] })];

function createRoutedDb(routes: DbRoute[]) {
  const query = vi.fn(async (sql: string, params: unknown[] = []) => {
    for (const [substring, respond] of [...routes, NO_AMOUNT_CHANGES]) {
      if (sql.includes(substring)) {
        const result = respond(params);
        return { rows: result.rows ?? [], rowCount: result.rowCount ?? result.rows?.length ?? 0 };
      }
    }
    throw new Error(`unrouted query in test: ${sql.replace(/\s+/g, " ").slice(0, 120)}`);
  });
  return { query };
}

const customerRow = (overrides: Record<string, unknown> = {}) => ({
  id: BILLING_CUSTOMER_ID,
  user_id: USER_ID,
  stripe_customer_id: STRIPE_CUSTOMER_ID,
  ...overrides,
});

function stripeSubscription(overrides: Record<string, unknown> = {}) {
  return {
    id: "sub_1",
    customer: STRIPE_CUSTOMER_ID,
    status: "active",
    cancel_at_period_end: false,
    canceled_at: null,
    start_date: 1_755_000_000,
    items: {
      data: [
        {
          id: "si_1",
          price: { unit_amount: 700, product: "prod_test", currency: "usd", recurring: { interval: "month" } },
          current_period_end: 1_757_600_000,
        },
      ],
    },
    ...overrides,
  };
}

function createStripeMock(overrides: Record<string, unknown> = {}): MembershipStripeClient & {
  checkout: { sessions: { create: ReturnType<typeof vi.fn> } };
  billingPortal: { sessions: { create: ReturnType<typeof vi.fn> } };
  customers: { create: ReturnType<typeof vi.fn>; update: ReturnType<typeof vi.fn> };
  subscriptions: {
    retrieve: ReturnType<typeof vi.fn>;
    update: ReturnType<typeof vi.fn>;
    cancel: ReturnType<typeof vi.fn>;
  };
  invoicePayments: { list: ReturnType<typeof vi.fn> };
  webhooks: { constructEvent: ReturnType<typeof vi.fn> };
} {
  return {
    checkout: {
      sessions: { create: vi.fn(async () => ({ id: "cs_1", url: "https://checkout.stripe.test/cs_1" })) },
    },
    billingPortal: { sessions: { create: vi.fn(async () => ({ url: "https://portal.stripe.test/ps_1" })) } },
    customers: {
      create: vi.fn(async () => ({ id: "cus_new" })),
      update: vi.fn(async () => ({ id: STRIPE_CUSTOMER_ID })),
    },
    subscriptions: {
      retrieve: vi.fn(async () => stripeSubscription()),
      update: vi.fn(async () => stripeSubscription()),
      cancel: vi.fn(async () => stripeSubscription({ status: "canceled", canceled_at: 1_755_900_000 })),
    },
    invoicePayments: { list: vi.fn(async () => ({ data: [] })) },
    webhooks: { constructEvent: vi.fn() },
    ...overrides,
  } as never;
}

function createService(input: {
  db: { query: ReturnType<typeof vi.fn> };
  stripe: MembershipStripeClient;
  sendMembershipStartedEmail?: ((i: { email: string; monthlyAmountCents: number }) => Promise<void>) | null;
  sendMembershipChangedEmail?: ((i: MembershipChangedEmailInput) => Promise<void>) | null;
}) {
  return createMembershipService({
    db: input.db as never,
    stripe: input.stripe,
    webhookSecret: "whsec_test",
    membershipProductId: "prod_test",
    publicBaseUrl: "https://site.test",
    sendMembershipStartedEmail: input.sendMembershipStartedEmail ?? null,
    sendMembershipChangedEmail: input.sendMembershipChangedEmail ?? null,
  });
}

function webhookEvent(type: string, object: unknown, created = 1_755_800_000) {
  return { type, created, data: { object } };
}

/** A stripe mock whose constructEvent returns the given event verbatim. */
function stripeDelivering(event: unknown, overrides: Record<string, unknown> = {}) {
  const stripe = createStripeMock(overrides);
  stripe.webhooks.constructEvent.mockReturnValue(event);
  return stripe;
}

const WEBHOOK_INPUT = { rawBody: Buffer.from("{}"), signatureHeader: "sig_header" };

describe("membership getMembership", () => {
  it("returns the empty enabled shape for a user with no billing customer", async () => {
    const db = createRoutedDb([["FROM public.billing_customers", () => ({ rows: [] })]]);
    const service = createService({ db, stripe: createStripeMock() });

    expect(await service.getMembership(USER_ID)).toEqual({
      enabled: true,
      membership: null,
      total_net_cents: 0,
      payments: [],
    });
  });

  it("returns the live subscription, payments, and refund-net total", async () => {
    const db = createRoutedDb([
      ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
      [
        "FROM public.billing_subscriptions",
        () => ({
          rows: [
            {
              stripe_status: "active",
              monthly_amount_cents: 700,
              cancel_at_period_end: false,
              current_period_end: new Date("2026-09-10T00:00:00Z"),
              started_at: new Date("2026-08-10T00:00:00Z"),
            },
          ],
        }),
      ],
      [
        "SUM(amount_cents - refunded_amount_cents)",
        () => ({ rows: [{ total_net_cents: 900 }] }),
      ],
      [
        "FROM public.billing_payments",
        () => ({
          rows: [
            {
              amount_cents: 700,
              refunded_amount_cents: 0,
              kind: "monthly",
              currency: "usd",
              paid_at: new Date("2026-08-10T00:00:00Z"),
            },
            {
              amount_cents: 500,
              refunded_amount_cents: 300,
              kind: "one_time",
              currency: "usd",
              paid_at: new Date("2026-08-01T00:00:00Z"),
            },
          ],
        }),
      ],
    ]);
    const service = createService({ db, stripe: createStripeMock() });

    const result = await service.getMembership(USER_ID);
    expect(result.membership).toEqual({
      stripe_status: "active",
      monthly_amount_cents: 700,
      cancel_at_period_end: false,
      current_period_end: "2026-09-10T00:00:00.000Z",
      started_at: "2026-08-10T00:00:00.000Z",
      pending_amount_change: null,
    });
    expect(result.total_net_cents).toBe(900);
    expect(result.payments).toHaveLength(2);
    expect(result.payments[1]).toMatchObject({ amount_cents: 500, refunded_amount_cents: 300, kind: "one_time" });
  });
});

describe("membership createCheckoutSession", () => {
  const dbWithCustomer = (extraRoutes: DbRoute[] = []) =>
    createRoutedDb([
      ["SELECT email", () => ({ rows: [{ email: "user@example.com" }] })],
      ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
      ["SELECT stripe_subscription_id", () => ({ rows: [] })],
      ...extraRoutes,
    ]);

  it("rejects amounts below the $5 minimum and above the $1,000 cap", async () => {
    const service = createService({ db: dbWithCustomer(), stripe: createStripeMock() });
    await expect(service.createCheckoutSession(USER_ID, { kind: "one_time", amount_cents: 499 })).rejects.toThrow(
      TypeError
    );
    await expect(service.createCheckoutSession(USER_ID, { kind: "monthly", amount_cents: 100_001 })).rejects.toThrow(
      TypeError
    );
    await expect(service.createCheckoutSession(USER_ID, { kind: "monthly", amount_cents: 10.5 })).rejects.toThrow(
      TypeError
    );
  });

  it("creates a one-time session with payment-intent metadata and no consent collection", async () => {
    const stripe = createStripeMock();
    const service = createService({ db: dbWithCustomer(), stripe });

    const result = await service.createCheckoutSession(USER_ID, { kind: "one_time", amount_cents: 2500 });
    expect(result).toEqual({ url: "https://checkout.stripe.test/cs_1" });

    const params = stripe.checkout.sessions.create.mock.calls[0]?.[0];
    expect(params).toMatchObject({
      mode: "payment",
      customer: STRIPE_CUSTOMER_ID,
      client_reference_id: BILLING_CUSTOMER_ID,
      payment_method_types: ["card"],
      success_url: "https://site.test/support/once?membership=success",
      cancel_url: "https://site.test/support/once?membership=canceled",
      payment_intent_data: { metadata: { billing_customer_id: BILLING_CUSTOMER_ID, kind: "one_time" } },
    });
    expect(params.line_items[0].price_data).toEqual({
      currency: "usd",
      product: "prod_test",
      unit_amount: 2500,
    });
    expect(params.consent_collection).toBeUndefined();
    // A stale tab cannot complete a forgotten session hours later.
    expect(params.expires_at).toBeGreaterThan(Math.floor(Date.now() / 1000));
    expect(params.expires_at).toBeLessThanOrEqual(Math.floor(Date.now() / 1000) + 30 * 60);
  });

  it("creates a monthly session with recurring price, §17602 consent collection, and renewal terms", async () => {
    const stripe = createStripeMock();
    const service = createService({ db: dbWithCustomer(), stripe });

    await service.createCheckoutSession(USER_ID, { kind: "monthly", amount_cents: 700 });

    const params = stripe.checkout.sessions.create.mock.calls[0]?.[0];
    expect(params.mode).toBe("subscription");
    expect(params.success_url).toBe("https://site.test/support/member?membership=success");
    expect(params.cancel_url).toBe("https://site.test/support/member?membership=canceled");
    expect(params.line_items[0].price_data.recurring).toEqual({ interval: "month" });
    expect(params.subscription_data).toEqual({
      metadata: { billing_customer_id: BILLING_CUSTOMER_ID, kind: "monthly" },
    });
    expect(params.payment_intent_data).toBeUndefined();
    expect(params.consent_collection).toEqual({ terms_of_service: "required" });
    const consentMessage = params.custom_text.terms_of_service_acceptance.message;
    expect(consentMessage).toContain("$7.00");
    expect(consentMessage).toContain("each month until you cancel");
    expect(consentMessage).toContain("https://site.test/terms");
  });

  it("refuses a monthly checkout while a live subscription exists", async () => {
    const db = createRoutedDb([
      ["SELECT email", () => ({ rows: [{ email: "user@example.com" }] })],
      ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
      ["SELECT stripe_subscription_id", () => ({ rows: [{ stripe_subscription_id: "sub_live" }] })],
    ]);
    const stripe = createStripeMock();
    const service = createService({ db, stripe });

    await expect(service.createCheckoutSession(USER_ID, { kind: "monthly", amount_cents: 700 })).rejects.toThrow(
      MembershipServiceError
    );
    expect(stripe.checkout.sessions.create).not.toHaveBeenCalled();
  });

  it("still allows a one-time payment while a subscription is live", async () => {
    const db = createRoutedDb([
      ["SELECT email", () => ({ rows: [{ email: "user@example.com" }] })],
      ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
      ["SELECT stripe_subscription_id", () => ({ rows: [{ stripe_subscription_id: "sub_live" }] })],
    ]);
    const stripe = createStripeMock();
    const service = createService({ db, stripe });

    await expect(service.createCheckoutSession(USER_ID, { kind: "one_time", amount_cents: 700 })).resolves.toEqual({
      url: "https://checkout.stripe.test/cs_1",
    });
  });

  it("refreshes the Stripe customer's email on checkout for an existing customer", async () => {
    const stripe = createStripeMock();
    const service = createService({ db: dbWithCustomer(), stripe });

    await service.createCheckoutSession(USER_ID, { kind: "one_time", amount_cents: 500 });

    // Checkout locks the customer's email and receipts follow it, so a stale
    // address self-heals here even if the email-change sync hook failed.
    expect(stripe.customers.update).toHaveBeenCalledWith(STRIPE_CUSTOMER_ID, { email: "user@example.com" });
  });

  it("creates the Stripe customer and billing row on first checkout", async () => {
    const db = createRoutedDb([
      ["SELECT email", () => ({ rows: [{ email: "user@example.com" }] })],
      ["INSERT INTO public.billing_customers", () => ({ rows: [customerRow({ stripe_customer_id: "cus_new" })] })],
      ["FROM public.billing_customers", () => ({ rows: [] })],
      ["SELECT stripe_subscription_id", () => ({ rows: [] })],
    ]);
    const stripe = createStripeMock();
    const service = createService({ db, stripe });

    await service.createCheckoutSession(USER_ID, { kind: "one_time", amount_cents: 500 });

    expect(stripe.customers.create).toHaveBeenCalledWith({ email: "user@example.com" });
    // A just-created customer already carries the current email; no refresh.
    expect(stripe.customers.update).not.toHaveBeenCalled();
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.billing_customers"));
    expect(insert?.[1]).toEqual([USER_ID, "cus_new"]);
  });

  it("rejects checkout for a deleted or unknown user", async () => {
    const db = createRoutedDb([["SELECT email", () => ({ rows: [] })]]);
    const service = createService({ db, stripe: createStripeMock() });

    await expect(service.createCheckoutSession(USER_ID, { kind: "one_time", amount_cents: 500 })).rejects.toThrow(
      MembershipServiceError
    );
  });
});

describe("membership createPortalSession", () => {
  it("returns null (404) without a billing customer", async () => {
    const db = createRoutedDb([["FROM public.billing_customers", () => ({ rows: [] })]]);
    const service = createService({ db, stripe: createStripeMock() });
    expect(await service.createPortalSession(USER_ID)).toBeNull();
  });

  it("returns the portal URL for an existing billing customer", async () => {
    const db = createRoutedDb([["FROM public.billing_customers", () => ({ rows: [customerRow()] })]]);
    const stripe = createStripeMock();
    const service = createService({ db, stripe });

    expect(await service.createPortalSession(USER_ID)).toEqual({ url: "https://portal.stripe.test/ps_1" });
    expect(stripe.billingPortal.sessions.create).toHaveBeenCalledWith({
      customer: STRIPE_CUSTOMER_ID,
      return_url: "https://site.test/me/settings",
    });
  });

  it("opens the general portal for an explicit { flow: null } (what shipped clients send)", async () => {
    const db = createRoutedDb([["FROM public.billing_customers", () => ({ rows: [customerRow()] })]]);
    const stripe = createStripeMock();
    const service = createService({ db, stripe });

    await service.createPortalSession(USER_ID, { flow: null });
    expect(stripe.billingPortal.sessions.create).toHaveBeenCalledWith({
      customer: STRIPE_CUSTOMER_ID,
      return_url: "https://site.test/me/settings",
    });
  });

  it("deep-links into the payment-method flow and returns to the membership page", async () => {
    const db = createRoutedDb([["FROM public.billing_customers", () => ({ rows: [customerRow()] })]]);
    const stripe = createStripeMock();
    const service = createService({ db, stripe });

    await service.createPortalSession(USER_ID, { flow: "payment_method_update" });
    expect(stripe.billingPortal.sessions.create).toHaveBeenCalledWith({
      customer: STRIPE_CUSTOMER_ID,
      return_url: "https://site.test/me/membership",
      flow_data: {
        type: "payment_method_update",
        after_completion: { type: "redirect", redirect: { return_url: "https://site.test/me/membership" } },
      },
    });
  });
});

// Manage-page actions (docs/plans/membership-manage-page.md).
const PERIOD_END_EPOCH = 1_757_600_000;

/** Routes for the manage path: customer + live row lookups, the pure upsert,
 * the email lookup, and the getMembership read that answers the action. */
const manageRoutes = (opts: { noCustomer?: boolean; noLive?: boolean; acknowledgmentSentAt?: Date | null } = {}): DbRoute[] => [
  ["FROM public.billing_customers", () => ({ rows: opts.noCustomer ? [] : [customerRow()] })],
  ["SELECT stripe_subscription_id", () => ({ rows: opts.noLive ? [] : [{ stripe_subscription_id: "sub_1" }] })],
  [
    "INSERT INTO public.billing_subscriptions",
    () => ({ rows: [{ acknowledgment_sent_at: opts.acknowledgmentSentAt === undefined ? new Date() : opts.acknowledgmentSentAt }] }),
  ],
  ["SELECT email", () => ({ rows: [{ email: "user@example.com" }] })],
  [
    "SELECT stripe_status",
    () => ({
      rows: [
        {
          stripe_status: "active",
          monthly_amount_cents: 700,
          cancel_at_period_end: true,
          current_period_end: new Date(PERIOD_END_EPOCH * 1000),
          started_at: new Date(1_755_000_000 * 1000),
        },
      ],
    }),
  ],
  ["SUM(amount_cents - refunded_amount_cents)", () => ({ rows: [{ total_net_cents: 0 }] })],
  ["FROM public.billing_payments", () => ({ rows: [] })],
];

function upsertParams(db: { query: ReturnType<typeof vi.fn> }): unknown[] | undefined {
  return db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.billing_subscriptions"))?.[1] as
    | unknown[]
    | undefined;
}

describe("membership cancelMembership", () => {
  it("throws no_membership without a billing customer or live subscription", async () => {
    const stripe = createStripeMock();
    const noCustomer = createService({ db: createRoutedDb(manageRoutes({ noCustomer: true })), stripe });
    await expect(noCustomer.cancelMembership(USER_ID)).rejects.toMatchObject({ code: "no_membership" });

    const noLive = createService({ db: createRoutedDb(manageRoutes({ noLive: true })), stripe });
    await expect(noLive.cancelMembership(USER_ID)).rejects.toMatchObject({ code: "no_membership" });
    expect(stripe.subscriptions.retrieve).not.toHaveBeenCalled();
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
  });

  it("schedules a period-end cancel at Stripe, records the returned state, emails, and answers the fresh status", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    // Stripe answers the update with the scheduled cancel in its current
    // representation (cancel_at, boolean still false).
    stripe.subscriptions.update.mockResolvedValue(stripeSubscription({ cancel_at: PERIOD_END_EPOCH }));
    const sendMembershipChangedEmail = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipChangedEmail });

    const result = await service.cancelMembership(USER_ID);

    expect(stripe.subscriptions.retrieve).toHaveBeenCalledWith("sub_1");
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", { cancel_at_period_end: true });
    expect(upsertParams(db)?.[5]).toBe(true);
    expect(sendMembershipChangedEmail).toHaveBeenCalledWith({
      kind: "canceled",
      email: "user@example.com",
      endsAt: new Date(PERIOD_END_EPOCH * 1000),
    });
    expect(result.membership).toMatchObject({ cancel_at_period_end: true });
  });

  it("is a no-op when already scheduled to cancel (portal cancel, or a retry after a lost response)", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ cancel_at: PERIOD_END_EPOCH }));
    const sendMembershipChangedEmail = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipChangedEmail });

    await expect(service.cancelMembership(USER_ID)).resolves.toMatchObject({ enabled: true });
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
    expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
    // The row still catches up with what Stripe has.
    expect(upsertParams(db)?.[5]).toBe(true);
  });

  it("refuses while the first payment is still confirming", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ status: "incomplete" }));
    const service = createService({ db, stripe });

    await expect(service.cancelMembership(USER_ID)).rejects.toMatchObject({ code: "membership_pending" });
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
  });

  it("refuses a subscription that no longer matches our records (product or customer)", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      for (const overrides of [{ items: { data: [{ price: { unit_amount: 700, product: "prod_other" } }] } }, { customer: "cus_other" }]) {
        const db = createRoutedDb(manageRoutes());
        const stripe = createStripeMock();
        stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription(overrides));
        const service = createService({ db, stripe });

        await expect(service.cancelMembership(USER_ID)).rejects.toMatchObject({ code: "membership_conflict" });
        expect(stripe.subscriptions.update).not.toHaveBeenCalled();
        expect(upsertParams(db)).toBeUndefined();
      }
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("records a subscription found already terminal and reports no membership", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ status: "canceled", canceled_at: 1_755_900_000 }));
    const service = createService({ db, stripe });

    await expect(service.cancelMembership(USER_ID)).rejects.toMatchObject({ code: "no_membership" });
    expect(upsertParams(db)?.[4]).toBe("canceled");
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
  });

  it("still succeeds when the confirmation email fails, and never sends the §17602 start acknowledgment", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      // acknowledgment_sent_at NULL: the webhook path would send the start
      // acknowledgment here; the manage path must not.
      const db = createRoutedDb(manageRoutes({ acknowledgmentSentAt: null }));
      const stripe = createStripeMock();
      stripe.subscriptions.update.mockResolvedValue(stripeSubscription({ cancel_at: PERIOD_END_EPOCH }));
      const sendMembershipStartedEmail = vi.fn(async () => {});
      const sendMembershipChangedEmail = vi.fn(async () => {
        throw new Error("SES down");
      });
      const service = createService({ db, stripe, sendMembershipStartedEmail, sendMembershipChangedEmail });

      await expect(service.cancelMembership(USER_ID)).resolves.toMatchObject({ enabled: true });
      expect(stripe.subscriptions.update).toHaveBeenCalledTimes(1);
      expect(sendMembershipStartedEmail).not.toHaveBeenCalled();
      expect(warnSpy).toHaveBeenCalled();
    } finally {
      warnSpy.mockRestore();
    }
  });
});

describe("membership resumeMembership", () => {
  it("clears a scheduled cancel_at (how current API versions store a period-end cancel), verifies, emails", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ cancel_at: PERIOD_END_EPOCH }));
    stripe.subscriptions.update.mockResolvedValue(stripeSubscription());
    const sendMembershipChangedEmail = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipChangedEmail });

    await expect(service.resumeMembership(USER_ID)).resolves.toMatchObject({ enabled: true });
    expect(stripe.subscriptions.update).toHaveBeenCalledTimes(1);
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", { cancel_at: "" });
    expect(upsertParams(db)?.[5]).toBe(false);
    expect(sendMembershipChangedEmail).toHaveBeenCalledWith({
      kind: "resumed",
      email: "user@example.com",
      monthlyAmountCents: 700,
      renewsAt: new Date(PERIOD_END_EPOCH * 1000),
    });
  });

  it("clears the legacy cancel_at_period_end boolean", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ cancel_at_period_end: true }));
    const service = createService({ db, stripe });

    await service.resumeMembership(USER_ID);
    expect(stripe.subscriptions.update).toHaveBeenCalledTimes(1);
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", { cancel_at_period_end: false });
  });

  it("clears both forms when both are set, cancel_at first", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    stripe.subscriptions.retrieve.mockResolvedValue(
      stripeSubscription({ cancel_at: PERIOD_END_EPOCH, cancel_at_period_end: true })
    );
    stripe.subscriptions.update
      .mockResolvedValueOnce(stripeSubscription({ cancel_at_period_end: true }))
      .mockResolvedValueOnce(stripeSubscription());
    const service = createService({ db, stripe });

    await service.resumeMembership(USER_ID);
    expect(stripe.subscriptions.update.mock.calls).toEqual([
      ["sub_1", { cancel_at: "" }],
      ["sub_1", { cancel_at_period_end: false }],
    ]);
    expect(upsertParams(db)?.[5]).toBe(false);
  });

  it("fails without recording or emailing when Stripe still reports a scheduled cancel after clearing", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const db = createRoutedDb(manageRoutes());
      const stripe = createStripeMock();
      stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ cancel_at: PERIOD_END_EPOCH }));
      stripe.subscriptions.update.mockResolvedValue(stripeSubscription({ cancel_at: PERIOD_END_EPOCH }));
      const sendMembershipChangedEmail = vi.fn(async () => {});
      const service = createService({ db, stripe, sendMembershipChangedEmail });

      await expect(service.resumeMembership(USER_ID)).rejects.toMatchObject({ code: "membership_update_failed" });
      expect(upsertParams(db)).toBeUndefined();
      expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("is a no-op when not scheduled to cancel (retry after a lost response)", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    const sendMembershipChangedEmail = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipChangedEmail });

    await expect(service.resumeMembership(USER_ID)).resolves.toMatchObject({ enabled: true });
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
    expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
    expect(upsertParams(db)?.[5]).toBe(false);
  });

  it("throws no_membership without a live subscription", async () => {
    const stripe = createStripeMock();
    const service = createService({ db: createRoutedDb(manageRoutes({ noLive: true })), stripe });
    await expect(service.resumeMembership(USER_ID)).rejects.toMatchObject({ code: "no_membership" });
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
  });
});

describe("membership webhook: signature", () => {
  it("reports bad_signature when the header is missing or verification fails", async () => {
    const stripe = createStripeMock();
    stripe.webhooks.constructEvent.mockImplementation(() => {
      throw new Error("No signatures found matching the expected signature for payload");
    });
    const service = createService({ db: createRoutedDb([]), stripe });

    expect(await service.handleWebhookEvent({ rawBody: Buffer.from("{}"), signatureHeader: null })).toBe(
      "bad_signature"
    );
    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("bad_signature");
  });

  it("ignores unknown event types", async () => {
    const stripe = stripeDelivering(webhookEvent("customer.created", {}));
    const db = createRoutedDb([]);
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(db.query).not.toHaveBeenCalled();
  });
});

describe("membership webhook: one-time checkout completion", () => {
  const paymentSession = (overrides: Record<string, unknown> = {}) => ({
    id: "cs_1",
    mode: "payment",
    client_reference_id: BILLING_CUSTOMER_ID,
    payment_intent: "pi_1",
    amount_total: 2500,
    currency: "usd",
    ...overrides,
  });

  it("inserts an idempotent one-time ledger row keyed by payment intent", async () => {
    const db = createRoutedDb([
      ["SELECT id::text AS id FROM public.billing_customers", () => ({ rows: [{ id: BILLING_CUSTOMER_ID }] })],
      ["INSERT INTO public.billing_payments", () => ({ rowCount: 1 })],
    ]);
    const stripe = stripeDelivering(webhookEvent("checkout.session.completed", paymentSession()));
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.billing_payments"));
    expect(String(insert?.[0])).toContain("ON CONFLICT (stripe_payment_ref) DO NOTHING");
    // ref and refund join key are the same value for one-time payments.
    expect(insert?.[1]).toEqual([BILLING_CUSTOMER_ID, 2500, "usd", "pi_1", new Date(1_755_800_000 * 1000)]);
  });

  it("acknowledges (200) and skips a session for an unknown billing customer", async () => {
    const db = createRoutedDb([
      ["SELECT id::text AS id FROM public.billing_customers", () => ({ rows: [] })],
    ]);
    const stripe = stripeDelivering(webhookEvent("checkout.session.completed", paymentSession()));
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.billing_payments"))).toBe(
      false
    );
  });
});

describe("membership webhook: subscription sync (poke pattern)", () => {
  const subscriptionUpsertRoutes = (opts: { acknowledgmentSentAt?: Date | null; customer?: Record<string, unknown> } = {}): DbRoute[] => [
    ["WHERE stripe_customer_id", () => ({ rows: [customerRow(opts.customer ?? {})] })],
    ["SELECT stripe_subscription_id", () => ({ rows: [] })],
    [
      "INSERT INTO public.billing_subscriptions",
      () => ({ rows: [{ acknowledgment_sent_at: opts.acknowledgmentSentAt ?? new Date() }] }),
    ],
    ["SET acknowledgment_sent_at", () => ({ rowCount: 1 })],
    ["SELECT email", () => ({ rows: [{ email: "user@example.com" }] })],
  ];

  it("writes the FETCHED subscription state, not the event snapshot", async () => {
    const db = createRoutedDb(subscriptionUpsertRoutes());
    const stripe = stripeDelivering(
      // Stale snapshot in the event: status incomplete.
      webhookEvent("customer.subscription.updated", stripeSubscription({ status: "incomplete" }))
    );
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ status: "active" }));
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    expect(stripe.subscriptions.retrieve).toHaveBeenCalledWith("sub_1");
    const upsert = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.billing_subscriptions")
    );
    // status param carries the fetched "active", not the snapshot.
    expect(upsert?.[1]).toEqual([
      "sub_1",
      BILLING_CUSTOMER_ID,
      null,
      700,
      "active",
      false,
      new Date(1_757_600_000 * 1000),
      new Date(1_755_000_000 * 1000),
      null,
      // stripe_synced_at: when this state was known current at Stripe.
      expect.any(Date),
    ]);
    // Consent pointers survive later pokes.
    expect(String(upsert?.[0])).toContain("COALESCE");
    // Stale-write guard: an older poke's write never lands over a newer one.
    expect(String(upsert?.[0])).toContain("stripe_synced_at <= EXCLUDED.stripe_synced_at");
  });

  it("records a portal period-end cancel that arrives as cancel_at with cancel_at_period_end false", async () => {
    // Current API versions express "cancel at end of billing period" as a
    // scheduled cancel_at timestamp; the legacy boolean stays false.
    const db = createRoutedDb(subscriptionUpsertRoutes());
    const stripe = stripeDelivering(webhookEvent("customer.subscription.updated", stripeSubscription()));
    stripe.subscriptions.retrieve.mockResolvedValue(
      stripeSubscription({ cancel_at_period_end: false, cancel_at: 1_757_600_000 })
    );
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    const upsert = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.billing_subscriptions")
    );
    expect(upsert?.[1]?.[5]).toBe(true);
  });

  it("stores the checkout session id as the consent pointer on subscription-mode completion", async () => {
    const db = createRoutedDb(subscriptionUpsertRoutes());
    const stripe = stripeDelivering(
      webhookEvent("checkout.session.completed", {
        id: "cs_sub_1",
        mode: "subscription",
        subscription: "sub_1",
        consent: { terms_of_service: "accepted" },
      })
    );
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    const upsert = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.billing_subscriptions")
    );
    expect(upsert?.[1]?.[2]).toBe("cs_sub_1");
    // No ledger row from the completion event — invoice.paid owns that.
    expect(db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.billing_payments"))).toBe(
      false
    );
  });

  it("fails closed on a completion WITHOUT consent: cancels the subscription and records it canceled", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const db = createRoutedDb(subscriptionUpsertRoutes());
      const stripe = stripeDelivering(
        webhookEvent("checkout.session.completed", {
          id: "cs_sub_1",
          mode: "subscription",
          subscription: "sub_1",
          consent: null,
        })
      );
      // The post-cancel poke fetches the canceled state.
      stripe.subscriptions.retrieve.mockResolvedValue(
        stripeSubscription({ status: "canceled", canceled_at: 1_755_900_000 })
      );
      const service = createService({ db, stripe });

      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

      // §17602 fails closed: no consent evidence, no continued billing.
      expect(stripe.subscriptions.cancel).toHaveBeenCalledWith("sub_1");
      const upsert = db.query.mock.calls.find((call) =>
        String(call[0]).includes("INSERT INTO public.billing_subscriptions")
      );
      expect(upsert?.[1]?.[2]).toBeNull();
      expect(upsert?.[1]?.[4]).toBe("canceled");
      expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("WITHOUT terms-of-service consent"));
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("does not cancel a foreign product's subscription on a consent-less completion", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const db = createRoutedDb([]);
      const stripe = stripeDelivering(
        webhookEvent("checkout.session.completed", {
          id: "cs_foreign",
          mode: "subscription",
          subscription: "sub_foreign",
          consent: null,
        })
      );
      stripe.subscriptions.retrieve.mockResolvedValue(
        stripeSubscription({
          id: "sub_foreign",
          items: { data: [{ price: { unit_amount: 700, product: "prod_other" }, current_period_end: 1_757_600_000 }] },
        })
      );
      const service = createService({ db, stripe });

      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
      // Ownership is verified BEFORE the fail-closed cancel fires.
      expect(stripe.subscriptions.cancel).not.toHaveBeenCalled();
      expect(db.query).not.toHaveBeenCalled();
      expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("not the membership product"));
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("ignores a subscription on a different Stripe product (no record, no guard cancels)", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const db = createRoutedDb([]);
      const stripe = stripeDelivering(webhookEvent("customer.subscription.updated", stripeSubscription()));
      stripe.subscriptions.retrieve.mockResolvedValue(
        stripeSubscription({
          items: { data: [{ price: { unit_amount: 700, product: "prod_other" }, current_period_end: 1_757_600_000 }] },
        })
      );
      const service = createService({ db, stripe });

      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
      expect(db.query).not.toHaveBeenCalled();
      expect(stripe.subscriptions.cancel).not.toHaveBeenCalled();
      expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("not the membership product"));
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("cancels a subscription arriving for a deleted account (user_id NULL)", async () => {
    const db = createRoutedDb(subscriptionUpsertRoutes({ customer: { user_id: null } }));
    const stripe = stripeDelivering(webhookEvent("customer.subscription.created", stripeSubscription()));
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    expect(stripe.subscriptions.cancel).toHaveBeenCalledWith("sub_1");
    const upsert = db.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.billing_subscriptions")
    );
    // The canceled state is what lands.
    expect(upsert?.[1]?.[4]).toBe("canceled");
  });

  it("cancels a newly arrived duplicate while another subscription is live", async () => {
    const db = createRoutedDb([
      ["WHERE stripe_customer_id", () => ({ rows: [customerRow()] })],
      ["SELECT stripe_subscription_id", () => ({ rows: [{ stripe_subscription_id: "sub_existing" }] })],
      [
        "INSERT INTO public.billing_subscriptions",
        () => ({ rows: [{ acknowledgment_sent_at: new Date() }] }),
      ],
    ]);
    const stripe = stripeDelivering(webhookEvent("customer.subscription.created", stripeSubscription({ id: "sub_2" })));
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ id: "sub_2" }));
    stripe.subscriptions.cancel.mockResolvedValue(
      stripeSubscription({ id: "sub_2", status: "canceled", canceled_at: 1_755_900_000 })
    );
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(stripe.subscriptions.cancel).toHaveBeenCalledWith("sub_2");
  });

  it("ignores a subscription for a Stripe customer we do not know", async () => {
    const db = createRoutedDb([["WHERE stripe_customer_id", () => ({ rows: [] })]]);
    const stripe = stripeDelivering(webhookEvent("customer.subscription.updated", stripeSubscription()));
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const service = createService({ db, stripe });
      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
      expect(
        db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.billing_subscriptions"))
      ).toBe(false);
    } finally {
      errorSpy.mockRestore();
    }
  });
});

describe("membership webhook: acknowledgment email (§17602)", () => {
  const routes = (acknowledgmentSentAt: Date | null): DbRoute[] => [
    ["WHERE stripe_customer_id", () => ({ rows: [customerRow()] })],
    ["SELECT stripe_subscription_id", () => ({ rows: [] })],
    ["INSERT INTO public.billing_subscriptions", () => ({ rows: [{ acknowledgment_sent_at: acknowledgmentSentAt }] })],
    ["SET acknowledgment_sent_at", () => ({ rowCount: 1 })],
    ["SELECT email", () => ({ rows: [{ email: "user@example.com" }] })],
  ];

  it("sends once and stamps acknowledgment_sent_at when the subscription is live and unacknowledged", async () => {
    const db = createRoutedDb(routes(null));
    const stripe = stripeDelivering(webhookEvent("customer.subscription.updated", stripeSubscription()));
    const sender = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipStartedEmail: sender });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    expect(sender).toHaveBeenCalledWith({ email: "user@example.com", monthlyAmountCents: 700 });
    const stamp = db.query.mock.calls.find((call) => String(call[0]).includes("SET acknowledgment_sent_at"));
    expect(String(stamp?.[0])).toContain("acknowledgment_sent_at IS NULL");
  });

  it("does not resend once acknowledged", async () => {
    const db = createRoutedDb(routes(new Date("2026-08-20T00:00:00Z")));
    const stripe = stripeDelivering(webhookEvent("customer.subscription.updated", stripeSubscription()));
    const sender = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipStartedEmail: sender });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(sender).not.toHaveBeenCalled();
  });

  it("fails the webhook retryably on a failed send; the stamp stays unset so the redelivery re-attempts", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const db = createRoutedDb(routes(null));
      const stripe = stripeDelivering(webhookEvent("customer.subscription.updated", stripeSubscription()));
      const sender = vi.fn(async () => {
        throw new Error("ses down");
      });
      const service = createService({ db, stripe, sendMembershipStartedEmail: sender });

      // §17602 notice must not be dropped with a 200: 5xx makes Stripe
      // redeliver this very event instead of waiting for a later poke.
      await expect(service.handleWebhookEvent(WEBHOOK_INPUT)).rejects.toThrow(MembershipWebhookRetryError);
      expect(db.query.mock.calls.some((call) => String(call[0]).includes("SET acknowledgment_sent_at"))).toBe(false);
    } finally {
      warnSpy.mockRestore();
    }
  });
});

describe("membership webhook: invoice.paid", () => {
  const invoice = (overrides: Record<string, unknown> = {}) => ({
    id: "in_1",
    customer: STRIPE_CUSTOMER_ID,
    amount_paid: 700,
    currency: "usd",
    status_transitions: { paid_at: 1_755_810_000 },
    ...overrides,
  });

  it("records a monthly ledger row with the intent resolved from the invoice payments", async () => {
    const db = createRoutedDb([
      ["WHERE stripe_customer_id", () => ({ rows: [customerRow()] })],
      ["SELECT 1 FROM public.billing_payments", () => ({ rows: [] })],
      ["INSERT INTO public.billing_payments", () => ({ rowCount: 1 })],
    ]);
    const stripe = stripeDelivering(webhookEvent("invoice.paid", invoice()));
    stripe.invoicePayments.list.mockResolvedValue({
      data: [{ status: "paid", payment: { type: "payment_intent", payment_intent: "pi_month_1" } }],
    });
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    expect(stripe.invoicePayments.list).toHaveBeenCalledWith({ invoice: "in_1" });
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.billing_payments"));
    expect(insert?.[1]).toEqual([BILLING_CUSTOMER_ID, 700, "usd", "in_1", "pi_month_1", new Date(1_755_810_000 * 1000)]);
  });

  it("skips the payments lookup entirely on a retry of an already-recorded invoice", async () => {
    const db = createRoutedDb([
      ["WHERE stripe_customer_id", () => ({ rows: [customerRow()] })],
      ["SELECT 1 FROM public.billing_payments", () => ({ rows: [{ "?column?": 1 }] })],
    ]);
    const stripe = stripeDelivering(webhookEvent("invoice.paid", invoice()));
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(stripe.invoicePayments.list).not.toHaveBeenCalled();
  });

  it("throws a retryable error when the invoice has no succeeded payment yet", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const db = createRoutedDb([
        ["WHERE stripe_customer_id", () => ({ rows: [customerRow()] })],
        ["SELECT 1 FROM public.billing_payments", () => ({ rows: [] })],
      ]);
      const stripe = stripeDelivering(webhookEvent("invoice.paid", invoice()));
      stripe.invoicePayments.list.mockResolvedValue({ data: [{ status: "open", payment: { type: "payment_intent", payment_intent: "pi_x" } }] });
      const service = createService({ db, stripe });

      await expect(service.handleWebhookEvent(WEBHOOK_INPUT)).rejects.toThrow(MembershipWebhookRetryError);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("ignores zero-amount invoices", async () => {
    const db = createRoutedDb([]);
    const stripe = stripeDelivering(webhookEvent("invoice.paid", invoice({ amount_paid: 0 })));
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(db.query).not.toHaveBeenCalled();
  });
});

describe("membership webhook: charge.refunded", () => {
  const charge = (overrides: Record<string, unknown> = {}) => ({
    id: "ch_1",
    payment_intent: "pi_1",
    amount_refunded: 300,
    ...overrides,
  });

  it("SETs the cumulative refunded amount (idempotent under retries and partial refunds)", async () => {
    const db = createRoutedDb([["UPDATE public.billing_payments", () => ({ rowCount: 1 })]]);
    const stripe = stripeDelivering(webhookEvent("charge.refunded", charge()));
    const service = createService({ db, stripe });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    const update = db.query.mock.calls.find((call) => String(call[0]).includes("UPDATE public.billing_payments"));
    expect(String(update?.[0])).toContain("SET refunded_amount_cents = $2");
    expect(String(update?.[0])).not.toContain("refunded_amount_cents +");
    expect(update?.[1]).toEqual(["pi_1", 300]);
  });

  it("throws a retryable error when the ledger row does not exist yet", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const db = createRoutedDb([["UPDATE public.billing_payments", () => ({ rowCount: 0 })]]);
      const stripe = stripeDelivering(webhookEvent("charge.refunded", charge()));
      const service = createService({ db, stripe });

      await expect(service.handleWebhookEvent(WEBHOOK_INPUT)).rejects.toThrow(MembershipWebhookRetryError);
    } finally {
      warnSpy.mockRestore();
    }
  });
});

describe("membership syncCustomerEmail", () => {
  it("pushes the current account email onto the Stripe customer", async () => {
    const db = createRoutedDb([
      ["SELECT email", () => ({ rows: [{ email: "new@example.com" }] })],
      ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
    ]);
    const stripe = createStripeMock();
    const service = createService({ db, stripe });

    await service.syncCustomerEmail(USER_ID);
    expect(stripe.customers.update).toHaveBeenCalledWith(STRIPE_CUSTOMER_ID, { email: "new@example.com" });
  });

  it("no-ops for a user without a billing customer", async () => {
    const db = createRoutedDb([["FROM public.billing_customers", () => ({ rows: [] })]]);
    const stripe = createStripeMock();
    const service = createService({ db, stripe });

    await service.syncCustomerEmail(USER_ID);
    expect(stripe.customers.update).not.toHaveBeenCalled();
  });
});

describe("membership cancelSubscriptionsForAccountDeletion", () => {
  it("no-ops without a billing customer or live subscription", async () => {
    const stripe = createStripeMock();
    const noCustomer = createService({
      db: createRoutedDb([["FROM public.billing_customers", () => ({ rows: [] })]]),
      stripe,
    });
    await noCustomer.cancelSubscriptionsForAccountDeletion(USER_ID);

    const noLive = createService({
      db: createRoutedDb([
        ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
        ["SELECT stripe_subscription_id", () => ({ rows: [] })],
      ]),
      stripe,
    });
    await noLive.cancelSubscriptionsForAccountDeletion(USER_ID);
    expect(stripe.subscriptions.cancel).not.toHaveBeenCalled();
  });

  it("cancels the live subscription at Stripe", async () => {
    const stripe = createStripeMock();
    const service = createService({
      db: createRoutedDb([
        ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
        ["SELECT stripe_subscription_id", () => ({ rows: [{ stripe_subscription_id: "sub_live" }] })],
      ]),
      stripe,
    });

    await expect(service.cancelSubscriptionsForAccountDeletion(USER_ID)).resolves.toBe(true);
    expect(stripe.subscriptions.cancel).toHaveBeenCalledWith("sub_live");
  });

  it("treats an already-canceled subscription as success (idempotent retry)", async () => {
    const stripe = createStripeMock();
    stripe.subscriptions.cancel.mockRejectedValue(new Error("This subscription is canceled"));
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ id: "sub_live", status: "canceled" }));
    const service = createService({
      db: createRoutedDb([
        ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
        ["SELECT stripe_subscription_id", () => ({ rows: [{ stripe_subscription_id: "sub_live" }] })],
      ]),
      stripe,
    });

    await expect(service.cancelSubscriptionsForAccountDeletion(USER_ID)).resolves.toBe(true);
  });

  it("throws the retryable precondition error when Stripe is unreachable", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const stripe = createStripeMock();
      stripe.subscriptions.cancel.mockRejectedValue(new Error("network down"));
      stripe.subscriptions.retrieve.mockRejectedValue(new Error("network down"));
      const service = createService({
        db: createRoutedDb([
          ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
          ["SELECT stripe_subscription_id", () => ({ rows: [{ stripe_subscription_id: "sub_live" }] })],
        ]),
        stripe,
      });

      await expect(service.cancelSubscriptionsForAccountDeletion(USER_ID)).rejects.toMatchObject({
        name: "MembershipServiceError",
        code: "subscription_cancel_failed",
      });
    } finally {
      errorSpy.mockRestore();
    }
  });
});

// Amount changes (docs/plans/membership-manage-page.md, PR 2). The §17602(g)(2)
// window — apply and notice only 7–30 days before the renewal — is driven by
// the clock, so these tests pin "now" relative to the fixture's period end.
const PERIOD_END = new Date(PERIOD_END_EPOCH * 1000);
const daysBeforePeriodEnd = (days: number) => new Date(PERIOD_END.getTime() - days * 24 * 60 * 60 * 1000);
const CHANGE_ID = "33333333-3333-4333-8333-333333333333";

const changeRow = (overrides: Record<string, unknown> = {}) => ({
  id: CHANGE_ID,
  previous_amount_cents: 700,
  new_amount_cents: 2000,
  requested_at: daysBeforePeriodEnd(12),
  applied_at: null,
  effective_at: null,
  notice_sent_at: null,
  superseded_at: null,
  ...overrides,
});

/** Amount-change table routes. `rows` answers every read of the table (the
 * request's pre-check, apply-if-due, the status): one array repeats; several
 * are consumed in order and the last repeats, so a test can show the
 * request its pre-state and then the row it just inserted. Writes are
 * routed by their SET clause; the supersede UPDATE goes first because its
 * predicate shares text with the read. */
const amountRoutes = (rows: unknown[][] = [[]]): DbRoute[] => {
  const answers = [...rows];
  return [
    ["SET superseded_at", () => ({ rowCount: 1 })],
    ["INSERT INTO public.billing_subscription_amount_changes", () => ({ rowCount: 1 })],
    ["SET applied_at", () => ({ rowCount: 1 })],
    ["SET notice_sent_at", () => ({ rowCount: 1 })],
    ["FROM public.billing_subscription_amount_changes", () => ({ rows: answers.length > 1 ? answers.shift() : answers[0] })],
  ];
};

function callsContaining(db: { query: ReturnType<typeof vi.fn> }, substring: string) {
  return db.query.mock.calls.filter((call) => String(call[0]).includes(substring));
}

const monthlyUsdItem = (unitAmount: number, periodEndEpoch = PERIOD_END_EPOCH) => ({
  id: "si_1",
  price: { unit_amount: unitAmount, product: "prod_test", currency: "usd", recurring: { interval: "month" } },
  current_period_end: periodEndEpoch,
});

const swapParams = (unitAmount: number) => ({
  items: [
    {
      id: "si_1",
      price_data: { currency: "usd", product: "prod_test", unit_amount: unitAmount, recurring: { interval: "month" } },
    },
  ],
  proration_behavior: "none",
});

const NOTICE = { kind: "amount_notice", email: "user@example.com", newAmountCents: 2000, startsAt: PERIOD_END };

describe("membership changeMonthlyAmount", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  /** Default: no prior request, then apply-if-due sees the inserted one. */
  function setup(opts: { rows?: unknown[][]; subscription?: Record<string, unknown> } = {}) {
    const db = createRoutedDb([...amountRoutes(opts.rows ?? [[], [changeRow()]]), ...manageRoutes()]);
    const stripe = createStripeMock();
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription(opts.subscription));
    stripe.subscriptions.update.mockResolvedValue(stripeSubscription({ items: { data: [monthlyUsdItem(2000)] } }));
    const sendMembershipChangedEmail = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipChangedEmail });
    return { db, stripe, sendMembershipChangedEmail, service };
  }

  it("rejects amounts outside the checkout bounds before touching Stripe", async () => {
    const { stripe, service } = setup();
    for (const amount_cents of [499, 100_001, 10.5]) {
      await expect(service.changeMonthlyAmount(USER_ID, { amount_cents })).rejects.toThrow(TypeError);
    }
    expect(stripe.subscriptions.retrieve).not.toHaveBeenCalled();
  });

  it("inside the notice window: records the request, sends the notice FIRST, then swaps the price with no proration", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const { db, stripe, sendMembershipChangedEmail, service } = setup();

    await service.changeMonthlyAmount(USER_ID, { amount_cents: 2000 });

    const insert = callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")[0];
    expect(insert?.[1]).toEqual(["sub_1", 700, 2000]);
    // One pending request per subscription: a concurrent duplicate is dropped.
    expect(String(insert?.[0])).toContain("ON CONFLICT (stripe_subscription_id) WHERE applied_at IS NULL AND superseded_at IS NULL DO NOTHING");
    expect(sendMembershipChangedEmail).toHaveBeenCalledWith(NOTICE);
    // The notice names the renewal it precedes.
    expect(callsContaining(db, "SET notice_sent_at")[0]?.[1]).toEqual([CHANGE_ID, PERIOD_END]);
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", swapParams(2000));
    // Order: the notice went out before the swap.
    const noticeOrder = sendMembershipChangedEmail.mock.invocationCallOrder[0];
    const swapOrder = stripe.subscriptions.update.mock.invocationCallOrder[0];
    expect(noticeOrder).toBeLessThan(swapOrder as number);
    const applied = callsContaining(db, "SET applied_at")[0];
    expect(applied?.[1]).toEqual([CHANGE_ID]);
    // A request replaced mid-flight is never stamped applied.
    expect(String(applied?.[0])).toContain("AND superseded_at IS NULL");
    // The row catches up with Stripe's new price after the swap.
    expect(upsertParams(db)).toBeDefined();
  });

  it.each([
    ["fewer than 7 days before the renewal", 3],
    ["more than 30 days before the renewal", 40],
  ])("%s: records the request and leaves notice and swap to a later poke", async (_name, days) => {
    vi.setSystemTime(daysBeforePeriodEnd(days));
    const { db, stripe, sendMembershipChangedEmail, service } = setup();

    await service.changeMonthlyAmount(USER_ID, { amount_cents: 2000 });

    expect(callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")).toHaveLength(1);
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
    expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
    expect(callsContaining(db, "SET applied_at")).toHaveLength(0);
  });

  it("re-saving the current amount withdraws a pending request and touches nothing else", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const { db, stripe, service } = setup({ rows: [[changeRow()], []] });

    await service.changeMonthlyAmount(USER_ID, { amount_cents: 700 });

    const supersede = callsContaining(db, "SET superseded_at");
    expect(supersede).toHaveLength(1);
    expect(String(supersede[0]?.[0])).toContain("AND applied_at IS NULL");
    expect(String(supersede[0]?.[0])).not.toContain("effective_at");
    expect(callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")).toHaveLength(0);
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
  });

  it("re-saving the current amount with nothing pending is a no-op (lost-response retry)", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const { db, stripe, service } = setup({ rows: [[]] });

    await expect(service.changeMonthlyAmount(USER_ID, { amount_cents: 700 })).resolves.toMatchObject({ enabled: true });
    expect(callsContaining(db, "SET superseded_at")).toHaveLength(0);
    expect(callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")).toHaveLength(0);
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
  });

  it("a newer request replaces a pending one", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(40));
    const { db, service } = setup({ rows: [[changeRow()], [changeRow({ new_amount_cents: 3000 })]] });

    await service.changeMonthlyAmount(USER_ID, { amount_cents: 3000 });

    const supersede = callsContaining(db, "SET superseded_at");
    expect(supersede).toHaveLength(1);
    expect(String(supersede[0]?.[0])).toContain("effective_at > now()");
    expect(callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")[0]?.[1]).toEqual(["sub_1", 700, 3000]);
  });

  it("an applied change can be replaced while the window is still open (new notice, new swap)", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const applied = changeRow({ applied_at: daysBeforePeriodEnd(12), effective_at: PERIOD_END, notice_sent_at: daysBeforePeriodEnd(12) });
    const replacement = changeRow({ id: "44444444-4444-4444-8444-444444444444", previous_amount_cents: 2000, new_amount_cents: 3000 });
    // Stripe already carries the applied $20 price.
    const { db, stripe, sendMembershipChangedEmail, service } = setup({
      rows: [[applied], [replacement]],
      subscription: { items: { data: [monthlyUsdItem(2000)] } },
    });

    await service.changeMonthlyAmount(USER_ID, { amount_cents: 3000 });

    expect(callsContaining(db, "SET superseded_at")).toHaveLength(1);
    expect(callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")[0]?.[1]).toEqual(["sub_1", 2000, 3000]);
    expect(sendMembershipChangedEmail).toHaveBeenCalledWith({ ...NOTICE, newAmountCents: 3000 });
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", swapParams(3000));
  });

  it("refuses to replace an applied change once fewer than 7 days remain (no time to notice again)", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(3));
    const applied = changeRow({ applied_at: daysBeforePeriodEnd(12), effective_at: PERIOD_END, notice_sent_at: daysBeforePeriodEnd(12) });
    const { db, stripe, service } = setup({ rows: [[applied]], subscription: { items: { data: [monthlyUsdItem(2000)] } } });

    await expect(service.changeMonthlyAmount(USER_ID, { amount_cents: 3000 })).rejects.toMatchObject({
      code: "membership_pending",
      message: expect.stringContaining("$20.00 is already set for your next renewal"),
    });
    expect(callsContaining(db, "SET superseded_at")).toHaveLength(0);
    expect(callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")).toHaveLength(0);
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
  });

  it.each([
    ["incomplete", { status: "incomplete" }, "still being confirmed"],
    ["unpaid", { status: "unpaid" }, "didn't go through"],
    ["scheduled to cancel", { cancel_at: PERIOD_END_EPOCH }, "Keep membership first"],
  ])("refuses a %s subscription without recording anything", async (_name, overrides, message) => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const { db, stripe, service } = setup({ subscription: overrides });

    await expect(service.changeMonthlyAmount(USER_ID, { amount_cents: 2000 })).rejects.toMatchObject({
      code: "membership_pending",
      message: expect.stringContaining(message),
    });
    expect(callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")).toHaveLength(0);
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
  });

  it("refuses to re-price anything but a single monthly USD item", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const usd = monthlyUsdItem(700);
      for (const items of [
        { data: [{ ...usd, price: { ...usd.price, currency: "eur" } }] },
        { data: [{ ...usd, price: { ...usd.price, recurring: { interval: "year" } } }] },
        { data: [usd, { ...usd, id: "si_2" }] },
      ]) {
        const { stripe, service } = setup({ subscription: { items } });
        await expect(service.changeMonthlyAmount(USER_ID, { amount_cents: 2000 })).rejects.toMatchObject({ code: "membership_conflict" });
        expect(stripe.subscriptions.update).not.toHaveBeenCalled();
      }
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("a failed notice does not fail the request, and nothing changes at Stripe: the request stays pending", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const { db, stripe, sendMembershipChangedEmail, service } = setup();
      sendMembershipChangedEmail.mockRejectedValue(new Error("SES down"));

      await expect(service.changeMonthlyAmount(USER_ID, { amount_cents: 2000 })).resolves.toMatchObject({ enabled: true });
      expect(callsContaining(db, "INSERT INTO public.billing_subscription_amount_changes")).toHaveLength(1);
      expect(stripe.subscriptions.update).not.toHaveBeenCalled();
      expect(callsContaining(db, "SET notice_sent_at")).toHaveLength(0);
      expect(callsContaining(db, "SET applied_at")).toHaveLength(0);
      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("stays pending"));
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("throws no_membership without a live subscription", async () => {
    const service = createService({ db: createRoutedDb(manageRoutes({ noLive: true })), stripe: createStripeMock() });
    await expect(service.changeMonthlyAmount(USER_ID, { amount_cents: 2000 })).rejects.toMatchObject({ code: "no_membership" });
  });
});

describe("membership webhook: amount change apply-if-due", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  const pokeRoutes = (rows: unknown[][]): DbRoute[] => [
    ...amountRoutes(rows),
    ["WHERE stripe_customer_id", () => ({ rows: [customerRow()] })],
    ["SELECT stripe_subscription_id", () => ({ rows: [] })],
    ["INSERT INTO public.billing_subscriptions", () => ({ rows: [{ acknowledgment_sent_at: new Date() }] })],
    ["SELECT email", () => ({ rows: [{ email: "user@example.com" }] })],
  ];

  const upcomingInvoice = (subscription: string | null) =>
    webhookEvent("invoice.upcoming", {
      object: "invoice",
      parent: subscription ? { type: "subscription_details", subscription_details: { subscription } } : null,
    });

  function setup(event: unknown, opts: { rows?: unknown[][]; subscription?: Record<string, unknown>; sender?: null } = {}) {
    const db = createRoutedDb(pokeRoutes(opts.rows ?? [[changeRow()]]));
    const stripe = stripeDelivering(event);
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription(opts.subscription));
    const sendMembershipChangedEmail = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipChangedEmail: opts.sender === null ? null : sendMembershipChangedEmail });
    return { db, stripe, sendMembershipChangedEmail, service };
  }

  it("invoice.upcoming (14 days out) applies the pending change: notice, then swap, both stamped", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(14));
    const { db, stripe, sendMembershipChangedEmail, service } = setup(upcomingInvoice("sub_1"));

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");

    expect(stripe.subscriptions.retrieve).toHaveBeenCalledWith("sub_1");
    expect(sendMembershipChangedEmail).toHaveBeenCalledWith(NOTICE);
    expect(callsContaining(db, "SET notice_sent_at")[0]?.[1]).toEqual([CHANGE_ID, PERIOD_END]);
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", swapParams(2000));
    expect(callsContaining(db, "SET applied_at")[0]?.[1]).toEqual([CHANGE_ID]);
  });

  it("invoice.upcoming without a subscription is ignored", async () => {
    const { stripe, service } = setup(upcomingInvoice(null));
    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(stripe.subscriptions.retrieve).not.toHaveBeenCalled();
  });

  it("a subscription poke outside the window leaves the change pending", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(3));
    const { db, stripe, sendMembershipChangedEmail, service } = setup(webhookEvent("customer.subscription.updated", stripeSubscription()));

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
    expect(callsContaining(db, "SET applied_at")).toHaveLength(0);
  });

  it("the post-renewal poke applies a change that missed the previous window", async () => {
    // Renewed: the period end moved a month on, and "now" is right after it.
    const NEXT_PERIOD_END_EPOCH = PERIOD_END_EPOCH + 30 * 24 * 60 * 60;
    vi.setSystemTime(new Date(PERIOD_END_EPOCH * 1000 + 60_000));
    const renewed = { items: { data: [monthlyUsdItem(700, NEXT_PERIOD_END_EPOCH)] } };
    const { db, stripe, sendMembershipChangedEmail, service } = setup(
      webhookEvent("customer.subscription.updated", stripeSubscription(renewed)),
      { subscription: renewed }
    );
    stripe.subscriptions.update.mockResolvedValue(stripeSubscription(renewed));

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(sendMembershipChangedEmail).toHaveBeenCalledWith({ ...NOTICE, startsAt: new Date(NEXT_PERIOD_END_EPOCH * 1000) });
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", swapParams(2000));
  });

  it("a noticed change whose swap failed earlier is swapped without a second notice", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const noticed = changeRow({ notice_sent_at: daysBeforePeriodEnd(11), effective_at: PERIOD_END });
    const { db, stripe, sendMembershipChangedEmail, service } = setup(
      webhookEvent("customer.subscription.updated", stripeSubscription()),
      { rows: [[noticed]] }
    );

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
    expect(callsContaining(db, "SET notice_sent_at")).toHaveLength(0);
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", swapParams(2000));
    expect(callsContaining(db, "SET applied_at")).toHaveLength(1);
  });

  it("a notice sent for a renewal that has since passed is stale: re-sent for the new date before the swap", async () => {
    const NEXT_PERIOD_END_EPOCH = PERIOD_END_EPOCH + 30 * 24 * 60 * 60;
    vi.setSystemTime(new Date(NEXT_PERIOD_END_EPOCH * 1000 - 14 * 24 * 60 * 60 * 1000));
    const renewed = { items: { data: [monthlyUsdItem(700, NEXT_PERIOD_END_EPOCH)] } };
    const staleNotice = changeRow({ notice_sent_at: daysBeforePeriodEnd(10), effective_at: PERIOD_END });
    const { db, stripe, sendMembershipChangedEmail, service } = setup(upcomingInvoice("sub_1"), {
      rows: [[staleNotice]],
      subscription: renewed,
    });
    stripe.subscriptions.update.mockResolvedValue(stripeSubscription(renewed));

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(sendMembershipChangedEmail).toHaveBeenCalledWith({ ...NOTICE, startsAt: new Date(NEXT_PERIOD_END_EPOCH * 1000) });
    expect(callsContaining(db, "SET notice_sent_at")[0]?.[1]).toEqual([CHANGE_ID, new Date(NEXT_PERIOD_END_EPOCH * 1000)]);
    expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", swapParams(2000));
  });

  it("a failed notice 5xxes the webhook (Stripe redelivers) and leaves the Stripe price alone", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(14));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const { db, stripe, sendMembershipChangedEmail, service } = setup(upcomingInvoice("sub_1"));
      sendMembershipChangedEmail.mockRejectedValue(new Error("SES down"));

      await expect(service.handleWebhookEvent(WEBHOOK_INPUT)).rejects.toThrow(MembershipWebhookRetryError);
      expect(stripe.subscriptions.update).not.toHaveBeenCalled();
      expect(callsContaining(db, "SET notice_sent_at")).toHaveLength(0);
      expect(callsContaining(db, "SET applied_at")).toHaveLength(0);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("no notice sender means no swap: the change stays pending", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(14));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const { db, stripe, service } = setup(upcomingInvoice("sub_1"), { sender: null });

      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
      expect(stripe.subscriptions.update).not.toHaveBeenCalled();
      expect(callsContaining(db, "SET applied_at")).toHaveLength(0);
      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("stays pending unnoticed"));
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("an applied change whose price Stripe no longer carries (an older swap landed late) is restored, no new notice", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(9));
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const applied = changeRow({ applied_at: daysBeforePeriodEnd(10), effective_at: PERIOD_END, notice_sent_at: daysBeforePeriodEnd(10) });
      // Stripe reads the OLDER request's $15, not the noticed $20.
      const drifted = { items: { data: [monthlyUsdItem(1500)] } };
      const { db, stripe, sendMembershipChangedEmail, service } = setup(
        webhookEvent("customer.subscription.updated", stripeSubscription(drifted)),
        { rows: [[applied]], subscription: drifted }
      );

      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
      expect(stripe.subscriptions.update).toHaveBeenCalledWith("sub_1", swapParams(2000));
      expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
      expect(callsContaining(db, "SET applied_at")).toHaveLength(0);
      expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("restoring"));
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("an applied change Stripe already carries needs nothing", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(9));
    const applied = changeRow({ applied_at: daysBeforePeriodEnd(10), effective_at: PERIOD_END, notice_sent_at: daysBeforePeriodEnd(10) });
    const current = { items: { data: [monthlyUsdItem(2000)] } };
    const { stripe, sendMembershipChangedEmail, service } = setup(
      webhookEvent("customer.subscription.updated", stripeSubscription(current)),
      { rows: [[applied]], subscription: current }
    );

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
    expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
  });

  it("a scheduled cancel parks the change (no notice, no swap) until the member resumes", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(14));
    const { db, stripe, sendMembershipChangedEmail, service } = setup(upcomingInvoice("sub_1"), {
      subscription: { cancel_at: PERIOD_END_EPOCH },
    });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(sendMembershipChangedEmail).not.toHaveBeenCalled();
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
    expect(callsContaining(db, "SET superseded_at")).toHaveLength(0);
  });

  it("a terminal subscription supersedes the change instead of applying it", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(14));
    const canceled = { status: "canceled", canceled_at: 1_755_900_000 };
    const { db, stripe, service } = setup(webhookEvent("customer.subscription.deleted", stripeSubscription(canceled)), {
      subscription: canceled,
    });

    expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
    expect(stripe.subscriptions.update).not.toHaveBeenCalled();
    expect(callsContaining(db, "SET superseded_at")[0]?.[1]).toEqual([CHANGE_ID]);
  });

  it("resume brings a parked change back into play", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(10));
    const db = createRoutedDb([...amountRoutes([[changeRow()]]), ...manageRoutes()]);
    const stripe = createStripeMock();
    stripe.subscriptions.retrieve.mockResolvedValue(stripeSubscription({ cancel_at: PERIOD_END_EPOCH }));
    // First update clears the cancel; the second is the price swap.
    stripe.subscriptions.update.mockResolvedValue(stripeSubscription());
    const sendMembershipChangedEmail = vi.fn(async () => {});
    const service = createService({ db, stripe, sendMembershipChangedEmail });

    await service.resumeMembership(USER_ID);
    expect(sendMembershipChangedEmail).toHaveBeenCalledWith(NOTICE);
    expect(stripe.subscriptions.update.mock.calls).toEqual([
      ["sub_1", { cancel_at: "" }],
      ["sub_1", swapParams(2000)],
    ]);
  });
});

describe("membership getMembership: pending amount change", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  function statusWith(
    changes: unknown[],
    opts: { rowAmountCents?: number; periodEnd?: Date | null; startedAt?: Date } = {}
  ) {
    const db = createRoutedDb([
      ["FROM public.billing_customers", () => ({ rows: [customerRow()] })],
      ["FROM public.billing_subscription_amount_changes", () => ({ rows: changes })],
      [
        "FROM public.billing_subscriptions",
        () => ({
          rows: [
            {
              stripe_subscription_id: "sub_1",
              stripe_status: "active",
              monthly_amount_cents: opts.rowAmountCents ?? 700,
              cancel_at_period_end: false,
              current_period_end: opts.periodEnd === undefined ? PERIOD_END : opts.periodEnd,
              started_at: opts.startedAt ?? new Date("2026-08-10T00:00:00Z"),
            },
          ],
        }),
      ],
      ["SUM(amount_cents - refunded_amount_cents)", () => ({ rows: [{ total_net_cents: 0 }] })],
      ["FROM public.billing_payments", () => ({ rows: [] })],
    ]);
    return createService({ db, stripe: createStripeMock() }).getMembership(USER_ID);
  }

  it("reports no pending change for a plain subscription", async () => {
    const result = await statusWith([]);
    expect(result.membership).toMatchObject({ monthly_amount_cents: 700, pending_amount_change: null });
  });

  it("projects an unapplied request onto this period's end while 7 days remain", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(12));
    const result = await statusWith([changeRow()]);
    expect(result.membership?.pending_amount_change).toEqual({
      new_amount_cents: 2000,
      starts_at: PERIOD_END.toISOString(),
      applied: false,
    });
    expect(result.membership?.monthly_amount_cents).toBe(700);
  });

  it("projects an unapplied request onto the renewal after next once fewer than 7 days remain", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(3));
    // Started on the 11th → renews on the 11th; 2025-09-11T14:13:20Z + one month.
    const result = await statusWith([changeRow()], { startedAt: new Date("2025-08-11T14:13:20Z") });
    expect(result.membership?.pending_amount_change?.starts_at).toBe("2025-10-11T14:13:20.000Z");
  });

  it("keeps the subscription's anchor day across a short month, like Stripe (31st → Feb 28 → Mar 31)", async () => {
    const feb28 = new Date("2026-02-28T12:00:00Z");
    vi.setSystemTime(new Date(feb28.getTime() - 3 * 24 * 60 * 60 * 1000));
    const result = await statusWith([changeRow()], { periodEnd: feb28, startedAt: new Date("2026-01-31T12:00:00Z") });
    expect(result.membership?.pending_amount_change?.starts_at).toBe("2026-03-31T12:00:00.000Z");
  });

  it("clamps the anchor day to the shorter month (31st → Apr 30)", async () => {
    const mar31 = new Date("2026-03-31T12:00:00Z");
    vi.setSystemTime(new Date(mar31.getTime() - 3 * 24 * 60 * 60 * 1000));
    const result = await statusWith([changeRow()], { periodEnd: mar31, startedAt: new Date("2026-01-31T12:00:00Z") });
    expect(result.membership?.pending_amount_change?.starts_at).toBe("2026-04-30T12:00:00.000Z");
  });

  it("omits the date rather than inventing one when the period end is unknown", async () => {
    const result = await statusWith([changeRow()], { periodEnd: null });
    expect(result.membership?.pending_amount_change).toEqual({ new_amount_cents: 2000, starts_at: null, applied: false });
  });

  it("an applied change reports the firm date and keeps this period's amount as the monthly amount", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(5));
    const applied = changeRow({ applied_at: daysBeforePeriodEnd(12), effective_at: PERIOD_END, notice_sent_at: daysBeforePeriodEnd(12) });
    // Stripe (and the row) already carry the new price.
    const result = await statusWith([applied], { rowAmountCents: 2000 });
    expect(result.membership).toMatchObject({
      monthly_amount_cents: 700,
      pending_amount_change: { new_amount_cents: 2000, starts_at: PERIOD_END.toISOString(), applied: true },
    });
  });

  it("a change back to this period's amount (revert inside the window) reports nothing pending", async () => {
    vi.setSystemTime(daysBeforePeriodEnd(5));
    const first = changeRow({
      applied_at: daysBeforePeriodEnd(12),
      effective_at: PERIOD_END,
      notice_sent_at: daysBeforePeriodEnd(12),
      superseded_at: daysBeforePeriodEnd(8),
    });
    const revert = changeRow({
      id: "44444444-4444-4444-8444-444444444444",
      previous_amount_cents: 2000,
      new_amount_cents: 700,
      requested_at: daysBeforePeriodEnd(8),
      applied_at: daysBeforePeriodEnd(8),
      effective_at: PERIOD_END,
      notice_sent_at: daysBeforePeriodEnd(8),
    });
    const result = await statusWith([first, revert], { rowAmountCents: 700 });
    expect(result.membership).toMatchObject({ monthly_amount_cents: 700, pending_amount_change: null });
  });
});

describe("membership stale-write guard", () => {
  it("a write the row refused as stale sends no acknowledgment", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    try {
      const db = createRoutedDb([
        ["WHERE stripe_customer_id", () => ({ rows: [customerRow()] })],
        ["SELECT stripe_subscription_id", () => ({ rows: [] })],
        // The guard's WHERE rejected the write: no RETURNING row.
        ["INSERT INTO public.billing_subscriptions", () => ({ rows: [] })],
      ]);
      const stripe = stripeDelivering(webhookEvent("customer.subscription.updated", stripeSubscription()));
      const sender = vi.fn(async () => {});
      const service = createService({ db, stripe, sendMembershipStartedEmail: sender });

      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
      expect(sender).not.toHaveBeenCalled();
      expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("skipped a stale write"));
    } finally {
      logSpy.mockRestore();
    }
  });

  it("a mutation's write carries an instant no earlier than the retrieve before it", async () => {
    const db = createRoutedDb(manageRoutes());
    const stripe = createStripeMock();
    stripe.subscriptions.update.mockResolvedValue(stripeSubscription({ cancel_at: PERIOD_END_EPOCH }));
    const service = createService({ db, stripe });
    const before = new Date();

    await service.cancelMembership(USER_ID);

    const syncedAt = upsertParams(db)?.[9] as Date;
    expect(syncedAt).toBeInstanceOf(Date);
    expect(syncedAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
  });
});
