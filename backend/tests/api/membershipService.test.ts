import { describe, expect, it, vi } from "vitest";

import {
  createMembershipService,
  MembershipServiceError,
  MembershipWebhookRetryError,
  type MembershipStripeClient,
} from "../../src/api/membership/membershipService.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const BILLING_CUSTOMER_ID = "22222222-2222-4222-8222-222222222222";
const STRIPE_CUSTOMER_ID = "cus_test1";

// Routed query mock: the service issues plain single statements, so each
// test wires responses by SQL substring instead of call order (order is an
// implementation detail; the statements are not).
type DbRoute = [substring: string, respond: (params: unknown[]) => { rows?: unknown[]; rowCount?: number }];

function createRoutedDb(routes: DbRoute[]) {
  const query = vi.fn(async (sql: string, params: unknown[] = []) => {
    for (const [substring, respond] of routes) {
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
    items: { data: [{ price: { unit_amount: 700 }, current_period_end: 1_757_600_000 }] },
    ...overrides,
  };
}

function createStripeMock(overrides: Record<string, unknown> = {}): MembershipStripeClient & {
  checkout: { sessions: { create: ReturnType<typeof vi.fn> } };
  billingPortal: { sessions: { create: ReturnType<typeof vi.fn> } };
  customers: { create: ReturnType<typeof vi.fn> };
  subscriptions: { retrieve: ReturnType<typeof vi.fn>; cancel: ReturnType<typeof vi.fn> };
  invoicePayments: { list: ReturnType<typeof vi.fn> };
  webhooks: { constructEvent: ReturnType<typeof vi.fn> };
} {
  return {
    checkout: {
      sessions: { create: vi.fn(async () => ({ id: "cs_1", url: "https://checkout.stripe.test/cs_1" })) },
    },
    billingPortal: { sessions: { create: vi.fn(async () => ({ url: "https://portal.stripe.test/ps_1" })) } },
    customers: { create: vi.fn(async () => ({ id: "cus_new" })) },
    subscriptions: {
      retrieve: vi.fn(async () => stripeSubscription()),
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
}) {
  return createMembershipService({
    db: input.db as never,
    stripe: input.stripe,
    webhookSecret: "whsec_test",
    membershipProductId: "prod_test",
    publicBaseUrl: "https://site.test",
    sendMembershipStartedEmail: input.sendMembershipStartedEmail ?? null,
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
      success_url: "https://site.test/me/settings?membership=success",
      cancel_url: "https://site.test/me/settings?membership=canceled",
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
    ]);
    // Consent pointers survive later pokes.
    expect(String(upsert?.[0])).toContain("COALESCE");
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

  it("records a completion WITHOUT consent, but with no consent pointer, and logs the anomaly", async () => {
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
      const service = createService({ db, stripe });

      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
      const upsert = db.query.mock.calls.find((call) =>
        String(call[0]).includes("INSERT INTO public.billing_subscriptions")
      );
      expect(upsert?.[1]?.[2]).toBeNull();
      expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("WITHOUT terms-of-service consent"));
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

  it("never fails the webhook over a failed send; the stamp stays unset for the next poke", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const db = createRoutedDb(routes(null));
      const stripe = stripeDelivering(webhookEvent("customer.subscription.updated", stripeSubscription()));
      const sender = vi.fn(async () => {
        throw new Error("ses down");
      });
      const service = createService({ db, stripe, sendMembershipStartedEmail: sender });

      expect(await service.handleWebhookEvent(WEBHOOK_INPUT)).toBe("ok");
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

    await service.cancelSubscriptionsForAccountDeletion(USER_ID);
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

    await expect(service.cancelSubscriptionsForAccountDeletion(USER_ID)).resolves.toBeUndefined();
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
