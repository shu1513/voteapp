import { type IncomingMessage, ServerResponse } from "node:http";
import { Readable, Writable } from "node:stream";
import express, { type Express } from "express";
import { describe, expect, it, vi } from "vitest";

import { createApiApp } from "../../src/api/apiServer.js";
import type { AddressApiServerOptions } from "../../src/api/addressApiTypes.js";
import {
  MembershipServiceError,
  MembershipWebhookRetryError,
} from "../../src/api/membership/membershipService.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";

// Same in-process invoker as apiServer.test.ts: a real express app driven
// through node streams, no sockets.
async function invokeExpressApp(
  app: Express,
  input: {
    method: string;
    path: string;
    body?: string;
    headers?: Record<string, string>;
    remoteAddress?: string;
  }
): Promise<{ statusCode: number; headers: Record<string, string>; body: unknown; rawBody: string }> {
  const requestBody = input.body ?? "";
  const headers = {
    ...(input.headers ?? {}),
    ...(requestBody.length > 0 && !input.headers?.["content-length"]
      ? { "content-length": Buffer.byteLength(requestBody).toString() }
      : {}),
  };
  // Buffer chunks, unlike the apiServer.test.ts original: express.raw's
  // raw-body concatenates Buffers and rejects string chunks, and the webhook
  // path runs through express.raw.
  const request = Readable.from(requestBody.length > 0 ? [Buffer.from(requestBody)] : []) as IncomingMessage;
  Object.assign(request, {
    method: input.method,
    url: input.path,
    headers,
    socket: {
      remoteAddress: input.remoteAddress ?? "127.0.0.1",
    },
  });

  const response = new ServerResponse(request);
  const responseChunks: Buffer[] = [];
  const socket = new Writable({
    write(chunk, _encoding, callback) {
      responseChunks.push(Buffer.from(chunk));
      callback();
    },
  });
  response.assignSocket(socket as never);

  return await new Promise((resolve, reject) => {
    response.on("finish", () => {
      const rawResponse = Buffer.concat(responseChunks).toString("utf8");
      const [, rawBody = ""] = rawResponse.split("\r\n\r\n");
      const body =
        rawBody.length > 0 && String(response.getHeader("content-type") ?? "").includes("application/json")
          ? JSON.parse(rawBody)
          : rawBody;
      const headers = Object.fromEntries(
        Object.entries(response.getHeaders()).map(([key, value]) => [key, String(value)])
      );
      resolve({
        statusCode: response.statusCode,
        headers,
        body,
        rawBody,
      });
    });
    response.on("error", reject);
    app(request, response);
  });
}

const resolveAddress = vi.fn();

function authedOptions(overrides: Partial<AddressApiServerOptions> = {}): AddressApiServerOptions {
  return {
    resolveAddress,
    resolveAuthenticatedUserId: () => USER_ID,
    lookupAuthenticatedUserEmailVerified: async () => true,
    ...overrides,
  };
}

const JSON_HEADERS = { "content-type": "application/json" };

describe("GET /api/me/membership", () => {
  it("requires authentication", async () => {
    const app = createApiApp({ resolveAddress });
    const response = await invokeExpressApp(app, { method: "GET", path: "/api/me/membership" });
    expect(response.statusCode).toBe(401);
  });

  it("answers { enabled: false } when Stripe is not configured", async () => {
    const app = createApiApp(authedOptions());
    const response = await invokeExpressApp(app, { method: "GET", path: "/api/me/membership" });
    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ enabled: false });
  });

  it("returns the wired membership state", async () => {
    const getAuthenticatedMembership = vi.fn().mockResolvedValue({
      enabled: true,
      membership: null,
      total_net_cents: 500,
      payments: [],
    });
    const app = createApiApp(authedOptions({ getAuthenticatedMembership }));

    const response = await invokeExpressApp(app, { method: "GET", path: "/api/me/membership" });
    expect(response.statusCode).toBe(200);
    expect(response.body).toMatchObject({ enabled: true, total_net_cents: 500 });
    expect(getAuthenticatedMembership).toHaveBeenCalledWith(USER_ID);
  });

  it("rejects non-GET methods", async () => {
    const app = createApiApp(authedOptions());
    const response = await invokeExpressApp(app, { method: "POST", path: "/api/me/membership" });
    expect(response.statusCode).toBe(405);
  });
});

describe("POST /api/me/membership/checkout", () => {
  it("404s when Stripe is not configured (feature hidden)", async () => {
    const app = createApiApp(authedOptions());
    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/checkout",
      body: JSON.stringify({ kind: "one_time", amount_cents: 500 }),
      headers: JSON_HEADERS,
    });
    expect(response.statusCode).toBe(404);
  });

  it("requires the application/json content type (blocks plain form POSTs)", async () => {
    const createAuthenticatedMembershipCheckout = vi.fn();
    const app = createApiApp(authedOptions({ createAuthenticatedMembershipCheckout }));
    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/checkout",
      body: "kind=one_time&amount_cents=500",
      headers: { "content-type": "application/x-www-form-urlencoded" },
    });
    expect(response.statusCode).toBe(415);
    expect(createAuthenticatedMembershipCheckout).not.toHaveBeenCalled();
  });

  it.each([
    ["missing kind", { amount_cents: 500 }],
    ["bad kind", { kind: "weekly", amount_cents: 500 }],
    ["non-integer amount", { kind: "monthly", amount_cents: 5.5 }],
    ["string amount", { kind: "monthly", amount_cents: "500" }],
    ["below the $5 minimum", { kind: "monthly", amount_cents: 499 }],
    ["above the $1,000 cap", { kind: "one_time", amount_cents: 100_001 }],
  ])("rejects %s with a 400", async (_name, body) => {
    const createAuthenticatedMembershipCheckout = vi.fn();
    const app = createApiApp(authedOptions({ createAuthenticatedMembershipCheckout }));
    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/checkout",
      body: JSON.stringify(body),
      headers: JSON_HEADERS,
    });
    expect(response.statusCode).toBe(400);
    expect(createAuthenticatedMembershipCheckout).not.toHaveBeenCalled();
  });

  it("accepts both boundary amounts and returns the redirect URL", async () => {
    const createAuthenticatedMembershipCheckout = vi
      .fn()
      .mockResolvedValue({ url: "https://checkout.stripe.test/cs_1" });
    const app = createApiApp(authedOptions({ createAuthenticatedMembershipCheckout }));

    for (const amount of [500, 100_000]) {
      const response = await invokeExpressApp(app, {
        method: "POST",
        path: "/api/me/membership/checkout",
        body: JSON.stringify({ kind: "monthly", amount_cents: amount }),
        headers: JSON_HEADERS,
      });
      expect(response.statusCode).toBe(200);
      expect(response.body).toEqual({ url: "https://checkout.stripe.test/cs_1" });
    }
    expect(createAuthenticatedMembershipCheckout).toHaveBeenCalledWith(USER_ID, {
      kind: "monthly",
      amount_cents: 500,
    });
  });

  it("maps an existing live membership to 409 membership_exists", async () => {
    const createAuthenticatedMembershipCheckout = vi
      .fn()
      .mockRejectedValue(new MembershipServiceError("membership_exists", "You already have a monthly membership."));
    const app = createApiApp(authedOptions({ createAuthenticatedMembershipCheckout }));

    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/checkout",
      body: JSON.stringify({ kind: "monthly", amount_cents: 700 }),
      headers: JSON_HEADERS,
    });
    expect(response.statusCode).toBe(409);
    expect(response.body).toMatchObject({ error: { code: "membership_exists" } });
  });
});

describe("POST /api/me/membership/portal", () => {
  it("404s when Stripe is not configured", async () => {
    const app = createApiApp(authedOptions());
    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/portal",
      body: JSON.stringify({}),
      headers: JSON_HEADERS,
    });
    expect(response.statusCode).toBe(404);
  });

  it("404s for a user with no billing account and 200s with the portal URL otherwise", async () => {
    const createAuthenticatedMembershipPortal = vi.fn().mockResolvedValueOnce(null).mockResolvedValueOnce({
      url: "https://portal.stripe.test/ps_1",
    });
    const app = createApiApp(authedOptions({ createAuthenticatedMembershipPortal }));

    const missing = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/portal",
      body: JSON.stringify({}),
      headers: JSON_HEADERS,
    });
    expect(missing.statusCode).toBe(404);

    const found = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/portal",
      body: JSON.stringify({}),
      headers: JSON_HEADERS,
    });
    expect(found.statusCode).toBe(200);
    expect(found.body).toEqual({ url: "https://portal.stripe.test/ps_1" });
    // `{}` (what shipped clients send) = the general portal.
    expect(createAuthenticatedMembershipPortal).toHaveBeenLastCalledWith(USER_ID, { flow: null });
  });

  it("passes a portal flow through and rejects an unknown one with 400", async () => {
    const createAuthenticatedMembershipPortal = vi.fn().mockResolvedValue({ url: "https://portal.stripe.test/ps_2" });
    const app = createApiApp(authedOptions({ createAuthenticatedMembershipPortal }));

    const flow = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/portal",
      body: JSON.stringify({ flow: "payment_method_update" }),
      headers: JSON_HEADERS,
    });
    expect(flow.statusCode).toBe(200);
    expect(createAuthenticatedMembershipPortal).toHaveBeenCalledWith(USER_ID, { flow: "payment_method_update" });

    const unknown = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/membership/portal",
      body: JSON.stringify({ flow: "subscription_cancel" }),
      headers: JSON_HEADERS,
    });
    expect(unknown.statusCode).toBe(400);
    expect(createAuthenticatedMembershipPortal).toHaveBeenCalledTimes(1);
  });
});

// Manage-page actions (docs/plans/membership-manage-page.md).
describe.each([
  ["/api/me/membership/cancel", "cancelAuthenticatedMembership"],
  ["/api/me/membership/resume", "resumeAuthenticatedMembership"],
] as const)("POST %s", (path, optionName) => {
  const STATUS = { enabled: true, membership: null, total_net_cents: 0, payments: [] };

  it("404s when Stripe is not configured (feature hidden)", async () => {
    const app = createApiApp(authedOptions());
    const response = await invokeExpressApp(app, { method: "POST", path, body: "{}", headers: JSON_HEADERS });
    expect(response.statusCode).toBe(404);
  });

  it("rejects non-POST methods once wired", async () => {
    const app = createApiApp(authedOptions({ [optionName]: vi.fn() }));
    const response = await invokeExpressApp(app, { method: "GET", path });
    expect(response.statusCode).toBe(405);
    expect(response.headers.allow).toBe("POST");
  });

  it("requires the application/json content type (blocks plain cross-site form POSTs)", async () => {
    const action = vi.fn();
    const app = createApiApp(authedOptions({ [optionName]: action }));
    const response = await invokeExpressApp(app, {
      method: "POST",
      path,
      body: "x=1",
      headers: { "content-type": "application/x-www-form-urlencoded" },
    });
    expect(response.statusCode).toBe(415);
    expect(action).not.toHaveBeenCalled();
  });

  it("requires a verified email", async () => {
    const action = vi.fn();
    const app = createApiApp(
      authedOptions({ [optionName]: action, lookupAuthenticatedUserEmailVerified: async () => false })
    );
    const response = await invokeExpressApp(app, { method: "POST", path, body: "{}", headers: JSON_HEADERS });
    expect(response.statusCode).toBe(403);
    expect(action).not.toHaveBeenCalled();
  });

  it("answers the fresh membership status", async () => {
    const action = vi.fn().mockResolvedValue(STATUS);
    const app = createApiApp(authedOptions({ [optionName]: action }));
    const response = await invokeExpressApp(app, { method: "POST", path, body: "{}", headers: JSON_HEADERS });
    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual(STATUS);
    expect(action).toHaveBeenCalledWith(USER_ID);
  });

  it.each([
    ["no_membership", 404, "not_found"],
    ["membership_pending", 409, "membership_pending"],
    ["membership_conflict", 409, "membership_conflict"],
    ["membership_update_failed", 503, "upstream_unavailable"],
  ] as const)("maps a %s service error to %s", async (code, statusCode, responseCode) => {
    const action = vi.fn().mockRejectedValue(new MembershipServiceError(code, "message"));
    const app = createApiApp(authedOptions({ [optionName]: action }));
    const response = await invokeExpressApp(app, { method: "POST", path, body: "{}", headers: JSON_HEADERS });
    expect(response.statusCode).toBe(statusCode);
    expect(response.body).toMatchObject({ error: { code: responseCode } });
  });
});

describe("POST /api/stripe/webhook", () => {
  it("404s when Stripe is not configured", async () => {
    const app = createApiApp({ resolveAddress });
    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/stripe/webhook",
      body: "{}",
      headers: JSON_HEADERS,
    });
    expect(response.statusCode).toBe(404);
  });

  it("hands the handler the raw bytes and the stripe-signature header, no session auth required", async () => {
    const handleStripeWebhookEvent = vi.fn().mockResolvedValue("ok");
    // No resolveAuthenticatedUserId at all: Stripe carries no session.
    const app = createApiApp({ resolveAddress, handleStripeWebhookEvent });

    const payload = JSON.stringify({ id: "evt_1", type: "invoice.paid" });
    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/stripe/webhook",
      body: payload,
      headers: { ...JSON_HEADERS, "stripe-signature": "t=1,v1=abc" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ received: true });
    const input = handleStripeWebhookEvent.mock.calls[0]?.[0];
    expect(Buffer.isBuffer(input.rawBody)).toBe(true);
    expect(input.rawBody.toString("utf8")).toBe(payload);
    expect(input.signatureHeader).toBe("t=1,v1=abc");
  });

  it("answers 400 on a bad signature", async () => {
    const handleStripeWebhookEvent = vi.fn().mockResolvedValue("bad_signature");
    const app = createApiApp({ resolveAddress, handleStripeWebhookEvent });

    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/stripe/webhook",
      body: "{}",
      headers: JSON_HEADERS,
    });
    expect(response.statusCode).toBe(400);
  });

  it("answers 400 on an empty body", async () => {
    const handleStripeWebhookEvent = vi.fn();
    const app = createApiApp({ resolveAddress, handleStripeWebhookEvent });

    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/stripe/webhook",
      headers: { "stripe-signature": "t=1,v1=abc" },
    });
    expect(response.statusCode).toBe(400);
    expect(handleStripeWebhookEvent).not.toHaveBeenCalled();
  });

  it("turns a retryable handler failure into a 5xx so Stripe redelivers", async () => {
    const handleStripeWebhookEvent = vi
      .fn()
      .mockRejectedValue(new MembershipWebhookRetryError("refund has no ledger row yet"));
    const app = createApiApp({ resolveAddress, handleStripeWebhookEvent });

    const response = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/stripe/webhook",
      body: "{}",
      headers: JSON_HEADERS,
    });
    expect(response.statusCode).toBe(503);
  });

  it("turns an unexpected handler failure (e.g. DB down) into a 500", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const handleStripeWebhookEvent = vi.fn().mockRejectedValue(new Error("connection refused"));
      const app = createApiApp({ resolveAddress, handleStripeWebhookEvent });

      const response = await invokeExpressApp(app, {
        method: "POST",
        path: "/api/stripe/webhook",
        body: "{}",
        headers: JSON_HEADERS,
      });
      expect(response.statusCode).toBe(500);
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("is exempt from the global per-IP rate limiter (Stripe's shared IPs must not 429)", async () => {
    const handleStripeWebhookEvent = vi.fn().mockResolvedValue("ok");
    // A limiter that rejects everything: normal paths 429, the webhook not.
    const rateLimit = vi.fn().mockReturnValue({ allowed: false, retryAfterSeconds: 60 });
    const app = createApiApp({ resolveAddress, handleStripeWebhookEvent, rateLimit });

    const webhook = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/stripe/webhook",
      body: "{}",
      headers: JSON_HEADERS,
    });
    expect(webhook.statusCode).toBe(200);

    const other = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "x" }),
      headers: JSON_HEADERS,
    });
    expect(other.statusCode).toBe(429);
  });
});
