import { afterEach, describe, expect, it, vi } from "vitest";
import type { ErrorEvent } from "@sentry/node";

import {
  captureError,
  describeError,
  flushSentry,
  initSentryFromEnv,
  scrubSentryEvent,
  scrubText,
} from "../../src/observability/sentry.js";

const sentryMock = vi.hoisted(() => ({
  init: vi.fn(),
  setTag: vi.fn(),
  captureException: vi.fn(),
  flush: vi.fn(),
}));

vi.mock("@sentry/node", () => sentryMock);

afterEach(() => {
  vi.unstubAllEnvs();
  vi.clearAllMocks();
});

describe("scrubText", () => {
  it("replaces email addresses", () => {
    expect(scrubText("lookup failed for voter+tag@example.co.uk during sync")).toBe(
      "lookup failed for [email] during sync"
    );
  });

  it("replaces URL query strings", () => {
    expect(scrubText("GET /api/ballot?d=abc-123,def-456&sort=my_areas failed")).toBe(
      "GET /api/ballot?[scrubbed] failed"
    );
  });

  it("leaves ordinary stack-trace text alone", () => {
    const frame = "Error: boom\n    at handler (/app/src/api/apiServer.ts:1439:11)";
    expect(scrubText(frame)).toBe(frame);
  });
});

describe("scrubSentryEvent", () => {
  it("drops request, user, breadcrumbs, and extra and scrubs exception values", () => {
    const event = {
      message: "wrapper says voter@example.com broke",
      request: { url: "https://api.example.com/api/ballot?d=abc" },
      user: { ip_address: "203.0.113.9" },
      breadcrumbs: [{ message: "clicked" }],
      extra: { requestBody: { address: "123 Main St", email: "voter@example.com" } },
      exception: {
        values: [{ type: "Error", value: "db rejected voter@example.com via /x?d=1" }],
      },
    } as unknown as ErrorEvent;

    const scrubbed = scrubSentryEvent(event);

    expect(scrubbed.request).toBeUndefined();
    expect(scrubbed.user).toBeUndefined();
    expect(scrubbed.breadcrumbs).toBeUndefined();
    expect(scrubbed.extra).toBeUndefined();
    expect(scrubbed.message).toBe("wrapper says [email] broke");
    expect(scrubbed.exception?.values?.[0]?.value).toBe("db rejected [email] via /x?[scrubbed]");
  });

  it("scrubs string tag values", () => {
    const event = {
      tags: { path: "/api/ballot?d=abc", note: "from voter@example.com", count: 3 },
    } as unknown as ErrorEvent;

    const scrubbed = scrubSentryEvent(event);

    expect(scrubbed.tags).toEqual({ path: "/api/ballot?[scrubbed]", note: "from [email]", count: 3 });
  });

  it("recursively scrubs strings inside contexts while keeping the shape", () => {
    const event = {
      contexts: {
        runtime: { name: "node", version: "v23.9.0" },
        custom: { nested: { detail: "sent to voter@example.com", urls: ["/x?d=1"] } },
      },
    } as unknown as ErrorEvent;

    const scrubbed = scrubSentryEvent(event);

    expect(scrubbed.contexts).toEqual({
      runtime: { name: "node", version: "v23.9.0" },
      custom: { nested: { detail: "sent to [email]", urls: ["/x?[scrubbed]"] } },
    });
  });
});

describe("describeError", () => {
  it("returns the scrubbed stack string for Errors", () => {
    const error = new Error("lookup for voter@example.com failed at /api/ballot?d=1");
    const described = describeError(error);
    expect(described).toContain("lookup for [email] failed at /api/ballot?[scrubbed]");
    expect(described).toContain("at "); // stack frames retained
  });

  it("stringifies and scrubs non-Errors", () => {
    expect(describeError("raw voter@example.com")).toBe("raw [email]");
  });
});

describe("initSentryFromEnv", () => {
  it("stays dark without SENTRY_DSN", () => {
    vi.stubEnv("SENTRY_DSN", "");
    expect(initSentryFromEnv("api")).toBe(false);
    expect(sentryMock.init).not.toHaveBeenCalled();
  });

  it("initializes errors-only and tags the component when a DSN is set", () => {
    vi.stubEnv("SENTRY_DSN", "https://key@example.ingest.sentry.io/1");
    vi.stubEnv("DEPLOY_ENV", "staging");
    vi.stubEnv("DEPLOY_RELEASE", "abc1234");

    expect(initSentryFromEnv("worker")).toBe(true);

    expect(sentryMock.init).toHaveBeenCalledWith(
      expect.objectContaining({
        dsn: "https://key@example.ingest.sentry.io/1",
        environment: "staging",
        release: "abc1234",
        sendDefaultPii: false,
        defaultIntegrations: false,
        tracesSampleRate: 0,
        maxBreadcrumbs: 0,
      })
    );
    expect(sentryMock.setTag).toHaveBeenCalledWith("component", "worker");
  });
});

describe("capture and flush never throw", () => {
  it("swallows captureException failures", () => {
    sentryMock.captureException.mockImplementation(() => {
      throw new Error("sentry down");
    });
    expect(() => captureError(new Error("boom"), { component: "api" })).not.toThrow();
  });

  it("resolves even when flush rejects", async () => {
    sentryMock.flush.mockRejectedValue(new Error("timeout"));
    await expect(flushSentry(10)).resolves.toBeUndefined();
  });
});
