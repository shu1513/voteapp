import { describe, expect, it } from "vitest";
import type { ErrorEvent } from "@sentry/node";

import { scrubSentryEvent, scrubText } from "../../src/observability/sentry.js";

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
  it("drops request, user, and breadcrumbs and scrubs exception values", () => {
    const event = {
      message: "wrapper says voter@example.com broke",
      request: { url: "https://api.example.com/api/ballot?d=abc" },
      user: { ip_address: "203.0.113.9" },
      breadcrumbs: [{ message: "clicked" }],
      exception: {
        values: [{ type: "Error", value: "db rejected voter@example.com via /x?d=1" }],
      },
    } as unknown as ErrorEvent;

    const scrubbed = scrubSentryEvent(event);

    expect(scrubbed.request).toBeUndefined();
    expect(scrubbed.user).toBeUndefined();
    expect(scrubbed.breadcrumbs).toBeUndefined();
    expect(scrubbed.message).toBe("wrapper says [email] broke");
    expect(scrubbed.exception?.values?.[0]?.value).toBe("db rejected [email] via /x?[scrubbed]");
  });
});
