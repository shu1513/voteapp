import { describe, expect, it } from "vitest";
import type { ErrorEvent } from "@sentry/react";

import { scrubSentryEvent, scrubText } from "./errorMonitoring";

describe("scrubText", () => {
  it("masks emails and query strings", () => {
    expect(scrubText("voter@example.com hit /ballot?d=abc,def")).toBe("[email] hit /ballot?[scrubbed]");
  });
});

describe("scrubSentryEvent", () => {
  it("drops request, user, breadcrumbs, and extra", () => {
    const event = {
      request: { url: "https://voteapp.example/ballot?d=abc" },
      user: { ip_address: "203.0.113.9" },
      breadcrumbs: [{ message: "clicked" }],
      extra: { body: { address: "123 Main St" } },
    } as unknown as ErrorEvent;

    const scrubbed = scrubSentryEvent(event);

    expect(scrubbed.request).toBeUndefined();
    expect(scrubbed.user).toBeUndefined();
    expect(scrubbed.breadcrumbs).toBeUndefined();
    expect(scrubbed.extra).toBeUndefined();
  });

  it("masks emails and query strings in messages, exception values, frames, tags, and contexts", () => {
    const event = {
      message: "boom for voter@example.com",
      exception: {
        values: [
          {
            type: "Error",
            value: "failed at /elections/x?d=1 for voter@example.com",
            stacktrace: { frames: [{ filename: "https://voteapp.example/assets/app.js?v=123" }] },
          },
        ],
      },
      tags: { page: "/ballot?d=abc", plain: "ok" },
      contexts: { custom: { note: "voter@example.com", list: ["/x?q=1"] } },
    } as unknown as ErrorEvent;

    const scrubbed = scrubSentryEvent(event);

    expect(scrubbed.message).toBe("boom for [email]");
    expect(scrubbed.exception?.values?.[0]?.value).toBe("failed at /elections/x?[scrubbed] for [email]");
    expect(scrubbed.exception?.values?.[0]?.stacktrace?.frames?.[0]?.filename).toBe(
      "https://voteapp.example/assets/app.js?[scrubbed]"
    );
    expect(scrubbed.tags).toEqual({ page: "/ballot?[scrubbed]", plain: "ok" });
    expect(scrubbed.contexts).toEqual({ custom: { note: "[email]", list: ["/x?[scrubbed]"] } });
  });
});
