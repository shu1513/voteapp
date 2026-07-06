import { describe, expect, it } from "vitest";

import { createInMemoryContentReportRateLimiter } from "../../src/api/contentReportRateLimiter.js";

function request(clientIp: string) {
  return { clientIp, method: "POST", pathname: "/api/content-reports" };
}

describe("createInMemoryContentReportRateLimiter", () => {
  it("limits content reports by IP", () => {
    const limiter = createInMemoryContentReportRateLimiter({ windowMs: 60_000, maxRequests: 2, now: () => 1_000 });

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: false, retryAfterSeconds: 60 });
    expect(limiter(request("203.0.113.11"))).toEqual({ allowed: true });
  });

  it("allows requests again after the rate limit window expires", () => {
    let now = 1_000;
    const limiter = createInMemoryContentReportRateLimiter({ windowMs: 60_000, maxRequests: 1, now: () => now });

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: false, retryAfterSeconds: 60 });

    now = 61_000;

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
  });

  it("evicts old buckets when the bucket cap is reached", () => {
    const limiter = createInMemoryContentReportRateLimiter({
      windowMs: 60_000,
      maxRequests: 1,
      maxBuckets: 2,
      now: () => 1_000,
    });

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.11"))).toEqual({ allowed: true });
    expect(limiter.getBucketCount()).toBe(2);

    expect(limiter(request("203.0.113.12"))).toEqual({ allowed: true });
    expect(limiter.getBucketCount()).toBe(2);

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
  });
});
