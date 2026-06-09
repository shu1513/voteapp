import { describe, expect, it } from "vitest";

import { createInMemoryAddressApiRateLimiter } from "../../src/api/addressApiRateLimiter.js";

function request(clientIp: string) {
  return {
    clientIp,
    method: "POST",
    pathname: "/api/address/resolve",
  };
}

describe("createInMemoryAddressApiRateLimiter", () => {
  it("rejects requests after the per-window limit", () => {
    let now = 1_000;
    const limiter = createInMemoryAddressApiRateLimiter({
      windowMs: 60_000,
      maxRequests: 2,
      now: () => now,
    });

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.10"))).toEqual({
      allowed: false,
      retryAfterSeconds: 60,
    });

    now += 60_000;
    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
  });

  it("sweeps expired buckets lazily", () => {
    let now = 1_000;
    const limiter = createInMemoryAddressApiRateLimiter({
      windowMs: 60_000,
      maxRequests: 1,
      sweepIntervalMs: 60_000,
      now: () => now,
    });

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.11"))).toEqual({ allowed: true });
    expect(limiter.getBucketCount()).toBe(2);

    now += 60_000;
    expect(limiter(request("203.0.113.12"))).toEqual({ allowed: true });
    expect(limiter.getBucketCount()).toBe(1);
  });

  it("caps total buckets by evicting oldest entries", () => {
    const limiter = createInMemoryAddressApiRateLimiter({
      windowMs: 60_000,
      maxRequests: 1,
      maxBuckets: 2,
      now: () => 1_000,
    });

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.11"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.12"))).toEqual({ allowed: true });
    expect(limiter.getBucketCount()).toBe(2);

    // The first IP was evicted to keep the map bounded, so it starts a fresh bucket.
    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter.getBucketCount()).toBe(2);
  });
});
