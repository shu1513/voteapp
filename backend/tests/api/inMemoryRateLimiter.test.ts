import { describe, expect, it } from "vitest";

import { createInMemoryKeyedRateLimiter } from "../../src/api/inMemoryRateLimiter.js";

describe("createInMemoryKeyedRateLimiter", () => {
  it("counts per key inside the window and blocks past maxRequests", () => {
    let now = 1_000;
    const limit = createInMemoryKeyedRateLimiter({ windowMs: 60_000, maxRequests: 2, maxBuckets: 10, now: () => now }, (k: string) => k);

    expect(limit("a")).toEqual({ allowed: true });
    expect(limit("a")).toEqual({ allowed: true });
    expect(limit("a")).toEqual({ allowed: false, retryAfterSeconds: 60 });
    // Another key is unaffected; the window resets the first key.
    expect(limit("b")).toEqual({ allowed: true });
    now += 60_000;
    expect(limit("a")).toEqual({ allowed: true });
  });

  it("under cap pressure evicts the least recently hit key, not the blocked one being hammered", () => {
    const now = 1_000;
    const limit = createInMemoryKeyedRateLimiter({ windowMs: 60_000, maxRequests: 1, maxBuckets: 2, now: () => now }, (k: string) => k);

    // "hot" is inserted first, then blocked and kept hot; "cold" is inserted
    // later and never hit again.
    expect(limit("hot")).toEqual({ allowed: true });
    expect(limit("cold")).toEqual({ allowed: true });
    expect(limit("hot").allowed).toBe(false);
    expect(limit("hot").allowed).toBe(false);

    // A third key needs room: with insertion-order eviction "hot" (the
    // oldest insert) would go and its counter would reset, letting the
    // hammered key through again.
    expect(limit("new")).toEqual({ allowed: true });
    expect(limit("hot").allowed).toBe(false);
    expect(limit.getBucketCount()).toBe(2);
  });
});
