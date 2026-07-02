import { describe, expect, it } from "vitest";

import { createInMemoryAuthApiRateLimiter } from "../../src/api/authApiRateLimiter.js";

function request(input: { clientIp: string; email: string; pathname: string }) {
  return {
    clientIp: input.clientIp,
    email: input.email,
    method: "POST",
    pathname: input.pathname,
  };
}

describe("createInMemoryAuthApiRateLimiter", () => {
  it("limits requests by IP and email on the same auth route", () => {
    let now = 1_000;
    const limiter = createInMemoryAuthApiRateLimiter({
      windowMs: 60_000,
      maxRequestsPerIp: 2,
      maxRequestsPerEmail: 2,
      now: () => now,
    });

    expect(limiter(request({ clientIp: "203.0.113.10", email: "user@example.com", pathname: "/api/auth/login" }))).toEqual({
      allowed: true,
    });
    expect(limiter(request({ clientIp: "203.0.113.11", email: "user@example.com", pathname: "/api/auth/login" }))).toEqual({
      allowed: true,
    });
    expect(limiter(request({ clientIp: "203.0.113.12", email: "user@example.com", pathname: "/api/auth/login" }))).toEqual({
      allowed: false,
      retryAfterSeconds: 60,
    });
  });

  it("limits requests by IP even when the email changes", () => {
    const limiter = createInMemoryAuthApiRateLimiter({
      windowMs: 60_000,
      maxRequestsPerIp: 2,
      maxRequestsPerEmail: 10,
      now: () => 1_000,
    });

    expect(limiter(request({ clientIp: "203.0.113.10", email: "one@example.com", pathname: "/api/auth/register" }))).toEqual({
      allowed: true,
    });
    expect(limiter(request({ clientIp: "203.0.113.10", email: "two@example.com", pathname: "/api/auth/register" }))).toEqual({
      allowed: true,
    });
    expect(limiter(request({ clientIp: "203.0.113.10", email: "three@example.com", pathname: "/api/auth/register" }))).toEqual({
      allowed: false,
      retryAfterSeconds: 60,
    });
  });

  it("shares one bucket across all auth routes so spreading endpoints cannot multiply quota", () => {
    const limiter = createInMemoryAuthApiRateLimiter({
      windowMs: 60_000,
      maxRequestsPerIp: 1,
      maxRequestsPerEmail: 1,
      now: () => 1_000,
    });

    expect(limiter(request({ clientIp: "203.0.113.10", email: "user@example.com", pathname: "/api/auth/register" }))).toEqual({
      allowed: true,
    });
    expect(limiter(request({ clientIp: "203.0.113.10", email: "user@example.com", pathname: "/api/auth/login" }))).toEqual({
      allowed: false,
      retryAfterSeconds: 60,
    });
    expect(limiter(request({ clientIp: "203.0.113.10", email: "user@example.com", pathname: "/api/auth/forgot-password" }))).toEqual({
      allowed: false,
      retryAfterSeconds: 60,
    });
  });

  it("evicts by recency so hot buckets survive the cap instead of being reset", () => {
    let now = 1_000;
    const limiter = createInMemoryAuthApiRateLimiter({
      windowMs: 60_000,
      maxRequestsPerIp: 3,
      maxRequestsPerEmail: 100,
      maxBuckets: 4,
      sweepIntervalMs: 10 * 60_000,
      now: () => now,
    });

    // Hot pair first, cold pair second, then refresh the hot pair so it is
    // the most recently used despite its earlier insertion.
    limiter(request({ clientIp: "203.0.113.10", email: "hot@example.com", pathname: "/api/auth/login" }));
    now += 1;
    limiter(request({ clientIp: "198.51.100.20", email: "cold@example.com", pathname: "/api/auth/login" }));
    now += 1;
    limiter(request({ clientIp: "203.0.113.10", email: "hot@example.com", pathname: "/api/auth/login" }));
    now += 1;
    // New pair forces eviction of two buckets: the cold pair must go.
    limiter(request({ clientIp: "192.0.2.30", email: "new@example.com", pathname: "/api/auth/login" }));
    now += 1;

    // If the hot IP bucket had been evicted and reset, this third hit would
    // recreate it at count 1 and a fourth hit would still pass; with the
    // counter intact (2 prior hits), the third passes and the fourth blocks.
    expect(limiter(request({ clientIp: "203.0.113.10", email: "hot@example.com", pathname: "/api/auth/login" }))).toEqual({
      allowed: true,
    });
    expect(limiter(request({ clientIp: "203.0.113.10", email: "hot@example.com", pathname: "/api/auth/login" }))).toEqual({
      allowed: false,
      retryAfterSeconds: 60,
    });
  });
});
