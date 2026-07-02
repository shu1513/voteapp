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

  it("keeps auth routes isolated by pathname", () => {
    const limiter = createInMemoryAuthApiRateLimiter({
      windowMs: 60_000,
      maxRequestsPerIp: 1,
      maxRequestsPerEmail: 1,
      now: () => 1_000,
    });

    expect(limiter(request({ clientIp: "203.0.113.10", email: "user@example.com", pathname: "/api/auth/register" }))).toEqual({
      allowed: true,
    });
    expect(limiter(request({ clientIp: "203.0.113.10", email: "user@example.com", pathname: "/api/auth/register" }))).toEqual({
      allowed: false,
      retryAfterSeconds: 60,
    });
    expect(limiter(request({ clientIp: "203.0.113.10", email: "user@example.com", pathname: "/api/auth/login" }))).toEqual({
      allowed: true,
    });
  });
});
