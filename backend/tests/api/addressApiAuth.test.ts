import { describe, expect, it, vi } from "vitest";

import { AUTH_SESSION_COOKIE_NAME } from "../../src/auth/authCookies.js";
import { hashSessionId } from "../../src/auth/authPrimitives.js";
import {
  assertTrustedUserIdHeaderConfigIsSafe,
  createSessionAwareTrustedUserIdResolver,
  createTrustedUserIdResolver,
} from "../../src/api/addressApiAuth.js";

describe("session-aware authenticated user resolver", () => {
  it("uses the Redis session cookie before the trusted header", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999:1"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue("88888888-8888-4888-8888-888888888888");
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
    });

    const userId = await resolveAuthenticatedUserId({
      headers: {
        cookie: `${AUTH_SESSION_COOKIE_NAME}=session-abc`,
        "x-user-id": "88888888-8888-4888-8888-888888888888",
      },
    });

    expect(userId).toBe("99999999-9999-4999-8999-999999999999");
    expect(redis.get).toHaveBeenCalledOnce();
    expect(trustedUserIdResolver).not.toHaveBeenCalled();
  });

  it("authenticates only when the session epoch matches the user's current epoch", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999:2"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue(null);
    const lookupUserSessionEpoch = vi.fn().mockResolvedValue(2);
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
      lookupUserSessionEpoch,
    });

    const userId = await resolveAuthenticatedUserId({
      headers: { cookie: `${AUTH_SESSION_COOKIE_NAME}=session-abc` },
    });

    expect(userId).toBe("99999999-9999-4999-8999-999999999999");
    expect(lookupUserSessionEpoch).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999");
  });

  it("rejects a session whose epoch is stale (password was reset after login)", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999:1"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue(null);
    // The reset bumped users.session_epoch to 2; this session captured 1.
    const lookupUserSessionEpoch = vi.fn().mockResolvedValue(2);
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
      lookupUserSessionEpoch,
    });

    const userId = await resolveAuthenticatedUserId({
      headers: { cookie: `${AUTH_SESSION_COOKIE_NAME}=session-abc` },
    });

    expect(userId).toBeNull();
  });

  it("rejects a session whose user no longer exists (deleted account)", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999:1"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue(null);
    const lookupUserSessionEpoch = vi.fn().mockResolvedValue(null);
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
      lookupUserSessionEpoch,
    });

    const userId = await resolveAuthenticatedUserId({
      headers: { cookie: `${AUTH_SESSION_COOKIE_NAME}=session-abc` },
    });

    expect(userId).toBeNull();
  });

  it("fails closed when the epoch lookup itself fails", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999:1"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue(null);
    const lookupUserSessionEpoch = vi.fn().mockRejectedValue(new Error("db down"));
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
      lookupUserSessionEpoch,
    });

    const userId = await resolveAuthenticatedUserId({
      headers: { cookie: `${AUTH_SESSION_COOKIE_NAME}=session-abc` },
    });

    expect(userId).toBeNull();
  });

  it("treats a legacy plain-userId session value as no session", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue(null);
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
      lookupUserSessionEpoch: vi.fn().mockResolvedValue(1),
    });

    const userId = await resolveAuthenticatedUserId({
      headers: { cookie: `${AUTH_SESSION_COOKIE_NAME}=session-abc` },
    });

    expect(userId).toBeNull();
    expect(trustedUserIdResolver).toHaveBeenCalled();
  });

  it("authenticates from an Authorization: Bearer session id (mobile transport)", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999:1"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue(null);
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
      lookupUserSessionEpoch: vi.fn().mockResolvedValue(1),
    });

    const userId = await resolveAuthenticatedUserId({
      headers: { authorization: "Bearer session-abc" },
    });

    expect(userId).toBe("99999999-9999-4999-8999-999999999999");
    expect(redis.get).toHaveBeenCalledOnce();
  });

  it("prefers the session cookie over a Bearer header when both are present", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999:1"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue(null);
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
    });

    await resolveAuthenticatedUserId({
      headers: {
        cookie: `${AUTH_SESSION_COOKIE_NAME}=cookie-session`,
        authorization: "Bearer bearer-session",
      },
    });

    expect(redis.get).toHaveBeenCalledOnce();
    expect(String(redis.get.mock.calls[0]?.[0])).toContain(hashSessionId("cookie-session"));
    expect(String(redis.get.mock.calls[0]?.[0])).not.toContain(hashSessionId("bearer-session"));
  });

  it("applies the same epoch revocation to bearer sessions", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999:1"),
    };
    const trustedUserIdResolver = vi.fn().mockReturnValue(null);
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
      lookupUserSessionEpoch: vi.fn().mockResolvedValue(2),
    });

    const userId = await resolveAuthenticatedUserId({
      headers: { authorization: "Bearer session-abc" },
    });

    expect(userId).toBeNull();
  });

  it("falls back to the trusted header when there is no session cookie", async () => {
    const redis = {
      get: vi.fn(),
    };
    const trustedUserIdResolver = createTrustedUserIdResolver("x-user-id");
    const resolveAuthenticatedUserId = createSessionAwareTrustedUserIdResolver({
      redis,
      trustedUserIdResolver,
    });

    const userId = await resolveAuthenticatedUserId({
      headers: {
        "x-user-id": "88888888-8888-4888-8888-888888888888",
      },
    });

    expect(userId).toBe("88888888-8888-4888-8888-888888888888");
    expect(redis.get).not.toHaveBeenCalled();
  });
});

describe("assertTrustedUserIdHeaderConfigIsSafe", () => {
  it("fails closed when session auth and a trusted user-id header are both configured", () => {
    expect(() =>
      assertTrustedUserIdHeaderConfigIsSafe({
        sessionAuthIntended: true,
        trustedUserIdHeader: "X-User-Id",
        allowTrustedHeaderWithSessions: false,
      })
    ).toThrow("API_TRUSTED_USER_ID_HEADER");
  });

  it("allows the combination only with the explicit gateway opt-in", () => {
    expect(() =>
      assertTrustedUserIdHeaderConfigIsSafe({
        sessionAuthIntended: true,
        trustedUserIdHeader: "X-User-Id",
        allowTrustedHeaderWithSessions: true,
      })
    ).not.toThrow();
  });

  it("allows either mechanism alone", () => {
    expect(() =>
      assertTrustedUserIdHeaderConfigIsSafe({
        sessionAuthIntended: false,
        trustedUserIdHeader: "X-User-Id",
        allowTrustedHeaderWithSessions: false,
      })
    ).not.toThrow();
    expect(() =>
      assertTrustedUserIdHeaderConfigIsSafe({
        sessionAuthIntended: true,
        trustedUserIdHeader: null,
        allowTrustedHeaderWithSessions: false,
      })
    ).not.toThrow();
  });
});
