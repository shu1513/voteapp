import { describe, expect, it, vi } from "vitest";

import { AUTH_SESSION_COOKIE_NAME } from "../../src/auth/authCookies.js";
import {
  assertTrustedUserIdHeaderConfigIsSafe,
  createSessionAwareTrustedUserIdResolver,
  createTrustedUserIdResolver,
} from "../../src/api/addressApiAuth.js";

describe("session-aware authenticated user resolver", () => {
  it("uses the Redis session cookie before the trusted header", async () => {
    const redis = {
      get: vi.fn().mockResolvedValue("99999999-9999-4999-8999-999999999999"),
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
