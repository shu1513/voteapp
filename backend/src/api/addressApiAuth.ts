import { AUTH_SESSION_COOKIE_NAME, parseCookieHeaderValue } from "../auth/authCookies.js";
import { resolveAuthSessionUserId, type AuthSessionRedisClient } from "../auth/authSessionStore.js";
import type { HeaderRecord } from "./addressApiClientIp.js";

export type AddressApiAuthenticatedUserInput = {
  headers: HeaderRecord | undefined;
};

function readHeader(headers: HeaderRecord | undefined, name: string): string | undefined {
  const lowerName = name.toLowerCase();
  const value = headers?.[lowerName] ?? headers?.[name];
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

export function parseTrustedUserIdHeader(value: string | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

export function createTrustedUserIdResolver(
  trustedUserIdHeader: string | null | undefined
): (input: AddressApiAuthenticatedUserInput) => string | null {
  return (input) => {
    if (!trustedUserIdHeader) {
      return null;
    }
    return parseTrustedUserIdHeader(readHeader(input.headers, trustedUserIdHeader));
  };
}

// Startup guard for the trusted-header fallback. The session-aware resolver
// falls back to the trusted user-id header on every request, so combining
// session auth with a trusted header means any client that can reach the API
// directly (bypassing the gateway) can impersonate any user with one header.
// Deployments that intentionally run both (an authenticated edge gateway in
// front of session auth) must opt in explicitly.
//
// sessionAuthIntended must reflect configuration intent (AUTH_PUBLIC_BASE_URL
// set), not whether the auth service successfully started: a Redis outage at
// boot must not silently re-open the header path.
export function assertTrustedUserIdHeaderConfigIsSafe(input: {
  sessionAuthIntended: boolean;
  trustedUserIdHeader: string | null | undefined;
  allowTrustedHeaderWithSessions: boolean;
}): void {
  if (!input.sessionAuthIntended || !input.trustedUserIdHeader) {
    return;
  }
  if (input.allowTrustedHeaderWithSessions) {
    return;
  }
  throw new Error(
    `API_TRUSTED_USER_ID_HEADER ("${input.trustedUserIdHeader}") is set while session authentication is configured (AUTH_PUBLIC_BASE_URL). ` +
      "The trusted header lets any direct client impersonate any user, so this combination fails closed. " +
      "Unset API_TRUSTED_USER_ID_HEADER, or set API_TRUSTED_USER_ID_HEADER_ALLOW_WITH_SESSIONS=true only when an authenticated edge gateway injects the header and strips client-supplied copies."
  );
}

export function createSessionAwareTrustedUserIdResolver(options: {
  redis: Pick<AuthSessionRedisClient, "get"> | null;
  trustedUserIdResolver: (input: AddressApiAuthenticatedUserInput) => string | null;
  cookieName?: string;
}): (input: AddressApiAuthenticatedUserInput) => Promise<string | null> {
  const cookieName = options.cookieName ?? AUTH_SESSION_COOKIE_NAME;

  return async (input) => {
    const cookieSessionId = parseCookieHeaderValue(readHeader(input.headers, "cookie"), cookieName);
    if (cookieSessionId && options.redis) {
      try {
        const sessionUserId = await resolveAuthSessionUserId(options.redis, cookieSessionId);
        if (sessionUserId) {
          return sessionUserId;
        }
      } catch {
        // Fall back to the trusted header path below.
      }
    }

    return options.trustedUserIdResolver(input);
  };
}
