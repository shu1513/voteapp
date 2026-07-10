import { parseBearerAuthorizationValue } from "../auth/authBearer.js";
import { AUTH_SESSION_COOKIE_NAME, parseCookieHeaderValue } from "../auth/authCookies.js";
import { resolveAuthSession, type AuthSessionRedisClient } from "../auth/authSessionStore.js";
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
  /**
   * Current users.session_epoch for a user id, or null when the user does
   * not exist / is deleted. When provided, a session only authenticates if
   * its stored epoch matches — this is what makes password reset/change and
   * logout-all revoke sessions even when the Redis destroy failed, and what
   * kills an old-password login session created concurrently with a reset.
   * Lookup failures fail closed (the session does not authenticate).
   */
  lookupUserSessionEpoch?: (userId: string) => Promise<number | null>;
  cookieName?: string;
}): (input: AddressApiAuthenticatedUserInput) => Promise<string | null> {
  const cookieName = options.cookieName ?? AUTH_SESSION_COOKIE_NAME;

  return async (input) => {
    // Same opaque session id, two transports: the web frontend's httpOnly
    // cookie wins when present; mobile clients send it as a Bearer header.
    const sessionId =
      parseCookieHeaderValue(readHeader(input.headers, "cookie"), cookieName) ??
      parseBearerAuthorizationValue(readHeader(input.headers, "authorization"));
    if (sessionId && options.redis) {
      try {
        const session = await resolveAuthSession(options.redis, sessionId);
        if (session) {
          if (!options.lookupUserSessionEpoch) {
            return session.userId;
          }
          const currentEpoch = await options.lookupUserSessionEpoch(session.userId);
          if (currentEpoch !== null && currentEpoch === session.sessionEpoch) {
            return session.userId;
          }
          // Stale epoch or missing/deleted user: the session is revoked and
          // behaves exactly like no session (same trusted-header fallback,
          // which resolves to null unless a gateway deployment opted in).
          return options.trustedUserIdResolver(input);
        }
      } catch {
        // Fall back to the trusted header path below.
      }
    }

    return options.trustedUserIdResolver(input);
  };
}
