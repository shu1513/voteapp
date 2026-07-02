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
