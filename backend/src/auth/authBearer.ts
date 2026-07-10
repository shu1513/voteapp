// Bearer transport for the opaque auth session id. Mobile clients cannot use
// httpOnly cookies, so they send the same session id as
// `Authorization: Bearer <sessionId>`; the cookie remains the only transport
// the web frontend ever sees.

/**
 * Extracts the session id from an Authorization header value. Returns null
 * for missing headers, non-Bearer schemes, and empty credentials. The scheme
 * match is case-insensitive per RFC 9110.
 */
export function parseBearerAuthorizationValue(value: string | string[] | undefined): string | null {
  const rawValue = Array.isArray(value) ? value[0] : value;
  if (!rawValue) {
    return null;
  }
  const match = /^Bearer\s+(\S+)$/i.exec(rawValue.trim());
  return match ? match[1] : null;
}
