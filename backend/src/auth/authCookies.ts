export const AUTH_SESSION_COOKIE_NAME = "voteapp_auth_session";

export type AuthSessionCookieOptions = {
  cookieName?: string;
  secure?: boolean;
  sameSite?: "lax" | "strict" | "none";
  domain?: string | null;
  path?: string;
  maxAgeSeconds?: number | null;
};

function normalizeCookieName(cookieName: string | undefined): string {
  const normalized = cookieName?.trim();
  return normalized && normalized.length > 0 ? normalized : AUTH_SESSION_COOKIE_NAME;
}

function normalizePath(path: string | undefined): string {
  const normalized = path?.trim();
  return normalized && normalized.length > 0 ? normalized : "/";
}

function normalizeSessionCookieValue(value: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new Error("Session cookie value must be a non-empty string");
  }
  return normalized;
}

export function parseCookieHeaderValue(
  cookieHeader: string | string[] | undefined,
  cookieName: string
): string | null {
  const normalizedCookieName = normalizeCookieName(cookieName);
  const rawCookieHeader = Array.isArray(cookieHeader) ? cookieHeader[0] : cookieHeader;
  if (!rawCookieHeader || rawCookieHeader.trim().length === 0) {
    return null;
  }

  for (const pair of rawCookieHeader.split(";")) {
    const trimmedPair = pair.trim();
    if (trimmedPair.length === 0) {
      continue;
    }
    const separatorIndex = trimmedPair.indexOf("=");
    if (separatorIndex <= 0) {
      continue;
    }
    const key = trimmedPair.slice(0, separatorIndex).trim();
    if (key !== normalizedCookieName) {
      continue;
    }
    const value = trimmedPair.slice(separatorIndex + 1).trim();
    return value.length > 0 ? value : null;
  }

  return null;
}

export function serializeAuthSessionCookie(
  sessionId: string,
  options: AuthSessionCookieOptions = {}
): string {
  const parts = [
    `${normalizeCookieName(options.cookieName)}=${normalizeSessionCookieValue(sessionId)}`,
    `Path=${normalizePath(options.path)}`,
    "HttpOnly",
    `SameSite=${(options.sameSite ?? "lax").replace(/^./, (char) => char.toUpperCase())}`,
  ];
  if (options.domain) {
    const domain = options.domain.trim();
    if (domain.length > 0) {
      parts.push(`Domain=${domain}`);
    }
  }
  if (options.secure) {
    parts.push("Secure");
  }
  if (options.maxAgeSeconds !== undefined && options.maxAgeSeconds !== null) {
    const maxAgeSeconds = Math.trunc(options.maxAgeSeconds);
    if (!Number.isFinite(maxAgeSeconds) || maxAgeSeconds < 0) {
      throw new Error("maxAgeSeconds must be a non-negative finite number");
    }
    parts.push(`Max-Age=${maxAgeSeconds}`);
  }
  return parts.join("; ");
}

export function serializeClearedAuthSessionCookie(options: AuthSessionCookieOptions = {}): string {
  return [
    `${normalizeCookieName(options.cookieName)}=`,
    `Path=${normalizePath(options.path)}`,
    "HttpOnly",
    `SameSite=${(options.sameSite ?? "lax").replace(/^./, (char) => char.toUpperCase())}`,
    "Expires=Thu, 01 Jan 1970 00:00:00 GMT",
    "Max-Age=0",
    ...(options.domain
      ? (() => {
          const domain = options.domain.trim();
          return domain.length > 0 ? [`Domain=${domain}`] : [];
        })()
      : []),
    ...(options.secure ? ["Secure"] : []),
  ].join("; ");
}
