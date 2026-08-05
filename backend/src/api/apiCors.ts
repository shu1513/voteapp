export const CORS_ALLOW_METHODS = "GET, POST, PUT, DELETE, OPTIONS";
// authorization / x-voteapp-client: the mobile session transport. Native
// requests skip CORS entirely; this only keeps browser-based dev tooling from
// failing preflight. Browser-originated requests never receive a session id
// regardless (the mobile transport requires the absence of browser
// provenance), so allowing these headers grants nothing.
export const CORS_ALLOW_HEADERS = "authorization, content-type, x-voteapp-client";
export const CORS_MAX_AGE_SECONDS = "600";

// Acceptance depends on Origin AND (via the same-origin fallback below)
// Sec-Fetch-Site, so shared caches must key on both. Emitted identically on
// every path — allowed, rejected, and fallback. A resource whose Vary value
// changes per request is the actual hazard: a cache that stored a response
// under `Vary: Origin` has no reason to re-key when a later response widens
// the set, so it would serve the stored copy to a request that varies only in
// the header it never recorded.
export const CORS_VARY = "Origin, Sec-Fetch-Site";

export type HeaderRecord = Record<string, string | string[] | undefined>;

export type CorsResolution = {
  ok: boolean;
  headers: Record<string, string>;
};

function normalizeAllowedOrigins(origins: readonly string[] | undefined): Set<string> {
  return new Set((origins ?? []).map((origin) => origin.trim()).filter((origin) => origin.length > 0));
}

// Only http(s) URLs yield a usable origin: every other scheme (data:, file:,
// javascript:, ...) parses to origin "null", which browsers also send as
// `Origin: null` from sandboxed iframes — allowlisting it would grant
// credentialed CORS to those contexts.
function toHttpOrigin(value: string): string | null {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:" ? url.origin : null;
  } catch {
    return null;
  }
}

// Browsers attach an Origin header to every non-GET request even when the
// request is same-origin, so a same-origin deployment still needs its own
// origin allowlisted or every login/settings/unsubscribe POST 403s. Folding
// in the deployment's public base URLs keeps an unset allowlist env var from
// breaking same-origin traffic.
export function buildAllowedOrigins(
  rawAllowedOrigins: string | undefined,
  fallbackOriginUrls: readonly (string | undefined)[] = []
): string[] {
  const origins = new Set<string>();
  for (const entry of (rawAllowedOrigins ?? "").split(",")) {
    const trimmed = entry.trim();
    if (!trimmed) {
      continue;
    }
    // Normalize http(s) entries to their origin so a trailing slash, path,
    // uppercase host, or explicit default port still matches the browser's
    // Origin header. Anything else ("*", deliberate exact-match strings)
    // stays verbatim — never coerced to "null".
    origins.add(toHttpOrigin(trimmed) ?? trimmed);
  }
  for (const url of fallbackOriginUrls) {
    const trimmed = url?.trim();
    if (!trimmed) {
      continue;
    }
    const origin = toHttpOrigin(trimmed);
    if (origin) {
      origins.add(origin);
    } else {
      // Surface the misconfiguration: a skipped fallback means same-origin
      // browser writes 403 unless the explicit allowlist covers the site.
      console.warn(`buildAllowedOrigins: ignoring non-http(s) or unparseable fallback URL "${trimmed}"`);
    }
  }
  return [...origins];
}

export function readHeader(headers: HeaderRecord | undefined, name: string): string | undefined {
  if (!headers) {
    return undefined;
  }
  const lowerName = name.toLowerCase();
  const value = headers[lowerName] ?? headers[name];
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

export function resolveCorsHeaders(
  headers: HeaderRecord | undefined,
  allowedOrigins: readonly string[] | undefined
): CorsResolution {
  const origin = readHeader(headers, "origin")?.trim();
  if (!origin) {
    return { ok: true, headers: {} };
  }

  const normalizedAllowedOrigins = normalizeAllowedOrigins(allowedOrigins);
  const allowAnyOrigin = normalizedAllowedOrigins.has("*");
  if (!allowAnyOrigin && !normalizedAllowedOrigins.has(origin)) {
    // Same-origin fallback: privacy-hardened browsers, in-app webviews, and
    // referrer-stripping extensions can rewrite the Origin of a same-origin
    // request to "null" (or something else unrecognizable). Sec-Fetch-Site is
    // a forbidden header the browser computes itself — scripts on attacker
    // pages cannot set it, and a cross-site or sandboxed-iframe request never
    // carries "same-origin" — so trusting it here keeps the CSRF gate intact.
    // Non-browser clients can forge it, but they never hold ambient cookies,
    // which is the only thing this origin check protects. Same-origin
    // responses need no access-control-* headers, so return only the cache
    // key.
    if (readHeader(headers, "sec-fetch-site")?.trim().toLowerCase() === "same-origin") {
      return { ok: true, headers: { vary: CORS_VARY } };
    }
    return { ok: false, headers: { vary: CORS_VARY } };
  }

  return {
    ok: true,
    headers: {
      "access-control-allow-origin": allowAnyOrigin ? "*" : origin,
      ...(allowAnyOrigin ? {} : { "access-control-allow-credentials": "true" }),
      "access-control-allow-methods": CORS_ALLOW_METHODS,
      "access-control-allow-headers": CORS_ALLOW_HEADERS,
      "access-control-max-age": CORS_MAX_AGE_SECONDS,
      vary: CORS_VARY,
    },
  };
}

// The CORS check necessarily runs before the rate limiter — a rejected request
// is answered without ever reaching it — so an unauthenticated caller can
// drive one log line per request just by varying the Origin header, and can
// pad each line with a multi-kilobyte header value. The diagnostic this log
// exists for is *which distinct values* real browsers send, not how often, so
// bounding volume costs no signal: identical values collapse to one line per
// cooldown, and a hard per-window ceiling caps randomized floods.
const CORS_LOG_VALUE_MAX_LENGTH = 128;
const CORS_LOG_KEY_COOLDOWN_MS = 10 * 60_000;
const CORS_LOG_WINDOW_MS = 60_000;
const CORS_LOG_MAX_PER_WINDOW = 20;
// Bounds the dedupe map: randomized origins would otherwise grow it without
// limit. Cleared wholesale rather than evicted entry-by-entry — this is a log
// filter, not a cache, and dropping cooldown state only permits a few extra
// lines that the per-window ceiling still bounds.
const CORS_LOG_MAX_TRACKED_KEYS = 500;

/** Caps an attacker-controlled header value before it reaches the log. */
export function truncateForLog(value: string | undefined): string {
  if (value === undefined) {
    return "(absent)";
  }
  return value.length > CORS_LOG_VALUE_MAX_LENGTH
    ? `${value.slice(0, CORS_LOG_VALUE_MAX_LENGTH)}…[${value.length} chars]`
    : value;
}

export type CorsRejectionLogVerdict = {
  shouldLog: boolean;
  /** Rejections dropped since the previous emitted line. Always 0 when shouldLog is false. */
  suppressed: number;
};

export function createCorsRejectionLogThrottle(): (key: string, now?: number) => CorsRejectionLogVerdict {
  const lastLoggedAtByKey = new Map<string, number>();
  let windowStartedAt = 0;
  let loggedInWindow = 0;
  let suppressedSinceLastLog = 0;

  return (key, now = Date.now()) => {
    if (now - windowStartedAt >= CORS_LOG_WINDOW_MS) {
      windowStartedAt = now;
      loggedInWindow = 0;
    }

    const lastLoggedAt = lastLoggedAtByKey.get(key);
    const withinCooldown = lastLoggedAt !== undefined && now - lastLoggedAt < CORS_LOG_KEY_COOLDOWN_MS;
    if (withinCooldown || loggedInWindow >= CORS_LOG_MAX_PER_WINDOW) {
      suppressedSinceLastLog += 1;
      return { shouldLog: false, suppressed: 0 };
    }

    if (lastLoggedAtByKey.size >= CORS_LOG_MAX_TRACKED_KEYS) {
      lastLoggedAtByKey.clear();
    }
    lastLoggedAtByKey.set(key, now);
    loggedInWindow += 1;

    const suppressed = suppressedSinceLastLog;
    suppressedSinceLastLog = 0;
    return { shouldLog: true, suppressed };
  };
}
