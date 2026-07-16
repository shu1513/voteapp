export const CORS_ALLOW_METHODS = "GET, POST, PUT, DELETE, OPTIONS";
// authorization / x-voteapp-client: the mobile session transport. Native
// requests skip CORS entirely; this only keeps browser-based dev tooling from
// failing preflight. Browser-originated requests never receive a session id
// regardless (the mobile transport requires the absence of browser
// provenance), so allowing these headers grants nothing.
export const CORS_ALLOW_HEADERS = "authorization, content-type, x-voteapp-client";
export const CORS_MAX_AGE_SECONDS = "600";

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
    return { ok: false, headers: { vary: "Origin" } };
  }

  return {
    ok: true,
    headers: {
      "access-control-allow-origin": allowAnyOrigin ? "*" : origin,
      ...(allowAnyOrigin ? {} : { "access-control-allow-credentials": "true" }),
      "access-control-allow-methods": CORS_ALLOW_METHODS,
      "access-control-allow-headers": CORS_ALLOW_HEADERS,
      "access-control-max-age": CORS_MAX_AGE_SECONDS,
      vary: "Origin",
    },
  };
}
