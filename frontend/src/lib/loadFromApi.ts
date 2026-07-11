// Server-side data fetch for route loaders. Framework mode strips `loader`
// exports (and this module with them) from the client bundle, so this only
// ever runs in Node — first on the SSR document request, then via the
// framework's own .data requests on client navigations.
//
// Deliberately near-bare fetch: never forward the browser's cookies or auth
// headers. Both detail endpoints return session-dependent fields
// (is_following / follow) when a session cookie rides along; forwarding
// would bake one user's personalized state into server HTML that crawlers
// read and a CDN would cache for everyone. Personalization stays in
// client-side TanStack Query.
//
// The ONE header that does get relayed is the deployment's trusted
// client-IP header (ADDRESS_API_TRUSTED_CLIENT_IP_HEADER — same env var the
// API server reads). The API rate-limits per client IP; without the relay,
// every loader fetch arrives from the SSR server's own socket IP and the
// entire site's detail-page traffic shares a single rate-limit bucket — one
// sitemap-following crawler could 429 the bucket and take detail pages down
// for everyone. The value is relayed verbatim from the incoming request
// (the edge proxy sets it); when the env var or header is absent (dev),
// nothing is sent and the API falls back to socket-IP keying as before.

// Generous for a loopback hop (normally milliseconds): a stalled API must
// fail the render fast instead of pinning SSR request handlers until
// undici's ~300s default timeouts fire.
const LOADER_TIMEOUT_MS = 10_000;

function rethrowTimeoutAs504(error: unknown): never {
  if (error instanceof DOMException && error.name === "TimeoutError") {
    throw new Response("Upstream API timeout", { status: 504 });
  }
  throw error;
}

/**
 * API base resolution, most explicit first:
 * 1. API_INTERNAL_URL — full URL. Dev override, or a deployment where the
 *    private network can't be used (Render free instances can send private
 *    traffic but not receive it, so the API's public URL goes here).
 * 2. API_INTERNAL_HOSTPORT — "host:port" on the private network, e.g. a
 *    Render blueprint `fromService` hostport reference that tracks the
 *    assigned internal hostname across service recreation. Plain http:
 *    private-network traffic isn't TLS-terminated.
 * 3. Loopback default for local dev.
 */
function resolveApiBase(): string {
  // Paths are appended verbatim and always start with "/"; a trailing slash
  // on either env var would produce "//api/..." — which the API server's
  // exact path matching 404s — so strip it rather than 404 every loader.
  const explicitUrl = process.env.API_INTERNAL_URL;
  if (explicitUrl) {
    return explicitUrl.replace(/\/+$/, "");
  }
  const hostport = process.env.API_INTERNAL_HOSTPORT;
  if (hostport) {
    return `http://${hostport.replace(/\/+$/, "")}`;
  }
  return "http://127.0.0.1:3001";
}

export async function loadFromApi<T>(path: string, incomingRequest: Request): Promise<T> {
  const base = resolveApiBase();
  const headers = new Headers();
  const trustedIpHeader = process.env.ADDRESS_API_TRUSTED_CLIENT_IP_HEADER;
  if (trustedIpHeader) {
    const clientIpValue = incomingRequest.headers.get(trustedIpHeader);
    if (clientIpValue) {
      headers.set(trustedIpHeader, clientIpValue);
    }
  }

  let response: Response;
  try {
    response = await fetch(`${base}${path}`, {
      headers,
      signal: AbortSignal.timeout(LOADER_TIMEOUT_MS),
    });
  } catch (error) {
    rethrowTimeoutAs504(error);
  }
  // 400 covers malformed ids (the API rejects non-UUID path segments);
  // for a crawler or visitor that is the same thing as not existing.
  if (response.status === 404 || response.status === 400) {
    throw new Response("Not Found", { status: 404 });
  }
  // Pass rate limiting through as 429 (a crawler backs off on 429; a 502
  // reads as an outage) and keep the API's retry-after hint.
  if (response.status === 429) {
    const retryAfter = response.headers.get("retry-after");
    throw new Response("Rate limited", {
      status: 429,
      headers: retryAfter ? { "retry-after": retryAfter } : undefined,
    });
  }
  if (!response.ok) {
    throw new Response("Upstream API error", { status: 502 });
  }
  try {
    // The timeout signal also aborts a stalled body read, not just the
    // connection.
    return (await response.json()) as T;
  } catch (error) {
    rethrowTimeoutAs504(error);
  }
}
