// Server-side data fetch for route loaders. Framework mode strips `loader`
// exports (and this module with them) from the client bundle, so this only
// ever runs in Node — first on the SSR document request, then via the
// framework's own .data requests on client navigations.
//
// Deliberately a bare fetch: never forward the browser's cookies or auth
// headers. Both detail endpoints return session-dependent fields
// (is_following / follow) when a session cookie rides along; forwarding
// would bake one user's personalized state into server HTML that crawlers
// read and a CDN would cache for everyone. Personalization stays in
// client-side TanStack Query.

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

export async function loadFromApi<T>(path: string): Promise<T> {
  const base = process.env.API_INTERNAL_URL ?? "http://127.0.0.1:3001";
  let response: Response;
  try {
    response = await fetch(`${base}${path}`, { signal: AbortSignal.timeout(LOADER_TIMEOUT_MS) });
  } catch (error) {
    rethrowTimeoutAs504(error);
  }
  // 400 covers malformed ids (the API rejects non-UUID path segments);
  // for a crawler or visitor that is the same thing as not existing.
  if (response.status === 404 || response.status === 400) {
    throw new Response("Not Found", { status: 404 });
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
