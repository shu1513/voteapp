// Thin fetch wrapper for the VoteApp API. Same-origin (Vite dev proxy /
// same-site deploy), JSON in/out, parses the standard error envelope
// { error: { code, message } } into a typed ApiError.

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  /** Seconds from the retry-after header on 429 responses. */
  readonly retryAfterSeconds: number | null;
  /** Backend correlation id from unexpected-500 envelopes: matches the
   * server's log line and Sentry event for the same failure. */
  readonly requestId: string | null;

  constructor(
    status: number,
    code: string,
    message: string,
    retryAfterSeconds: number | null = null,
    requestId: string | null = null
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.retryAfterSeconds = retryAfterSeconds;
    this.requestId = requestId;
  }
}

type RequestOptions = {
  method?: "GET" | "POST" | "PUT" | "DELETE";
  body?: unknown;
  /** Caller-side cancellation (e.g. superseded autocomplete requests). */
  signal?: AbortSignal;
};

export const REQUEST_TIMEOUT_MS = 15_000;

type ApiClientConfig = {
  /**
   * Origin prepended to request paths. Empty (the default) keeps the web
   * behavior: same-origin relative paths through the Vite proxy / same-site
   * deploy. The mobile app sets its API origin here.
   */
  baseUrl: string;
  /**
   * Returns the Authorization header value (e.g. "Bearer <sessionId>") or
   * null when there is no session. Null (the default) sends no header — the
   * web frontend authenticates with the httpOnly cookie instead.
   */
  getAuthHeader: (() => string | null | Promise<string | null>) | null;
  /**
   * Headers attached to every request (lowercase names). The mobile app
   * sends { "x-voteapp-client": "mobile" } so login/password-change return
   * the session id in the body instead of a Set-Cookie. Computed headers
   * (content-type, authorization) win on collision.
   */
  defaultHeaders: Record<string, string>;
  /**
   * Per-request timeout ceiling. The default suits an always-on API; a
   * deployment whose API cold-starts from idle (e.g. a free-tier instance
   * that takes ~a minute to wake) raises it so requests ride out the wake
   * instead of failing at the ceiling.
   */
  requestTimeoutMs: number;
};

const config: ApiClientConfig = {
  baseUrl: "",
  getAuthHeader: null,
  defaultHeaders: {},
  requestTimeoutMs: REQUEST_TIMEOUT_MS,
};

/** Platform setup, called once at app start; web apps need no call at all. */
export function configureApi(overrides: Partial<ApiClientConfig>): void {
  Object.assign(config, overrides);
  // A trailing slash would produce "https://host//api/..." on every request.
  config.baseUrl = config.baseUrl.replace(/\/+$/, "");
  // Callers often derive this from an env string; a NaN or nonpositive value
  // would silently disable or break the timeout, so fall back to the default.
  if (!Number.isInteger(config.requestTimeoutMs) || config.requestTimeoutMs <= 0) {
    config.requestTimeoutMs = REQUEST_TIMEOUT_MS;
  }
}

/**
 * AbortSignal.timeout (Chrome 103+/Safari 16+/Node 17.3+) with a manual
 * timer fallback: on runtimes without it (older Safari, Hermes) the timeout
 * must degrade to a timer, not silently vanish — a stalled request with no
 * ceiling pins every query in pending forever ("Loading…" with no way out).
 */
function makeTimeoutSignal(): AbortSignal {
  if (typeof AbortSignal.timeout === "function") {
    return AbortSignal.timeout(config.requestTimeoutMs);
  }
  const controller = new AbortController();
  // Mirrors the native TimeoutError reason where DOMException exists; a bare
  // abort() on runtimes without it still fails the request. The one-shot
  // timer firing after the request settles is a no-op, so it is not cleared.
  setTimeout(() => {
    controller.abort(
      typeof DOMException === "function" ? new DOMException("signal timed out", "TimeoutError") : undefined
    );
  }, config.requestTimeoutMs);
  return controller.signal;
}

/**
 * Combines the request timeout with an optional caller signal without
 * requiring AbortSignal.any (Chrome 116+/Safari 17.4+): on older browsers a
 * missing .any must degrade to a manual combine, not throw before fetch and
 * silently kill features like autocomplete.
 *
 * The manual combine leaves its { once } listener on the caller signal until
 * that signal aborts or is collected, so callers must pass per-request
 * signals (as autocomplete does) — reusing one long-lived signal across many
 * requests would accumulate a listener per request on pre-.any runtimes.
 */
function combineWithTimeout(callerSignal: AbortSignal | undefined): AbortSignal {
  const timeout = makeTimeoutSignal();
  if (!callerSignal) {
    return timeout;
  }
  if (typeof AbortSignal.any === "function") {
    return AbortSignal.any([timeout, callerSignal]);
  }
  const controller = new AbortController();
  for (const signal of [timeout, callerSignal]) {
    if (signal.aborted) {
      controller.abort(signal.reason);
      break;
    }
    signal.addEventListener("abort", () => controller.abort(signal.reason), { once: true });
  }
  return controller.signal;
}

export async function apiRequest<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const method = options.method ?? "GET";
  const headers: Record<string, string> = { ...config.defaultHeaders };
  if (options.body !== undefined) {
    headers["content-type"] = "application/json";
  }
  const authHeader = config.getAuthHeader ? await config.getAuthHeader() : null;
  if (authHeader) {
    headers.authorization = authHeader;
  }
  const response = await fetch(`${config.baseUrl}${path}`, {
    method,
    // "include" behaves identically same-origin but keeps the session cookie
    // flowing if production ever splits onto app./api. subdomains.
    credentials: "include",
    headers: Object.keys(headers).length > 0 ? headers : undefined,
    body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
    // A stalled request must fail instead of pinning queries in pending
    // forever; callers can additionally cancel.
    signal: combineWithTimeout(options.signal),
  });

  if (!response.ok) {
    const retryAfterHeader = response.headers.get("retry-after");
    const retryAfterSeconds = retryAfterHeader ? Number.parseInt(retryAfterHeader, 10) || null : null;
    let code = "unknown_error";
    let message = `Request failed with status ${response.status}`;
    let requestId: string | null = null;
    try {
      const parsed = (await response.json()) as {
        error?: { code?: string; message?: string; request_id?: string };
      };
      if (parsed.error?.code) {
        code = parsed.error.code;
      }
      if (parsed.error?.message) {
        message = parsed.error.message;
      }
      if (parsed.error?.request_id) {
        requestId = parsed.error.request_id;
      }
    } catch {
      // Non-JSON error body; keep the generic message.
    }
    throw new ApiError(response.status, code, message, retryAfterSeconds, requestId);
  }

  return (await response.json()) as T;
}
