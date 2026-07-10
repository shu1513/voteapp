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
};

const config: ApiClientConfig = { baseUrl: "", getAuthHeader: null };

/** Platform setup, called once at app start; web apps need no call at all. */
export function configureApi(overrides: Partial<ApiClientConfig>): void {
  Object.assign(config, overrides);
}

/**
 * Combines the request timeout with an optional caller signal without
 * requiring AbortSignal.any (Chrome 116+/Safari 17.4+): on older browsers a
 * missing .any must degrade to a manual combine, not throw before fetch and
 * silently kill features like autocomplete.
 */
function combineWithTimeout(callerSignal: AbortSignal | undefined): AbortSignal | undefined {
  const timeout = typeof AbortSignal.timeout === "function" ? AbortSignal.timeout(REQUEST_TIMEOUT_MS) : undefined;
  if (!callerSignal) {
    return timeout;
  }
  if (!timeout) {
    return callerSignal;
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
  const headers: Record<string, string> = {};
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
