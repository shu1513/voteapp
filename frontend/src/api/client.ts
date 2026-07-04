// Thin fetch wrapper for the VoteApp API. Same-origin (Vite dev proxy /
// same-site deploy), JSON in/out, parses the standard error envelope
// { error: { code, message } } into a typed ApiError.

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  /** Seconds from the retry-after header on 429 responses. */
  readonly retryAfterSeconds: number | null;

  constructor(status: number, code: string, message: string, retryAfterSeconds: number | null = null) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

type RequestOptions = {
  method?: "GET" | "POST" | "PUT" | "DELETE";
  body?: unknown;
  /** Caller-side cancellation (e.g. superseded autocomplete requests). */
  signal?: AbortSignal;
};

export const REQUEST_TIMEOUT_MS = 15_000;

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
  const response = await fetch(path, {
    method,
    // "include" behaves identically same-origin but keeps the session cookie
    // flowing if production ever splits onto app./api. subdomains.
    credentials: "include",
    headers: options.body !== undefined ? { "content-type": "application/json" } : undefined,
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
    try {
      const parsed = (await response.json()) as { error?: { code?: string; message?: string } };
      if (parsed.error?.code) {
        code = parsed.error.code;
      }
      if (parsed.error?.message) {
        message = parsed.error.message;
      }
    } catch {
      // Non-JSON error body; keep the generic message.
    }
    throw new ApiError(response.status, code, message, retryAfterSeconds);
  }

  return (await response.json()) as T;
}
