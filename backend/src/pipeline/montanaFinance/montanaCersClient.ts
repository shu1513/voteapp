// Montana CERS portal client (docs/plans/montana-finance.md, Phase 1).
//
// CERS (cers-ext.mt.gov/CampaignTracker) is an old Spring MVC + DataTables
// app: server-rendered, session-scoped, no auth, no CSRF, no WAF (verified
// live 2026-08-26 and re-verified 2026-08-27). This client is the module's
// single fetch layer. Verified gotchas it enforces (backend/docs/
// montana-campaign-finance.md — do not rediscover):
// - SESSION SCOPE: search state lives server-side per JSESSIONID, and stale
//   state silently overrides later retrieve* POSTs with the PREVIOUS
//   entity's data. Callers must create a FRESH session per entity.
// - `iSortCol_0`/`sSortDir_0` are REQUIRED on every DataTables list GET —
//   omitting them throws a server IllegalStateException (HTML error page).
// - Silent validation bounce: a rejected searchFinancials POST answers 200
//   with the SEARCH page again — the only tell is the page `<title>` saying
//   `(search)` instead of `(searchResults)`. assertMontanaCersPageTitle
//   fails closed on it.
// - retrieve* POSTs answer 302; the redirect target is then fetched with
//   the same session. Redirects are never followed automatically.
// - Form actions resolve relative to `/CampaignTracker/public/`, so URLs
//   are always built absolute here via buildMontanaCersUrl.
// - PII: contribution artifacts carry donor street addresses. This layer
//   returns raw bytes; callers keep artifacts in restricted storage and
//   never log address fields.

export const MONTANA_CERS_BASE_URL = "https://cers-ext.mt.gov";
export const MONTANA_CERS_PUBLIC_PATH_PREFIX = "/CampaignTracker/public/";
/** Stable public entry point; deep links are POST/session driven. */
export const MONTANA_CERS_DASHBOARD_URL = "https://cers-ext.mt.gov/CampaignTracker/dashboard";

export const DEFAULT_MONTANA_CERS_USER_AGENT =
  "VoteApp election research (https://electionssimplified.com)";
export const DEFAULT_MONTANA_CERS_REQUEST_SPACING_MS = 1_000;
export const DEFAULT_MONTANA_CERS_REQUEST_TIMEOUT_MS = 120_000;
// The largest observed body is a full-cycle CSV export (tens of KB) and the
// 1,090-row registration list (~3 MB). Anything past this is drift.
export const MONTANA_CERS_MAX_RESPONSE_BYTES = 64 * 1024 * 1024;

/**
 * Query string every DataTables list GET must carry. `iSortCol_0` is
 * load-bearing (server 500s without it); the display length is set high
 * enough that the fetches this module performs are single-page.
 */
export function buildMontanaCersDataTablesQuery(displayLength = 1_000): Record<string, string> {
  return {
    sEcho: "1",
    iColumns: "9",
    iDisplayStart: "0",
    iDisplayLength: String(displayLength),
    iSortCol_0: "1",
    sSortDir_0: "asc",
  };
}

export type MontanaCersClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response"
  | "validation_bounce";

export class MontanaCersClientError extends Error {
  constructor(
    public readonly code: MontanaCersClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "MontanaCersClientError";
  }
}

/**
 * CERS page titles carry a page marker in parentheses:
 * `Campaign Electronic Reporting System (searchResults)`. A successful
 * search must land on the expected marker — a silent validation bounce
 * re-renders the search form (marker `(search)`) with HTTP 200.
 */
export function extractMontanaCersPageTitleMarker(html: string): string | null {
  const match = /<title>[^<]*\(([^)<]+)\)\s*<\/title>/i.exec(html);
  return match === null ? null : match[1]!.trim();
}

export function assertMontanaCersPageTitle(html: string, expectedMarker: string, context: string): void {
  const marker = extractMontanaCersPageTitleMarker(html);
  if (marker !== expectedMarker) {
    throw new MontanaCersClientError(
      "validation_bounce",
      `CERS ${context} landed on page marker ${JSON.stringify(marker)}, expected (${expectedMarker}) — ` +
        "the portal silently bounced the request (bad field combination?)"
    );
  }
}

export function buildMontanaCersUrl(path: string, query?: Record<string, string>): string {
  const url = new URL(`${MONTANA_CERS_PUBLIC_PATH_PREFIX}${path}`, MONTANA_CERS_BASE_URL);
  for (const [key, value] of Object.entries(query ?? {})) {
    url.searchParams.set(key, value);
  }
  return url.toString();
}

export type MontanaCersResponse = {
  status: number;
  contentType: string | null;
  /** Location header when the server answered with a redirect; never followed. */
  redirectLocation: string | null;
  body: Buffer;
  text: () => string;
};

export type MontanaCersFetchFn = (
  url: string,
  init: {
    method: "GET" | "POST";
    headers: Record<string, string>;
    body?: string;
    redirect: "manual";
    signal: AbortSignal;
  }
) => Promise<Response>;

export type MontanaCersSessionOptions = {
  fetchImpl?: MontanaCersFetchFn;
  sleep?: (ms: number) => Promise<void>;
  spacingMs?: number;
  maxAttempts?: number;
  retryBackoffMs?: number;
  userAgent?: string;
  timeoutMs?: number;
  maxResponseBytes?: number;
  log?: (message: string) => void;
};

export type MontanaCersSession = {
  get: (url: string, options?: { referer?: string }) => Promise<MontanaCersResponse>;
  postForm: (
    url: string,
    fields: Record<string, string>,
    options?: { referer?: string }
  ) => Promise<MontanaCersResponse>;
};

/** Cookie jar: single host, session lifetime — name/value only, attributes ignored. */
function parseSetCookieHeader(header: string): { name: string; value: string } | null {
  const pair = header.split(";", 1)[0] ?? "";
  const separator = pair.indexOf("=");
  if (separator <= 0) {
    return null;
  }
  return { name: pair.slice(0, separator).trim(), value: pair.slice(separator + 1).trim() };
}

/**
 * A session owns a cookie jar (JSESSIONID) and a single-flight request
 * queue with courteous spacing. Create one session per entity harvest —
 * CERS keeps search state server-side per session, and reusing a session
 * across entities silently serves the previous entity's data.
 */
export function createMontanaCersSession(options: MontanaCersSessionOptions = {}): MontanaCersSession {
  const spacingMs = options.spacingMs ?? DEFAULT_MONTANA_CERS_REQUEST_SPACING_MS;
  const maxAttempts = options.maxAttempts ?? 3;
  const retryBackoffMs = options.retryBackoffMs ?? 5_000;
  const timeoutMs = options.timeoutMs ?? DEFAULT_MONTANA_CERS_REQUEST_TIMEOUT_MS;
  const maxResponseBytes = options.maxResponseBytes ?? MONTANA_CERS_MAX_RESPONSE_BYTES;
  const userAgent = options.userAgent ?? DEFAULT_MONTANA_CERS_USER_AGENT;
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const fetchImpl: MontanaCersFetchFn = options.fetchImpl ?? ((url, init) => fetch(url, init));

  const cookies = new Map<string, string>();
  let queue: Promise<unknown> = Promise.resolve();
  let anyRequestStarted = false;

  function requireCersUrl(url: string): void {
    if (!url.startsWith(`${MONTANA_CERS_BASE_URL}/`)) {
      throw new MontanaCersClientError("invalid_request", `not a cers-ext.mt.gov URL: ${url}`);
    }
  }

  async function performOnce(
    url: string,
    method: "GET" | "POST",
    body: string | undefined,
    referer: string | undefined
  ): Promise<MontanaCersResponse> {
    const headers: Record<string, string> = { "User-Agent": userAgent };
    if (referer !== undefined) {
      headers.Referer = referer;
    }
    if (cookies.size > 0) {
      headers.Cookie = [...cookies.entries()].map(([name, value]) => `${name}=${value}`).join("; ");
    }
    if (method === "POST") {
      headers["Content-Type"] = "application/x-www-form-urlencoded";
    }

    let response: Response;
    try {
      response = await fetchImpl(url, {
        method,
        headers,
        ...(body === undefined ? {} : { body }),
        redirect: "manual",
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new MontanaCersClientError("network_error", `CERS request failed: ${method} ${url} — ${message}`);
    }

    for (const header of response.headers.getSetCookie()) {
      const cookie = parseSetCookieHeader(header);
      if (cookie !== null) {
        cookies.set(cookie.name, cookie.value);
      }
    }

    const declaredLength = Number.parseInt(response.headers.get("content-length") ?? "", 10);
    if (Number.isSafeInteger(declaredLength) && declaredLength > maxResponseBytes) {
      throw new MontanaCersClientError(
        "bad_response",
        `CERS response declares ${declaredLength} bytes, over the ${maxResponseBytes} limit: ${url}`
      );
    }

    let bytes: ArrayBuffer;
    try {
      bytes = await response.arrayBuffer();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new MontanaCersClientError("network_error", `CERS response body read failed: ${url} — ${message}`);
    }
    if (bytes.byteLength > maxResponseBytes) {
      throw new MontanaCersClientError(
        "bad_response",
        `CERS response exceeds ${maxResponseBytes} bytes (${bytes.byteLength}): ${url}`
      );
    }

    const buffer = Buffer.from(bytes);
    const result: MontanaCersResponse = {
      status: response.status,
      contentType: response.headers.get("content-type"),
      redirectLocation: response.headers.get("location"),
      body: buffer,
      text: () => buffer.toString("utf-8"),
    };

    if (response.status >= 400) {
      throw new MontanaCersClientError(
        "http_error",
        `CERS request failed with HTTP ${response.status}: ${url}`,
        response.status
      );
    }
    return result;
  }

  function enqueue(
    url: string,
    method: "GET" | "POST",
    body: string | undefined,
    referer: string | undefined
  ): Promise<MontanaCersResponse> {
    try {
      requireCersUrl(url);
    } catch (error) {
      return Promise.reject(error);
    }
    const run = queue.then(async () => {
      let lastError: Error | null = null;
      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        if (anyRequestStarted) {
          await sleep(attempt === 1 ? spacingMs : attempt * retryBackoffMs);
        }
        anyRequestStarted = true;
        try {
          return await performOnce(url, method, body, referer);
        } catch (error) {
          if (error instanceof MontanaCersClientError) {
            const retryable =
              error.code === "network_error" ||
              (error.code === "http_error" && (error.status === 429 || (error.status ?? 0) >= 500));
            if (!retryable) {
              throw error;
            }
            lastError = error;
          } else {
            lastError = error as Error;
          }
        }
        options.log?.(`CERS attempt ${attempt}/${maxAttempts} failed for ${url}: ${lastError.message}`);
      }
      throw new MontanaCersClientError(
        "network_error",
        `CERS request failed after ${maxAttempts} attempts: ${url} — ${lastError?.message}`
      );
    });
    // Later requests wait for this one to settle, success or failure.
    queue = run.catch(() => {});
    return run;
  }

  return {
    get: (url, getOptions) => enqueue(url, "GET", undefined, getOptions?.referer),
    postForm: (url, fields, postOptions) => {
      const params = new URLSearchParams();
      for (const [name, value] of Object.entries(fields)) {
        params.append(name, value);
      }
      return enqueue(url, "POST", params.toString(), postOptions?.referer);
    },
  };
}
