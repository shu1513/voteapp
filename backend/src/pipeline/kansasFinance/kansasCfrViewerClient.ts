// Kansas SOS CFR viewer client (plan-kansas-finance.md, Phase 0+).
//
// The viewer (sos.ks.gov/elections/cfr_viewer/) is an ASP.NET WebForms app.
// Everything below was verified live 2026-08-26:
// - The server 403s requests whose User-Agent does not start with
//   "Mozilla/5.0" (plain curl and a bare product-token UA are both refused),
//   so the default UA is an honest browser-compatible token.
// - Navigation is POST -> 302 -> GET: category select, searches, and grid-row
//   postbacks all answer 302 and the target page must be fetched with the
//   same session cookies. Schedules A-D of the currently-open report are
//   plain GETs (reports/schedule_a_report.aspx etc.).
// - Every POST must echo ALL hidden inputs of the page it came from
//   (__VIEWSTATE, __EVENTVALIDATION, __VIEWSTATEGENERATOR, and — on grid
//   pages — __VIEWSTATEENCRYPTED and __SCROLLPOSITIONX/Y; omitting
//   __VIEWSTATEENCRYPTED produced a 500 live). Callers therefore hand this
//   client the previous page's parsed hidden fields verbatim.
// - Report identity lives in server session state; no URL carries a report
//   id. One walk per session, one request in flight per session.
// - A 500 "Runtime Error" page means a bad postback (wrong field or stale
//   state), not a transient fault: it is never retried.

import { parseKansasHiddenFields } from "./kansasCfrViewerParsers.js";

export const KANSAS_CFR_VIEWER_BASE_URL = "https://sos.ks.gov";
export const KANSAS_CFR_VIEWER_PATH_PREFIX = "/elections/cfr_viewer/";

export const KANSAS_CFR_VIEWER_PAGES = {
  entry: "cfr_examiner_entry.aspx",
  /** Candidate-filings / individual-entity search form (category-dependent). */
  examinerForm: "cfr_examiner.aspx",
  contributionForm: "cfr_examiner_contribution.aspx",
  contributionResults: "cfr_examiner_contribution_results.aspx",
  expenditureResults: "cfr_examiner_expenditure_results.aspx",
  searchResults: "cfr_examiner_search_results.aspx",
  lookupResults: "cfr_examiner_lookup_results.aspx",
  reportCover: "reports/exp_report_main.aspx",
  scheduleA: "reports/schedule_a_report.aspx",
  scheduleB: "reports/schedule_b_report.aspx",
  scheduleC: "reports/schedule_c_report.aspx",
  scheduleD: "reports/schedule_d_report.aspx",
} as const;

export type KansasCfrViewerCategory =
  | "Contribution"
  | "Expenditure"
  | "Candidate"
  | "PAC"
  | "GubernatorialInaguration" // sic — the server's own option value
  | "IndividualEntity";

/** drpdownOffice option values, captured live 2026-08-26. */
export const KANSAS_CFR_OFFICE_CODES = {
  governor: "1",
  secretaryOfState: "2",
  attorneyGeneral: "3",
  insuranceCommissioner: "4",
  stateTreasurer: "5",
  stateSenator: "6",
  stateRepresentative: "7",
  stateBoardOfEducation: "8",
  districtAttorney: "9",
} as const;

export const DEFAULT_KANSAS_CFR_USER_AGENT =
  "Mozilla/5.0 (compatible; VoteApp election research; +https://electionssimplified.com)";
export const DEFAULT_KANSAS_CFR_REQUEST_SPACING_MS = 1_500;
export const DEFAULT_KANSAS_CFR_REQUEST_TIMEOUT_MS = 120_000;
// The largest observed export (4,285 rows) was 6.9 MB; the cap-probe export
// may be far larger, but anything past this is drift, not data.
export const KANSAS_CFR_MAX_RESPONSE_BYTES = 256 * 1024 * 1024;

export type KansasCfrClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response";

export class KansasCfrClientError extends Error {
  constructor(
    public readonly code: KansasCfrClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "KansasCfrClientError";
  }
}

export type KansasCfrResponse = {
  status: number;
  contentType: string | null;
  contentDisposition: string | null;
  /** Location header when the server redirected; never followed implicitly. */
  redirectLocation: string | null;
  body: Buffer;
  text: () => string;
};

export type KansasCfrFetchFn = (
  url: string,
  init: {
    method: "GET" | "POST";
    headers: Record<string, string>;
    body?: string;
    redirect: "manual";
    signal: AbortSignal;
  }
) => Promise<Response>;

export type KansasCfrSessionOptions = {
  fetchImpl?: KansasCfrFetchFn;
  sleep?: (ms: number) => Promise<void>;
  spacingMs?: number;
  maxAttempts?: number;
  retryBackoffMs?: number;
  userAgent?: string;
  timeoutMs?: number;
  maxResponseBytes?: number;
};

export type KansasCfrSession = {
  get: (url: string, options?: { referer?: string }) => Promise<KansasCfrResponse>;
  postForm: (
    url: string,
    fields: Record<string, string>,
    options?: { referer?: string }
  ) => Promise<KansasCfrResponse>;
};

export function buildKansasCfrUrl(page: string): string {
  return new URL(`${KANSAS_CFR_VIEWER_PATH_PREFIX}${page}`, KANSAS_CFR_VIEWER_BASE_URL).toString();
}

/** Cookie jar: single host, session lifetime — name/value only. */
function parseSetCookieHeader(header: string): { name: string; value: string } | null {
  const pair = header.split(";", 1)[0] ?? "";
  const separator = pair.indexOf("=");
  if (separator <= 0) return null;
  return { name: pair.slice(0, separator).trim(), value: pair.slice(separator + 1).trim() };
}

// Retries deliberately cover POSTs too: every POST here (category select,
// search, grid-row postback, export) is a read-only, deterministic navigation
// — a replay re-executes the same request against an app with no writes, and
// the resubmitted hidden fields are byte-identical to attempt 1 (no page was
// fetched in between), so __EVENTVALIDATION still matches its rendering page.
// The non-retryable 500 "Runtime Error" arises from WRONG postback state, not
// from replaying identical state; if a replay ever did 500, it surfaces as a
// visible http_error rather than silent bad data.
const RETRYABLE_STATUSES = new Set([429, 502, 503, 504]);

export function createKansasCfrSession(options: KansasCfrSessionOptions = {}): KansasCfrSession {
  const spacingMs = options.spacingMs ?? DEFAULT_KANSAS_CFR_REQUEST_SPACING_MS;
  const maxAttempts = options.maxAttempts ?? 3;
  const retryBackoffMs = options.retryBackoffMs ?? 5_000;
  const timeoutMs = options.timeoutMs ?? DEFAULT_KANSAS_CFR_REQUEST_TIMEOUT_MS;
  const maxResponseBytes = options.maxResponseBytes ?? KANSAS_CFR_MAX_RESPONSE_BYTES;
  const userAgent = options.userAgent ?? DEFAULT_KANSAS_CFR_USER_AGENT;
  const sleep =
    options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const fetchImpl: KansasCfrFetchFn = options.fetchImpl ?? ((url, init) => fetch(url, init));

  const cookies = new Map<string, string>();
  let queue: Promise<unknown> = Promise.resolve();
  let anyRequestStarted = false;

  function requireViewerUrl(url: string): void {
    if (!url.startsWith(`${KANSAS_CFR_VIEWER_BASE_URL}/`)) {
      throw new KansasCfrClientError("invalid_request", `not a sos.ks.gov URL: ${url}`);
    }
  }

  async function performOnce(
    url: string,
    method: "GET" | "POST",
    body: string | undefined,
    referer: string | undefined
  ): Promise<KansasCfrResponse> {
    const headers: Record<string, string> = { "User-Agent": userAgent };
    if (referer !== undefined) headers.Referer = referer;
    if (cookies.size > 0) {
      headers.Cookie = [...cookies.entries()].map(([name, value]) => `${name}=${value}`).join("; ");
    }
    if (method === "POST") headers["Content-Type"] = "application/x-www-form-urlencoded";

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    let response: Response;
    let bytes: Buffer;
    // The timer must stay armed through body consumption: headers can arrive
    // and the body then stall, which would otherwise leave session.get()
    // pending forever and block the serialized session queue.
    try {
      try {
        response = await fetchImpl(url, {
          method,
          headers,
          body,
          redirect: "manual",
          signal: controller.signal,
        });
      } catch (error) {
        throw new KansasCfrClientError(
          "network_error",
          `${method} ${url} failed: ${error instanceof Error ? error.message : String(error)}`
        );
      }
      const declaredLength = Number(response.headers.get("content-length") ?? Number.NaN);
      if (Number.isFinite(declaredLength) && declaredLength > maxResponseBytes) {
        throw new KansasCfrClientError(
          "bad_response",
          `${method} ${url} declared ${declaredLength} bytes (limit ${maxResponseBytes})`
        );
      }
      try {
        bytes = Buffer.from(await response.arrayBuffer());
      } catch (error) {
        throw new KansasCfrClientError(
          "network_error",
          `${method} ${url} body read failed: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    } finally {
      clearTimeout(timer);
    }

    for (const header of response.headers.getSetCookie?.() ?? []) {
      const cookie = parseSetCookieHeader(header);
      if (cookie) cookies.set(cookie.name, cookie.value);
    }

    if (bytes.byteLength > maxResponseBytes) {
      throw new KansasCfrClientError(
        "bad_response",
        `${method} ${url} answered ${bytes.byteLength} bytes (limit ${maxResponseBytes})`
      );
    }
    return {
      status: response.status,
      contentType: response.headers.get("content-type"),
      contentDisposition: response.headers.get("content-disposition"),
      redirectLocation: response.headers.get("location"),
      body: bytes,
      text: () => bytes.toString("utf8"),
    };
  }

  async function perform(
    url: string,
    method: "GET" | "POST",
    body: string | undefined,
    referer: string | undefined
  ): Promise<KansasCfrResponse> {
    requireViewerUrl(url);
    const run = queue.then(async () => {
      if (anyRequestStarted && spacingMs > 0) await sleep(spacingMs);
      anyRequestStarted = true;
      let lastError: unknown;
      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        try {
          const response = await performOnce(url, method, body, referer);
          if (RETRYABLE_STATUSES.has(response.status)) {
            lastError = new KansasCfrClientError(
              "http_error",
              `${method} ${url} answered ${response.status}`,
              response.status
            );
          } else {
            return response;
          }
        } catch (error) {
          if (error instanceof KansasCfrClientError && error.code !== "network_error") throw error;
          lastError = error;
        }
        if (attempt < maxAttempts) await sleep(retryBackoffMs * attempt);
      }
      throw lastError instanceof Error
        ? lastError
        : new KansasCfrClientError("network_error", `${method} ${url} failed`);
    });
    queue = run.catch(() => undefined);
    return run;
  }

  return {
    get: (url, getOptions) => perform(url, "GET", undefined, getOptions?.referer),
    postForm: (url, fields, postOptions) => {
      const body = new URLSearchParams(fields).toString();
      return perform(url, "POST", body, postOptions?.referer);
    },
  };
}

// ---------------------------------------------------------------------------
// Flow helpers. Each returns the landed page's HTML plus its hidden fields so
// the caller can chain the next postback without re-parsing.

export type KansasCfrPage = {
  url: string;
  html: string;
  hiddenFields: Record<string, string>;
};

function requireHtmlPage(response: KansasCfrResponse, url: string): void {
  if (response.status !== 200) {
    throw new KansasCfrClientError("http_error", `GET ${url} answered ${response.status}`, response.status);
  }
}

async function getPage(session: KansasCfrSession, page: string, referer?: string): Promise<KansasCfrPage> {
  const url = buildKansasCfrUrl(page);
  const response = await session.get(url, { referer });
  requireHtmlPage(response, url);
  const html = response.text();
  return { url, html, hiddenFields: parseKansasHiddenFields(html) };
}

/**
 * POST a WebForms form and land on the page the server redirects to. The
 * viewer answers navigation POSTs with 302 (verified live for the category
 * select, both searches, and grid-row postbacks); a 200 answer is returned
 * as the landed page itself (the export POST responds 200 with the file).
 */
export async function postAndFollow(
  session: KansasCfrSession,
  fromPage: KansasCfrPage,
  fields: Record<string, string>
): Promise<KansasCfrPage> {
  const response = await session.postForm(fromPage.url, { ...fromPage.hiddenFields, ...fields }, { referer: fromPage.url });
  if (response.status === 302 && response.redirectLocation) {
    const target = new URL(response.redirectLocation, fromPage.url).toString();
    if (!target.startsWith(`${KANSAS_CFR_VIEWER_BASE_URL}/`)) {
      throw new KansasCfrClientError("bad_response", `unexpected redirect to ${target}`);
    }
    const landed = await session.get(target, { referer: fromPage.url });
    requireHtmlPage(landed, target);
    const html = landed.text();
    return { url: target, html, hiddenFields: parseKansasHiddenFields(html) };
  }
  if (response.status === 200) {
    const html = response.text();
    return { url: fromPage.url, html, hiddenFields: parseKansasHiddenFields(html) };
  }
  throw new KansasCfrClientError(
    "http_error",
    `POST ${fromPage.url} answered ${response.status}`,
    response.status
  );
}

/** GET the entry page, select a category, and land on that category's form. */
export async function openKansasCfrCategory(
  session: KansasCfrSession,
  category: KansasCfrViewerCategory
): Promise<KansasCfrPage> {
  const entry = await getPage(session, KANSAS_CFR_VIEWER_PAGES.entry);
  return postAndFollow(session, entry, {
    ddlViewerOptions: category,
    btnSubmit: "Submit",
  });
}

/** Fire a __doPostBack link (grid row, schedule link) and land on the target. */
export async function postbackAndFollow(
  session: KansasCfrSession,
  fromPage: KansasCfrPage,
  eventTarget: string
): Promise<KansasCfrPage> {
  return postAndFollow(session, fromPage, {
    __EVENTTARGET: eventTarget,
    __EVENTARGUMENT: "",
  });
}

/** GET a schedule of the report currently open in this session. */
export async function getKansasReportSchedule(
  session: KansasCfrSession,
  schedule: "A" | "B" | "C" | "D"
): Promise<KansasCfrPage> {
  const page = {
    A: KANSAS_CFR_VIEWER_PAGES.scheduleA,
    B: KANSAS_CFR_VIEWER_PAGES.scheduleB,
    C: KANSAS_CFR_VIEWER_PAGES.scheduleC,
    D: KANSAS_CFR_VIEWER_PAGES.scheduleD,
  }[schedule];
  return getPage(session, page, buildKansasCfrUrl(KANSAS_CFR_VIEWER_PAGES.reportCover));
}

/**
 * POST the export button on a search-results page. Answers 200 with an
 * HTML-table body served as an attachment (Contributions.xls, verified
 * complete at 4,285 rows / 6.9 MB live).
 */
export async function exportKansasSearchResults(
  session: KansasCfrSession,
  resultsPage: KansasCfrPage
): Promise<{ contentDisposition: string | null; body: Buffer }> {
  const response = await session.postForm(
    resultsPage.url,
    {
      ...resultsPage.hiddenFields,
      btnExport: "CLICK HERE TO EXPORT THE RESULTS OF YOUR SEARCH",
    },
    { referer: resultsPage.url }
  );
  if (response.status !== 200) {
    throw new KansasCfrClientError("http_error", `export answered ${response.status}`, response.status);
  }
  if (!response.contentDisposition?.includes("attachment")) {
    throw new KansasCfrClientError("bad_response", "export did not answer an attachment");
  }
  return { contentDisposition: response.contentDisposition, body: response.body };
}
