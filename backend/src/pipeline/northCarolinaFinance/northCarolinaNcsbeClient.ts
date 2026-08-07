import {
  parseNcsbeCommitteeSearchPage,
  parseNcsbeDocumentListPage,
  parseNcsbeExpendituresPage,
  parseNcsbeReceiptsPage,
  parseNcsbeReportDetailPage,
  type NcsbeCommitteeSearchRow,
  type NcsbeDocumentRow,
  type NcsbeExpenditureRow,
  type NcsbeReceiptRow,
  type NcsbeReportDetail,
} from "./northCarolinaNcsbeParsers.js";

// HTTP client for the NCSBE portal. Every route lives here and nowhere else
// (north_carolina_plan.md decision 10 — the state is actively replacing this
// early-2000s ASP.NET app, so route changes must be one-module migrations).
// Transport is fail-closed per decision 9: bodies are validated by shape
// (the server labels real JSON `text/html`), transaction fetches page until
// the row count equals `recordCountKey`, and any mismatch fails the report.
//
// Retrieval is polite by construction (decision 10): one request in flight,
// a fixed delay before every request after the first, bounded retries with
// backoff, and a descriptive user agent. ~60 spike requests at 2 s pacing
// drew zero blocks or 429s; full-cycle scale is unproven, so the pacing is
// not negotiable.

export const NCSBE_PORTAL_BASE_URL = "https://cf.ncsbe.gov";

export const DEFAULT_NCSBE_USER_AGENT = "VoteApp election research (https://electionssimplified.com)";

// Delay before each request after the first. The spike ran at 2 s.
export const DEFAULT_NCSBE_REQUEST_SPACING_MS = 2_000;

export const DEFAULT_NCSBE_REQUEST_TIMEOUT_MS = 90_000;

// The server ignores pageSize and always serves 300-row pages (spike results
// item 1). The parameter is still required — a bare call returns an HTML
// error page — so it is sent with the observed fixed size.
export const NCSBE_TRANSACTION_PAGE_SIZE = 300;

// The three IE doc-type codes (decision 3): unregistered IE reports,
// registered-committee IE informational reports, and electioneering reports.
export const NCSBE_IE_DOC_TYPE_CODES = ["IRIEX", "IRCIX", "RPIER"] as const;

function requireNcsbeReportId(reportId: string): string {
  const trimmed = reportId.trim();
  if (!/^\d+$/.test(trimmed)) {
    throw new Error(`Invalid NCSBE report id: ${JSON.stringify(reportId)}`);
  }
  return trimmed;
}

function requireNcsbeOrgGroupId(orgGroupId: number): number {
  if (!Number.isInteger(orgGroupId) || orgGroupId <= 0) {
    throw new Error(`Invalid NCSBE OrgGroupID: ${orgGroupId}`);
  }
  return orgGroupId;
}

export function requireNcsbeYear(year: number): number {
  if (!Number.isInteger(year) || year < 1990 || year > 2100) {
    throw new Error(`Invalid NCSBE year: ${year}`);
  }
  return year;
}

export function ncsbeCommitteeSearchUrl(name: string): string {
  const trimmed = name.trim();
  if (trimmed.length === 0) {
    throw new Error("NCSBE committee search needs a non-empty name");
  }
  return (
    `${NCSBE_PORTAL_BASE_URL}/CFOrgLkup/CommitteeGeneralResult/?name=${encodeURIComponent(trimmed)}` +
    "&useOrgName=True&useCandName=True&useInHouseName=True&useAcronym=False"
  );
}

export function ncsbeDocumentInventoryUrl(input: { orgGroupId: number; sboeId: string }): string {
  const sboeId = input.sboeId.trim();
  if (sboeId.length === 0) {
    throw new Error("NCSBE document inventory needs a non-empty SBoEID");
  }
  return (
    `${NCSBE_PORTAL_BASE_URL}/CFOrgLkup/DocumentGeneralResult/` +
    `?OGID=${requireNcsbeOrgGroupId(input.orgGroupId)}&SID=${encodeURIComponent(sboeId)}`
  );
}

export function ncsbeReportDetailUrl(reportId: string): string {
  return `${NCSBE_PORTAL_BASE_URL}/CFOrgLkup/ReportDetail/?RID=${requireNcsbeReportId(reportId)}&TP=ALL`;
}

export function ncsbeReceiptsUrl(reportId: string, page: number): string {
  if (!Number.isInteger(page) || page < 0) {
    throw new Error(`Invalid NCSBE receipts page: ${page}`);
  }
  return (
    `${NCSBE_PORTAL_BASE_URL}/CFOrgLkup/GetReceipts?ReportID=${requireNcsbeReportId(reportId)}` +
    `&page=${page}&pageSize=${NCSBE_TRANSACTION_PAGE_SIZE}`
  );
}

export function ncsbeExpendituresUrl(reportId: string, page: number): string {
  if (!Number.isInteger(page) || page < 0) {
    throw new Error(`Invalid NCSBE expenditures page: ${page}`);
  }
  return (
    `${NCSBE_PORTAL_BASE_URL}/CFOrgLkup/GetExpenditures?ReportID=${requireNcsbeReportId(reportId)}` +
    `&ShowIEColumns=true&page=${page}&pageSize=${NCSBE_TRANSACTION_PAGE_SIZE}`
  );
}

export function ncsbeCsvExportUrl(reportId: string, title: string): string {
  return (
    `${NCSBE_PORTAL_BASE_URL}/CFOrgLkup/ExportDetailResults/?ReportID=${requireNcsbeReportId(reportId)}` +
    `&Type=ALL&Title=${encodeURIComponent(title)}`
  );
}

// The doc-type codes must be single-quoted or the portal serves an error page
// (decision 9). encodeURIComponent would keep the quotes literal, which the
// portal also rejects — the %27 encoding is load-bearing and pinned here.
export function ncsbeIeDocTypeInventoryUrl(year: number): string {
  const reports = NCSBE_IE_DOC_TYPE_CODES.map((code) => `%27${code}%27`).join(",");
  return `${NCSBE_PORTAL_BASE_URL}/CFDocLkup/DocumentResult/?year=${requireNcsbeYear(year)}&reports=${reports}`;
}

export type NcsbeHttpResponse = {
  status: number;
  body: string;
};

export type NcsbeFetchFn = (url: string) => Promise<NcsbeHttpResponse>;

export type NcsbeTransport = {
  fetchText: (url: string) => Promise<string>;
};

function defaultNcsbeFetch(userAgent: string, timeoutMs: number): NcsbeFetchFn {
  return async (url: string) => {
    const response = await fetch(url, {
      headers: { "User-Agent": userAgent, Accept: "*/*" },
      redirect: "follow",
      signal: AbortSignal.timeout(timeoutMs),
    });
    return { status: response.status, body: await response.text() };
  };
}

// One request in flight, spacing before every request after the first,
// bounded retries with linear backoff. Retries cover transient failures only
// (network errors, 429, 5xx); other statuses fail immediately — a 404 will
// not become a 200 by asking again.
export function createNcsbeTransport(
  options: {
    fetch?: NcsbeFetchFn;
    sleep?: (ms: number) => Promise<void>;
    spacingMs?: number;
    maxAttempts?: number;
    retryBackoffMs?: number;
    userAgent?: string;
    timeoutMs?: number;
    log?: (message: string) => void;
  } = {}
): NcsbeTransport {
  const spacingMs = options.spacingMs ?? DEFAULT_NCSBE_REQUEST_SPACING_MS;
  const maxAttempts = options.maxAttempts ?? 3;
  const retryBackoffMs = options.retryBackoffMs ?? 5_000;
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const fetchFn =
    options.fetch ??
    defaultNcsbeFetch(
      options.userAgent ?? DEFAULT_NCSBE_USER_AGENT,
      options.timeoutMs ?? DEFAULT_NCSBE_REQUEST_TIMEOUT_MS
    );

  let queue: Promise<unknown> = Promise.resolve();
  let anyRequestStarted = false;

  return {
    fetchText: (url: string) => {
      const run = queue.then(async () => {
        let lastError: Error | null = null;
        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
          if (anyRequestStarted) {
            await sleep(attempt === 1 ? spacingMs : attempt * retryBackoffMs);
          }
          anyRequestStarted = true;
          try {
            const response = await fetchFn(url);
            if (response.status === 200) {
              return response.body;
            }
            const failure = new Error(`NCSBE request failed with HTTP ${response.status}: ${url}`);
            if (response.status !== 429 && response.status < 500) {
              throw failure;
            }
            lastError = failure;
          } catch (error) {
            if ((error as Error).message.startsWith("NCSBE request failed with HTTP ")) {
              throw error;
            }
            lastError = error as Error;
          }
          options.log?.(`NCSBE attempt ${attempt}/${maxAttempts} failed for ${url}: ${lastError.message}`);
        }
        throw new Error(`NCSBE request failed after ${maxAttempts} attempts: ${url} — ${lastError?.message}`);
      });
      // Later requests wait for this one to settle, success or failure.
      queue = run.catch(() => {});
      return run;
    },
  };
}

// --- High-level fetches (raw body + parsed rows) ----------------------------
// Each returns the raw body alongside the parsed result: the acquisition
// stores exactly the bytes that were fetched and validated, and the sync
// re-parses from the cache (decision 10 — retrieval stays separate from
// parsing).

export type NcsbeFetchedPage<Parsed> = {
  url: string;
  body: string;
  parsed: Parsed;
};

export async function fetchNcsbeCommitteeSearch(
  transport: NcsbeTransport,
  name: string
): Promise<NcsbeFetchedPage<NcsbeCommitteeSearchRow[]>> {
  const url = ncsbeCommitteeSearchUrl(name);
  const body = await transport.fetchText(url);
  return { url, body, parsed: parseNcsbeCommitteeSearchPage(body) };
}

export async function fetchNcsbeDocumentInventory(
  transport: NcsbeTransport,
  input: { orgGroupId: number; sboeId: string }
): Promise<NcsbeFetchedPage<NcsbeDocumentRow[]>> {
  const url = ncsbeDocumentInventoryUrl(input);
  const body = await transport.fetchText(url);
  return { url, body, parsed: parseNcsbeDocumentListPage(body) };
}

export async function fetchNcsbeIeDocTypeInventory(
  transport: NcsbeTransport,
  year: number
): Promise<NcsbeFetchedPage<NcsbeDocumentRow[]>> {
  const url = ncsbeIeDocTypeInventoryUrl(year);
  const body = await transport.fetchText(url);
  return { url, body, parsed: parseNcsbeDocumentListPage(body) };
}

export async function fetchNcsbeReportDetail(
  transport: NcsbeTransport,
  reportId: string
): Promise<NcsbeFetchedPage<NcsbeReportDetail>> {
  const url = ncsbeReportDetailUrl(reportId);
  const body = await transport.fetchText(url);
  return { url, body, parsed: parseNcsbeReportDetailPage(body) };
}

export type NcsbeTransactionPageFetch = {
  page: number;
  url: string;
  body: string;
  rowCount: number;
};

export type NcsbeTransactionFetchResult<Row> = {
  reportId: string;
  kind: "receipts" | "expenditures";
  recordCount: number;
  pages: NcsbeTransactionPageFetch[];
  rows: Row[];
};

// Pages 0.. until the fetched row count equals the report's recordCountKey.
// Completeness is the contract (decision 9): an empty page before the count
// is reached, a drifting recordCountKey, or an overshoot all fail the report
// closed rather than caching a partial transaction set.
async function fetchNcsbeTransactionPagesInternal<Row>(
  transport: NcsbeTransport,
  input: { reportId: string; kind: "receipts" | "expenditures" },
  urlFor: (reportId: string, page: number) => string,
  parsePage: (body: string) => { recordCount: number; rows: Row[] }
): Promise<NcsbeTransactionFetchResult<Row>> {
  const reportId = requireNcsbeReportId(input.reportId);
  const pages: NcsbeTransactionPageFetch[] = [];
  const rows: Row[] = [];
  let recordCount: number | null = null;

  for (let page = 0; ; page += 1) {
    const url = urlFor(reportId, page);
    const body = await transport.fetchText(url);
    const parsed = parsePage(body);
    if (recordCount === null) {
      recordCount = parsed.recordCount;
    } else if (parsed.recordCount !== recordCount) {
      throw new Error(
        `NCSBE ${input.kind} report ${reportId}: recordCountKey changed mid-fetch ` +
          `(${recordCount} -> ${parsed.recordCount} on page ${page})`
      );
    }
    if (rows.length < recordCount && parsed.rows.length === 0) {
      throw new Error(
        `NCSBE ${input.kind} report ${reportId}: page ${page} was empty with ` +
          `${rows.length} of ${recordCount} rows fetched`
      );
    }
    pages.push({ page, url, body, rowCount: parsed.rows.length });
    rows.push(...parsed.rows);
    if (rows.length > recordCount) {
      throw new Error(
        `NCSBE ${input.kind} report ${reportId}: fetched ${rows.length} rows, expected ${recordCount}`
      );
    }
    if (rows.length === recordCount) {
      return { reportId, kind: input.kind, recordCount, pages, rows };
    }
  }
}

export function fetchNcsbeReceiptPages(
  transport: NcsbeTransport,
  reportId: string
): Promise<NcsbeTransactionFetchResult<NcsbeReceiptRow>> {
  return fetchNcsbeTransactionPagesInternal(
    transport,
    { reportId, kind: "receipts" },
    ncsbeReceiptsUrl,
    parseNcsbeReceiptsPage
  );
}

export function fetchNcsbeExpenditurePages(
  transport: NcsbeTransport,
  reportId: string
): Promise<NcsbeTransactionFetchResult<NcsbeExpenditureRow>> {
  return fetchNcsbeTransactionPagesInternal(
    transport,
    { reportId, kind: "expenditures" },
    ncsbeExpendituresUrl,
    parseNcsbeExpendituresPage
  );
}
