export const ALASKA_APOC_CAMPAIGN_INCOME_URL =
  "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx";
export const ALASKA_APOC_IE_EXPENDITURES_URL =
  "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEExpenditures.aspx";
export const ALASKA_APOC_IE_CONTRIBUTIONS_URL =
  "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEContributions.aspx";

export const ALASKA_APOC_DEFAULT_TIMEOUT_MS = 30_000;
export const ALASKA_APOC_DEFAULT_RETRY_COUNT = 2;
export const ALASKA_APOC_DEFAULT_RETRY_DELAY_MS = 1_000;
export const ALASKA_APOC_DEFAULT_REQUEST_SPACING_MS = 250;
// The export streams the full report year in one response (~20 MB for income),
// so it needs a far longer ceiling than an ordinary page request.
export const ALASKA_APOC_DEFAULT_EXPORT_TIMEOUT_MS = 600_000;
// aws.state.ak.us sits behind an F5 BIG-IP WAF that rejects requests with a
// terse or absent user agent ("The requested URL was rejected"), so a full
// browser user agent string is required -- "Mozilla/5.0" alone is not enough.
export const ALASKA_APOC_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

export type AlaskaApocCampaignIncomeRow = {
  reportYear: number | null;
  filerId: string;
  filerName: string;
  filerType: string;
  name: string;
  office: string;
  date: string;
  type: string;
  contributor: string;
  address: string;
  city: string;
  state: string;
  zip: string;
  country: string;
  paymentType: string;
  paymentDetail: string;
  occupation: string;
  employer: string;
  purpose: string;
  amount: number;
  submitted: string;
  status: string;
  sourceUrl: string | null;
};

export type AlaskaApocIndependentExpenditureRow = {
  reportYear: number | null;
  filerId: string;
  filerName: string;
  filerType: string;
  businessPhone: string;
  businessType: string;
  type: string;
  date: string;
  recipient: string;
  address: string;
  city: string;
  state: string;
  zip: string;
  country: string;
  position: string;
  candidateProposition: string;
  description: string;
  reportType: string;
  election: string;
  paymentType: string;
  paymentDetail: string;
  amount: number;
  submitted: string;
  status: string;
  sourceUrl: string | null;
};

export type AlaskaApocIndependentContributionRow = {
  reportYear: number | null;
  filerId: string;
  filerName: string;
  filerType: string;
  businessPhone: string;
  businessType: string;
  type: string;
  date: string;
  contributor: string;
  contributorAddress: string;
  contributorCity: string;
  contributorState: string;
  contributorZip: string;
  contributorCountry: string;
  employer: string;
  occupation: string;
  reportType: string;
  election: string;
  officers: string;
  amount: number;
  submitted: string;
  status: string;
  sourceUrl: string | null;
};

export type AlaskaApocCsvFetchFn = (url: string, init?: RequestInit) => Promise<Response>;

export type AlaskaApocCsvFetchOptions = {
  fetchFn?: AlaskaApocCsvFetchFn;
  timeoutMs?: number;
  retryCount?: number;
  retryDelayMs?: number;
  logger?: Pick<typeof console, "warn">;
};

export type AlaskaApocFinanceCsvBundle = {
  incomeCsv: string;
  independentExpenditureCsv: string | null;
  independentContributionCsv: string | null;
  incomeSourceUrl: string;
  independentExpenditureSourceUrl: string | null;
  independentContributionSourceUrl: string | null;
};

export type AlaskaApocExportFetchOptions = AlaskaApocCsvFetchOptions & {
  reportYear: number;
  exportTimeoutMs?: number;
};

export type AlaskaApocFinanceCsvBundleFetchOptions = AlaskaApocCsvFetchOptions & {
  incomeUrl?: string;
  independentExpenditureUrl?: string;
  independentContributionUrl?: string;
  includeIndependentExpenditures?: boolean;
  includeIndependentContributions?: boolean;
  requestSpacingMs?: number;
  reportYear?: number;
  exportTimeoutMs?: number;
};

export function defaultAlaskaApocReportYear(now = new Date()): number {
  return now.getUTCFullYear();
}

type CsvRecord = Record<string, string>;

function normalizeHeader(value: string): string {
  return value
    .replace(/^\uFEFF/, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function normalizeWhitespace(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function parseCsvRows(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < csv.length; index += 1) {
    const char = csv[index];
    const next = csv[index + 1];

    if (inQuotes) {
      if (char === "\"" && next === "\"") {
        field += "\"";
        index += 1;
        continue;
      }
      if (char === "\"") {
        inQuotes = false;
        continue;
      }
      field += char;
      continue;
    }

    if (char === "\"") {
      inQuotes = true;
      continue;
    }
    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }
    if (char === "\n" || char === "\r") {
      if (char === "\r" && next === "\n") {
        index += 1;
      }
      row.push(field);
      if (row.some((cell) => cell.trim().length > 0)) {
        rows.push(row);
      }
      row = [];
      field = "";
      continue;
    }
    field += char;
  }

  row.push(field);
  if (row.some((cell) => cell.trim().length > 0)) {
    rows.push(row);
  }

  return rows;
}

function parseCsvRecords(csv: string): CsvRecord[] {
  const rows = parseCsvRows(csv);
  const header = rows[0];
  if (!header) {
    throw new Error("Alaska APOC CSV export is missing a header row");
  }

  const normalizedHeaders = header.map(normalizeHeader);
  return rows.slice(1).map((cells) => {
    const record: CsvRecord = {};
    normalizedHeaders.forEach((key, index) => {
      if (!key) {
        return;
      }
      record[key] = normalizeWhitespace(cells[index] ?? "");
    });
    return record;
  });
}

function getString(record: CsvRecord, ...headers: string[]): string {
  for (const header of headers) {
    const value = record[normalizeHeader(header)];
    if (value && value.trim().length > 0) {
      return normalizeWhitespace(value);
    }
  }
  return "";
}

export function parseAlaskaApocAmount(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const negative = /^\(.*\)$/.test(trimmed);
  const normalized = trimmed.replace(/[($,)\s]/g, "");
  if (!/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
    return null;
  }
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return Math.round((negative ? -Math.abs(parsed) : parsed) * 100) / 100;
}

function getAmount(record: CsvRecord): number | null {
  return parseAlaskaApocAmount(getString(record, "Amount"));
}

function getReportYear(record: CsvRecord, date: string): number | null {
  const rawYear = getString(record, "Report Year");
  if (/^\d{4}$/.test(rawYear)) {
    return Number(rawYear);
  }
  return parseAlaskaApocDateYear(date);
}

export function parseAlaskaApocDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[3]) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number(isoMatch[1]);
  }
  return null;
}

function compactId(value: string): string {
  return value.trim();
}

function mapValidRows<T>(csv: string, map: (record: CsvRecord) => T | null): T[] {
  const rows: T[] = [];
  for (const record of parseCsvRecords(csv)) {
    const row = map(record);
    if (row) {
      rows.push(row);
    }
  }
  return rows;
}

function assertPositiveInteger(value: number, label: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid Alaska APOC ${label}: ${value}`);
  }
}

function assertNonNegativeInteger(value: number, label: string): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`Invalid Alaska APOC ${label}: ${value}`);
  }
}

function normalizeApocUrl(value: string, label: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`Missing Alaska APOC ${label}`);
  }
  const parsed = new URL(trimmed);
  if (parsed.protocol !== "https:") {
    throw new Error(`Unsupported Alaska APOC ${label} protocol: ${parsed.protocol}. Only https is allowed.`);
  }
  return parsed.toString();
}

function sleep(ms: number): Promise<void> {
  if (ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function isRetryableStatus(status: number): boolean {
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function looksLikeHtmlDocument(body: string): boolean {
  const sample = body.slice(0, 2_000).replace(/^\uFEFF/, "").trimStart().toLowerCase();
  return /^<(?:!doctype\s+html|html)(?:\s|>)/.test(sample);
}

function assertCsvResponse(input: { url: string; body: string; contentType: string | null }): void {
  if (!looksLikeHtmlDocument(input.body)) {
    return;
  }
  const contentTypeDetail = input.contentType ? ` content-type=${input.contentType.toLowerCase()}` : "";
  throw new Error(
    `Alaska APOC CSV request returned an HTML report page instead of a CSV export for ${input.url}.${contentTypeDetail} Use an APOC CSV export file or an export URL.`
  );
}

async function fetchAlaskaApocCsvOnce(input: {
  url: string;
  fetchFn: AlaskaApocCsvFetchFn;
  timeoutMs: number;
}): Promise<string> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), input.timeoutMs);
  timeout.unref?.();

  try {
    const response = await input.fetchFn(input.url, {
      signal: controller.signal,
      headers: {
        accept: "text/csv,text/plain,text/html;q=0.8,*/*;q=0.5",
      },
    });
    if (!response.ok) {
      throw new Error(`Alaska APOC CSV request failed with HTTP ${response.status} for ${input.url}`);
    }
    const body = await response.text();
    assertCsvResponse({ url: input.url, body, contentType: response.headers.get("content-type") });
    return body;
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Alaska APOC CSV request timed out after ${input.timeoutMs}ms for ${input.url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function shouldRetryFetchError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return true;
  }
  const statusMatch = /HTTP\s+(\d{3})/.exec(error.message);
  if (!statusMatch?.[1]) {
    return true;
  }
  return isRetryableStatus(Number(statusMatch[1]));
}

export async function fetchAlaskaApocCsv(url: string, options: AlaskaApocCsvFetchOptions = {}): Promise<string> {
  const normalizedUrl = normalizeApocUrl(url, "CSV URL");
  const timeoutMs = options.timeoutMs ?? ALASKA_APOC_DEFAULT_TIMEOUT_MS;
  const retryCount = options.retryCount ?? ALASKA_APOC_DEFAULT_RETRY_COUNT;
  const retryDelayMs = options.retryDelayMs ?? ALASKA_APOC_DEFAULT_RETRY_DELAY_MS;
  assertPositiveInteger(timeoutMs, "timeoutMs");
  assertNonNegativeInteger(retryCount, "retryCount");
  assertNonNegativeInteger(retryDelayMs, "retryDelayMs");

  const fetchFn = options.fetchFn ?? globalThis.fetch?.bind(globalThis);
  if (!fetchFn) {
    throw new Error("global fetch is unavailable for Alaska APOC CSV fetch");
  }

  let lastError: unknown = null;
  for (let attempt = 0; attempt <= retryCount; attempt += 1) {
    try {
      return await fetchAlaskaApocCsvOnce({ url: normalizedUrl, fetchFn, timeoutMs });
    } catch (error) {
      lastError = error;
      if (attempt >= retryCount || !shouldRetryFetchError(error)) {
        break;
      }
      options.logger?.warn(
        `Alaska APOC CSV fetch retrying url=${normalizedUrl} attempt=${attempt + 1} of ${retryCount}: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      await sleep(retryDelayMs * (attempt + 1));
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

// --- APOC report export chain -------------------------------------------
//
// The APOC report pages are ASP.NET WebForms. Their "Export" button is not a
// direct download: a CSV is only produced by a four-step session flow, and any
// shortcut silently redirects to the site home page instead of failing.
//
//   1. GET the report page            -> session cookies + form state
//   2. POST btnSearch                 -> results held in server-side session
//   3. POST btnExport                 -> renders a dialog holding the CSV href
//   4. GET that href (with Referer)   -> the CSV itself
//
// The href is a plain querystring URL, but it depends on both the session
// cookies and the searched result set, so steps 1-3 cannot be skipped.

type AlaskaApocCookieJar = Map<string, string>;

function rememberCookies(jar: AlaskaApocCookieJar, response: Response): void {
  const setCookies =
    typeof response.headers.getSetCookie === "function" ? response.headers.getSetCookie() : [];
  for (const cookie of setCookies) {
    const [pair] = cookie.split(";");
    const separator = pair?.indexOf("=") ?? -1;
    if (!pair || separator <= 0) {
      continue;
    }
    jar.set(pair.slice(0, separator).trim(), pair.slice(separator + 1).trim());
  }
}

function cookieHeader(jar: AlaskaApocCookieJar): string {
  return [...jar.entries()].map(([name, value]) => `${name}=${value}`).join("; ");
}

function browserRequestHeaders(jar: AlaskaApocCookieJar, referer?: string): Record<string, string> {
  const headers: Record<string, string> = {
    "user-agent": ALASKA_APOC_USER_AGENT,
    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
  };
  const cookies = cookieHeader(jar);
  if (cookies) {
    headers.cookie = cookies;
  }
  if (referer) {
    headers.referer = referer;
  }
  return headers;
}

function decodeHtmlAttribute(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function findAlaskaApocFilterPrefix(html: string): string {
  const match = /name="((?:[^"]*\$)?csfFilter\$)btnSearch"/.exec(html);
  if (!match?.[1]) {
    throw new Error("Alaska APOC report page is missing its search filter form fields");
  }
  return match[1];
}

// Collects the form state the server expects back: hidden fields, the selected
// option of every dropdown, every text input, and the Telerik grid client
// state. Posting blanks instead of the page's real selections makes the export
// dialog silently fail to render.
function collectAlaskaApocFormFields(html: string): Map<string, string> {
  const fields = new Map<string, string>();

  for (const match of html.matchAll(/<input\b[^>]*>/gi)) {
    const tag = match[0];
    const name = /name="([^"]+)"/.exec(tag)?.[1];
    if (!name) {
      continue;
    }
    const type = /type="([^"]+)"/.exec(tag)?.[1]?.toLowerCase() ?? "text";
    if (type === "hidden" || type === "text") {
      fields.set(name, decodeHtmlAttribute(/value="([^"]*)"/.exec(tag)?.[1] ?? ""));
    }
  }

  for (const match of html.matchAll(/<select\b[^>]*name="([^"]+)"[^>]*>([\s\S]*?)<\/select>/gi)) {
    const [, name, body] = match;
    if (!name || body === undefined) {
      continue;
    }
    const selected =
      /<option[^>]*\bselected="selected"[^>]*value="([^"]*)"/.exec(body) ??
      /<option[^>]*value="([^"]*)"/.exec(body);
    fields.set(name, decodeHtmlAttribute(selected?.[1] ?? ""));
  }

  fields.set("__EVENTTARGET", "");
  fields.set("__EVENTARGUMENT", "");
  fields.set("__LASTFOCUS", "");
  return fields;
}

function findAlaskaApocExportCsvHref(html: string): string | null {
  const match = /ExportDialog_hlAllCSV"[^>]*href="([^"]+)"/.exec(html);
  return match?.[1] ? decodeHtmlAttribute(match[1]) : null;
}

// Reads the body inside the timeout window: the ~20 MB export download is the
// part most likely to stall, so the ceiling must cover body consumption, not
// just the response headers.
async function requestAlaskaApoc(input: {
  url: string;
  fetchFn: AlaskaApocCsvFetchFn;
  jar: AlaskaApocCookieJar;
  timeoutMs: number;
  referer?: string;
  body?: URLSearchParams;
}): Promise<{ body: string; contentType: string | null }> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), input.timeoutMs);
  timeout.unref?.();
  try {
    const headers = browserRequestHeaders(input.jar, input.referer);
    if (input.body) {
      headers["content-type"] = "application/x-www-form-urlencoded";
    }
    const response = await input.fetchFn(input.url, {
      signal: controller.signal,
      method: input.body ? "POST" : "GET",
      headers,
      ...(input.body ? { body: input.body.toString() } : {}),
    });
    rememberCookies(input.jar, response);
    if (!response.ok) {
      throw new Error(`Alaska APOC request failed with HTTP ${response.status} for ${input.url}`);
    }
    return { body: await response.text(), contentType: response.headers.get("content-type") };
  } catch (error) {
    if (isAbortError(error)) {
      throw new Error(`Alaska APOC request timed out after ${input.timeoutMs}ms for ${input.url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function alaskaApocFormBody(fields: ReadonlyMap<string, string>, overrides: Record<string, string>): URLSearchParams {
  const body = new URLSearchParams();
  for (const [name, value] of fields) {
    body.set(name, value);
  }
  for (const [name, value] of Object.entries(overrides)) {
    body.set(name, value);
  }
  return body;
}

async function fetchAlaskaApocExportCsvOnce(input: {
  pageUrl: string;
  reportYear: number;
  fetchFn: AlaskaApocCsvFetchFn;
  timeoutMs: number;
  exportTimeoutMs: number;
}): Promise<string> {
  const jar: AlaskaApocCookieJar = new Map();
  const page = await requestAlaskaApoc({
    url: input.pageUrl,
    fetchFn: input.fetchFn,
    jar,
    timeoutMs: input.timeoutMs,
  });
  const prefix = findAlaskaApocFilterPrefix(page.body);
  const reportYearField = `${prefix}ddlReportYear`;

  const search = await requestAlaskaApoc({
    url: input.pageUrl,
    fetchFn: input.fetchFn,
    jar,
    timeoutMs: input.timeoutMs,
    referer: input.pageUrl,
    body: alaskaApocFormBody(collectAlaskaApocFormFields(page.body), {
      [reportYearField]: String(input.reportYear),
      [`${prefix}btnSearch`]: "Search",
    }),
  });

  const exportDialog = await requestAlaskaApoc({
    url: input.pageUrl,
    fetchFn: input.fetchFn,
    jar,
    timeoutMs: input.timeoutMs,
    referer: input.pageUrl,
    body: alaskaApocFormBody(collectAlaskaApocFormFields(search.body), {
      [reportYearField]: String(input.reportYear),
      [`${prefix}btnExport`]: "Export",
    }),
  });
  const href = findAlaskaApocExportCsvHref(exportDialog.body);
  if (!href) {
    throw new Error(
      `Alaska APOC export dialog did not offer a CSV download for ${input.pageUrl} (report year ${input.reportYear})`
    );
  }

  const csvUrl = new URL(href, input.pageUrl).toString();
  const csv = await requestAlaskaApoc({
    url: csvUrl,
    fetchFn: input.fetchFn,
    jar,
    timeoutMs: input.exportTimeoutMs,
    referer: input.pageUrl,
  });
  assertCsvResponse({ url: csvUrl, body: csv.body, contentType: csv.contentType });
  return csv.body;
}

export async function fetchAlaskaApocExportCsv(
  pageUrl: string,
  options: AlaskaApocExportFetchOptions
): Promise<string> {
  const normalizedPageUrl = normalizeApocUrl(pageUrl, "report page URL");
  const reportYear = options.reportYear;
  if (!Number.isInteger(reportYear) || reportYear < 2000 || reportYear > 2100) {
    throw new Error(`Invalid Alaska APOC report year: ${reportYear}`);
  }
  const timeoutMs = options.timeoutMs ?? ALASKA_APOC_DEFAULT_TIMEOUT_MS;
  const exportTimeoutMs = options.exportTimeoutMs ?? ALASKA_APOC_DEFAULT_EXPORT_TIMEOUT_MS;
  const retryCount = options.retryCount ?? ALASKA_APOC_DEFAULT_RETRY_COUNT;
  const retryDelayMs = options.retryDelayMs ?? ALASKA_APOC_DEFAULT_RETRY_DELAY_MS;
  assertPositiveInteger(timeoutMs, "timeoutMs");
  assertPositiveInteger(exportTimeoutMs, "exportTimeoutMs");
  assertNonNegativeInteger(retryCount, "retryCount");
  assertNonNegativeInteger(retryDelayMs, "retryDelayMs");

  const fetchFn = options.fetchFn ?? globalThis.fetch?.bind(globalThis);
  if (!fetchFn) {
    throw new Error("global fetch is unavailable for Alaska APOC CSV fetch");
  }

  let lastError: unknown = null;
  // The whole chain is retried as a unit: the session state it builds cannot be
  // resumed from a partially failed attempt.
  for (let attempt = 0; attempt <= retryCount; attempt += 1) {
    try {
      return await fetchAlaskaApocExportCsvOnce({
        pageUrl: normalizedPageUrl,
        reportYear,
        fetchFn,
        timeoutMs,
        exportTimeoutMs,
      });
    } catch (error) {
      lastError = error;
      if (attempt >= retryCount || !shouldRetryFetchError(error)) {
        break;
      }
      options.logger?.warn(
        `Alaska APOC export retrying url=${normalizedPageUrl} attempt=${attempt + 1} of ${retryCount}: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      await sleep(retryDelayMs * (attempt + 1));
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

export async function fetchAlaskaApocFinanceCsvBundle(
  options: AlaskaApocFinanceCsvBundleFetchOptions = {}
): Promise<AlaskaApocFinanceCsvBundle> {
  const incomeSourceUrl = normalizeApocUrl(options.incomeUrl ?? ALASKA_APOC_CAMPAIGN_INCOME_URL, "campaign income URL");
  const includeIndependentExpenditures = options.includeIndependentExpenditures !== false;
  const includeIndependentContributions = options.includeIndependentContributions !== false;
  const independentExpenditureSourceUrl = includeIndependentExpenditures
    ? normalizeApocUrl(options.independentExpenditureUrl ?? ALASKA_APOC_IE_EXPENDITURES_URL, "independent expenditure URL")
    : null;
  const independentContributionSourceUrl = includeIndependentContributions
    ? normalizeApocUrl(options.independentContributionUrl ?? ALASKA_APOC_IE_CONTRIBUTIONS_URL, "independent contribution URL")
    : null;
  const requestSpacingMs = options.requestSpacingMs ?? ALASKA_APOC_DEFAULT_REQUEST_SPACING_MS;
  assertNonNegativeInteger(requestSpacingMs, "requestSpacingMs");

  const exportOptions: AlaskaApocExportFetchOptions = {
    reportYear: options.reportYear ?? defaultAlaskaApocReportYear(),
    fetchFn: options.fetchFn,
    timeoutMs: options.timeoutMs,
    exportTimeoutMs: options.exportTimeoutMs,
    retryCount: options.retryCount,
    retryDelayMs: options.retryDelayMs,
    logger: options.logger,
  };
  const incomeCsv = await fetchAlaskaApocExportCsv(incomeSourceUrl, exportOptions);
  let independentExpenditureCsv: string | null = null;
  let independentContributionCsv: string | null = null;

  if (independentExpenditureSourceUrl) {
    await sleep(requestSpacingMs);
    independentExpenditureCsv = await fetchAlaskaApocExportCsv(independentExpenditureSourceUrl, exportOptions);
  }
  if (independentContributionSourceUrl) {
    await sleep(requestSpacingMs);
    independentContributionCsv = await fetchAlaskaApocExportCsv(independentContributionSourceUrl, exportOptions);
  }

  return {
    incomeCsv,
    independentExpenditureCsv,
    independentContributionCsv,
    incomeSourceUrl,
    independentExpenditureSourceUrl: independentExpenditureCsv ? independentExpenditureSourceUrl : null,
    independentContributionSourceUrl: independentContributionCsv ? independentContributionSourceUrl : null,
  };
}

// The official export splits individual contributors into "Last/Business Name"
// and "First Name"; rejoin them in the legacy "Last, First" shape so contributor
// identity stays stable for grouping.
function incomeContributor(record: CsvRecord): string {
  const lastOrBusiness = getString(record, "Contributor/Vendor", "Contributor", "Vendor", "Last/Business Name");
  const firstName = getString(record, "First Name");
  return lastOrBusiness && firstName ? `${lastOrBusiness}, ${firstName}` : lastOrBusiness;
}

export function parseAlaskaApocCampaignIncomeCsv(
  csv: string,
  options: { sourceUrl?: string | null } = {}
): AlaskaApocCampaignIncomeRow[] {
  return mapValidRows(csv, (record) => {
    const amount = getAmount(record);
    if (amount === null) {
      return null;
    }
    const date = getString(record, "Date");
    return {
      reportYear: getReportYear(record, date),
      filerId: compactId(getString(record, "Filer ID", "Filer Number", "Filer")),
      // The official CD income export carries no Filer ID or Filer Name column;
      // its "Name" column holds the filing candidate or group, and the
      // resolver's filer key falls back to this name.
      filerName: getString(record, "Filer Name", "Filer", "Name"),
      filerType: getString(record, "Filer Type"),
      name: getString(record, "Name", "Candidate Name", "Group Name"),
      office: getString(record, "Office"),
      date,
      type: getString(record, "Type", "Transaction Type"),
      contributor: incomeContributor(record),
      address: getString(record, "Address"),
      city: getString(record, "City"),
      state: getString(record, "State"),
      zip: getString(record, "Zip"),
      country: getString(record, "Country"),
      paymentType: getString(record, "Payment Type"),
      paymentDetail: getString(record, "Payment Detail"),
      occupation: getString(record, "Occupation"),
      employer: getString(record, "Employer"),
      purpose: getString(record, "Purpose", "Purpose of Expenditure"),
      amount,
      submitted: getString(record, "Submitted"),
      status: getString(record, "Status"),
      sourceUrl: options.sourceUrl ?? null,
    };
  });
}

export function parseAlaskaApocIndependentExpenditureCsv(
  csv: string,
  options: { sourceUrl?: string | null } = {}
): AlaskaApocIndependentExpenditureRow[] {
  return mapValidRows(csv, (record) => {
    const amount = getAmount(record);
    if (amount === null) {
      return null;
    }
    const date = getString(record, "Date");
    return {
      reportYear: getReportYear(record, date),
      filerId: compactId(getString(record, "Filer ID", "Filer Number", "Filer")),
      filerName: getString(record, "Filer Name"),
      filerType: getString(record, "Filer Type"),
      businessPhone: getString(record, "Business Phone"),
      businessType: getString(record, "Business Type"),
      type: getString(record, "Type"),
      date,
      recipient: getString(record, "Recipient"),
      address: getString(record, "Address", "Recipient Address"),
      city: getString(record, "City", "Recipient City"),
      state: getString(record, "State", "Recipient State"),
      zip: getString(record, "Zip", "Recipient Zip"),
      country: getString(record, "Country", "Recipient Country"),
      position: getString(record, "Position"),
      candidateProposition: getString(record, "Candidate/Proposition", "Candidate Proposition"),
      description: getString(record, "Description"),
      reportType: getString(record, "Report Type"),
      election: getString(record, "Election", "Election Name"),
      paymentType: getString(record, "Payment Type"),
      paymentDetail: getString(record, "Payment Detail"),
      amount,
      submitted: getString(record, "Submitted"),
      status: getString(record, "Status"),
      sourceUrl: options.sourceUrl ?? null,
    };
  });
}

export function parseAlaskaApocIndependentContributionCsv(
  csv: string,
  options: { sourceUrl?: string | null } = {}
): AlaskaApocIndependentContributionRow[] {
  return mapValidRows(csv, (record) => {
    const amount = getAmount(record);
    if (amount === null) {
      return null;
    }
    const date = getString(record, "Date");
    return {
      reportYear: getReportYear(record, date),
      filerId: compactId(getString(record, "Filer ID", "Filer Number", "Filer")),
      filerName: getString(record, "Filer Name"),
      filerType: getString(record, "Filer Type"),
      businessPhone: getString(record, "Business Phone"),
      businessType: getString(record, "Business Type"),
      type: getString(record, "Type"),
      date,
      contributor: getString(record, "Contributor"),
      contributorAddress: getString(record, "Contributor Address", "Address"),
      contributorCity: getString(record, "Contributor City", "City"),
      contributorState: getString(record, "Contributor State", "State"),
      contributorZip: getString(record, "Contributor Zip", "Zip"),
      contributorCountry: getString(record, "Contributor Country", "Country"),
      employer: getString(record, "Employer"),
      occupation: getString(record, "Occupation"),
      reportType: getString(record, "Report Type"),
      election: getString(record, "Election"),
      officers: getString(record, "Officers"),
      amount,
      submitted: getString(record, "Submitted"),
      status: getString(record, "Status"),
      sourceUrl: options.sourceUrl ?? null,
    };
  });
}
