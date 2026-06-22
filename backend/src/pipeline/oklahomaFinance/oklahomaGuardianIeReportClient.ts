export const OKLAHOMA_GUARDIAN_IE_REPORT_SEARCH_URL =
  "https://guardian.ok.gov/PublicSite/SearchPages/Search.aspx?SearchTypeCodeHook=3FF86095-94E1-4145-A34F-248A7C4B4540";
export const OKLAHOMA_GUARDIAN_IE_REPORT_SEARCH_TIMEOUT_MS = 30_000;

const IE_SEARCH_PREFIX =
  "ctl00$Content$3ff86095-94e1-4145-a34f-248a7c4b4540$IEEC_SearchManager$";

const TEXT_SEARCH_TYPE_CODES = {
  contains: "8EDAAC9F-D8C1-41B5-97F9-3D72EFBA987B",
  starts_with: "9730ABEC-5F6B-4DCF-916B-8398A06484C6",
  exact: "FF85FEEC-6E1E-4256-986F-946E242BA7E9",
} as const;

const EXPENDITURE_TYPE_CODES = {
  independent_expenditure: "A7BC217D-68C2-4601-BA0A-0B68C9BF66EA",
  electioneering_communication: "3D7A1D3F-34F4-4FE0-8C73-A545949916E7",
  state_question_communication: "A36FC88B-7C7B-4FFB-852D-44FFC142FC42",
} as const;

export type OklahomaGuardianIeTextSearchMode = keyof typeof TEXT_SEARCH_TYPE_CODES;
export type OklahomaGuardianIeExpenditureType = keyof typeof EXPENDITURE_TYPE_CODES;

export type OklahomaGuardianIeReportSearchInput = {
  candidateName: string;
  electionYear?: number;
  dateFrom?: string;
  dateThrough?: string;
  candidateSearchMode?: OklahomaGuardianIeTextSearchMode;
  expenditureType?: OklahomaGuardianIeExpenditureType;
};

export type OklahomaGuardianIeReportSearchRequest = {
  url: string;
  method: "POST";
  headers: Record<string, string>;
  body: URLSearchParams;
  sourceUrl: string;
};

export type OklahomaGuardianIeReportViewRequest = {
  url: string;
  method: "POST";
  headers: Record<string, string>;
  body: URLSearchParams;
  sourceUrl: string;
  viewReportPostbackTarget: string;
};

export type OklahomaGuardianIeReportSearchRow = {
  filerName: string;
  reportDescription: string;
  periodBegin: string;
  periodEnd: string;
  filedDate: string;
  viewReportPostbackTarget: string;
};

export type OklahomaGuardianIeReportSearchResult = {
  candidateName: string;
  dateFrom: string;
  dateThrough: string;
  expenditureType: OklahomaGuardianIeExpenditureType;
  rows: OklahomaGuardianIeReportSearchRow[];
  sourceUrl: string;
};

export type OklahomaGuardianIeReportPdfArtifact = {
  mimeType: "application/pdf";
  dataUrl: string;
  base64Length: number;
  byteLength: number;
};

export type OklahomaGuardianIeReportDocumentProbeInput = OklahomaGuardianIeReportSearchInput & {
  rowIndex?: number;
  viewReportPostbackTarget?: string;
};

export type OklahomaGuardianIeReportDocumentProbeResult = {
  search: OklahomaGuardianIeReportSearchResult;
  selectedRow: OklahomaGuardianIeReportSearchRow;
  viewReportPostbackTarget: string;
  reportPageHtmlLength: number;
  pdfArtifacts: OklahomaGuardianIeReportPdfArtifact[];
  sourceUrl: string;
};

export type OklahomaGuardianIeReportClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export type OklahomaGuardianIeReportClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response";

export class OklahomaGuardianIeReportClientError extends Error {
  constructor(
    public readonly code: OklahomaGuardianIeReportClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "OklahomaGuardianIeReportClientError";
  }
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&quot;/g, '"')
    .replace(/&#34;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/&#(\d+);/g, (_match, code: string) => String.fromCodePoint(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_match, code: string) => String.fromCodePoint(Number.parseInt(code, 16)));
}

function tagAttributes(tag: string): Map<string, string> {
  const attributes = new Map<string, string>();
  for (const match of tag.matchAll(/([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/g)) {
    attributes.set(match[1].toLowerCase(), decodeHtmlEntities(match[2] ?? match[3] ?? match[4] ?? ""));
  }
  return attributes;
}

function normalizeCandidateName(value: string): string {
  const normalized = value.trim().replace(/\s+/g, " ");
  if (normalized.length === 0) {
    throw new OklahomaGuardianIeReportClientError("invalid_request", "Oklahoma IE candidate name is required");
  }
  return normalized;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new OklahomaGuardianIeReportClientError("invalid_request", `Invalid Oklahoma IE election year: ${value}`);
  }
  return value;
}

function normalizeGuardianDate(value: string, fieldName: string): string {
  const normalized = value.trim();
  if (!/^(0?[1-9]|1[0-2])\/(0?[1-9]|[12]\d|3[01])\/\d{4}$/.test(normalized)) {
    throw new OklahomaGuardianIeReportClientError("invalid_request", `Invalid Oklahoma IE ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeSearchDates(input: OklahomaGuardianIeReportSearchInput): { dateFrom: string; dateThrough: string } {
  if (input.electionYear !== undefined) {
    const year = normalizeElectionYear(input.electionYear);
    return { dateFrom: `01/01/${year}`, dateThrough: `12/31/${year}` };
  }
  if (!input.dateFrom || !input.dateThrough) {
    throw new OklahomaGuardianIeReportClientError(
      "invalid_request",
      "Oklahoma IE search requires electionYear or both dateFrom and dateThrough"
    );
  }
  return {
    dateFrom: normalizeGuardianDate(input.dateFrom, "dateFrom"),
    dateThrough: normalizeGuardianDate(input.dateThrough, "dateThrough"),
  };
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

type GuardianCookieJar = Map<string, string>;

function setCookieHeaderValues(headers: Headers): string[] {
  const nodeHeaders = headers as Headers & { getSetCookie?: () => string[] };
  const setCookies = nodeHeaders.getSetCookie?.();
  if (setCookies && setCookies.length > 0) {
    return setCookies;
  }
  const singleHeader = headers.get("set-cookie");
  return singleHeader ? [singleHeader] : [];
}

function storeResponseCookies(headers: Headers, cookieJar: GuardianCookieJar): void {
  for (const header of setCookieHeaderValues(headers)) {
    const firstPart = header.split(";")[0]?.trim();
    if (!firstPart) {
      continue;
    }
    const separatorIndex = firstPart.indexOf("=");
    if (separatorIndex <= 0) {
      continue;
    }
    cookieJar.set(firstPart.slice(0, separatorIndex), firstPart.slice(separatorIndex + 1));
  }
}

function formatCookieHeader(cookieJar: GuardianCookieJar): string | null {
  const cookies = Array.from(cookieJar.entries()).map(([name, value]) => `${name}=${value}`);
  return cookies.length > 0 ? cookies.join("; ") : null;
}

async function fetchGuardianSession(
  url: string,
  init: RequestInit,
  options: OklahomaGuardianIeReportClientOptions,
  cookieJar: GuardianCookieJar
): Promise<Response> {
  const headers = new Headers(init.headers);
  const cookie = formatCookieHeader(cookieJar);
  if (cookie) {
    headers.set("cookie", cookie);
  }
  const response = await fetchWithTimeout(url, { ...init, headers }, options);
  storeResponseCookies(response.headers, cookieJar);
  return response;
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  options: OklahomaGuardianIeReportClientOptions
): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? OKLAHOMA_GUARDIAN_IE_REPORT_SEARCH_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers);
  if (!headers.has("accept")) {
    headers.set("accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1");
  }
  try {
    return await (options.fetchImpl ?? fetch)(url, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new OklahomaGuardianIeReportClientError(
        "network_error",
        `Oklahoma Guardian IE report request timed out after ${timeoutMs}ms for ${url}`
      );
    }
    throw new OklahomaGuardianIeReportClientError(
      "network_error",
      `Oklahoma Guardian IE report request failed for ${url}: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    clearTimeout(timeout);
  }
}

export function extractOklahomaGuardianWebFormHiddenFields(html: string): URLSearchParams {
  const fields = new URLSearchParams();
  for (const match of html.matchAll(/<input\b[^>]*>/gi)) {
    const attributes = tagAttributes(match[0]);
    if ((attributes.get("type") ?? "").toLowerCase() !== "hidden") {
      continue;
    }
    const name = attributes.get("name");
    if (!name) {
      continue;
    }
    fields.set(name, attributes.get("value") ?? "");
  }
  if (!fields.has("__VIEWSTATE") || !fields.has("__EVENTVALIDATION")) {
    throw new OklahomaGuardianIeReportClientError(
      "bad_response",
      "Oklahoma Guardian IE search page did not include required WebForms hidden fields"
    );
  }
  return fields;
}

export function buildOklahomaGuardianIeReportSearchRequest(input: {
  searchPageHtml: string;
  search: OklahomaGuardianIeReportSearchInput;
  url?: string;
}): OklahomaGuardianIeReportSearchRequest {
  const candidateName = normalizeCandidateName(input.search.candidateName);
  const { dateFrom, dateThrough } = normalizeSearchDates(input.search);
  const candidateSearchMode = input.search.candidateSearchMode ?? "contains";
  const expenditureType = input.search.expenditureType ?? "independent_expenditure";
  const textSearchCode = TEXT_SEARCH_TYPE_CODES[candidateSearchMode];
  const expenditureTypeCode = EXPENDITURE_TYPE_CODES[expenditureType];
  const url = input.url ?? OKLAHOMA_GUARDIAN_IE_REPORT_SEARCH_URL;
  const body = extractOklahomaGuardianWebFormHiddenFields(input.searchPageHtml);
  const searchButtonName = `${IE_SEARCH_PREFIX}IEEC_SearchButton$ctl01`;

  body.set("__EVENTTARGET", searchButtonName);
  body.set("__EVENTARGUMENT", "");
  body.set(`${IE_SEARCH_PREFIX}IEEC_SearchParams$DisbursementCodeHook$ctl01`, expenditureTypeCode);
  body.set(`${IE_SEARCH_PREFIX}IEEC_SearchParams$DateFrom$ctl01`, dateFrom);
  body.set(`${IE_SEARCH_PREFIX}IEEC_SearchParams$DateThrough$ctl01`, dateThrough);
  body.set(`${IE_SEARCH_PREFIX}IEEC_SearchParams$AssocCandidateName$ctl01`, candidateName);
  body.set(`${IE_SEARCH_PREFIX}IEEC_SearchParams$AssocCandidateTextSearchTypeCodeHook$ctl01`, textSearchCode);
  body.set(searchButtonName, "Search");

  return {
    url,
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      referer: url,
    },
    body,
    sourceUrl: url,
  };
}

export function buildOklahomaGuardianIeReportViewRequest(input: {
  searchResultsHtml: string;
  viewReportPostbackTarget: string;
  url?: string;
}): OklahomaGuardianIeReportViewRequest {
  const viewReportPostbackTarget = input.viewReportPostbackTarget.trim();
  if (!viewReportPostbackTarget.includes("ViewReport")) {
    throw new OklahomaGuardianIeReportClientError(
      "invalid_request",
      `Invalid Oklahoma IE View Report postback target: ${input.viewReportPostbackTarget}`
    );
  }
  const url = input.url ?? OKLAHOMA_GUARDIAN_IE_REPORT_SEARCH_URL;
  const body = extractOklahomaGuardianWebFormHiddenFields(input.searchResultsHtml);
  body.set("__EVENTTARGET", viewReportPostbackTarget);
  body.set("__EVENTARGUMENT", "");

  return {
    url,
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      referer: url,
    },
    body,
    sourceUrl: url,
    viewReportPostbackTarget,
  };
}

function stripTags(html: string): string {
  return decodeHtmlEntities(html.replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
}

function tableContainsGuardianIeHeaders(tableHtml: string): boolean {
  return (
    /abbr=["']FilerName["']/i.test(tableHtml) &&
    /abbr=["']Description["']/i.test(tableHtml) &&
    /abbr=["']ViewReport["']/i.test(tableHtml)
  );
}

function extractViewReportPostbackTarget(cellHtml: string): string | null {
  const decoded = decodeHtmlEntities(cellHtml);
  const match = decoded.match(/__doPostBack\('([^']+)'\s*,\s*'[^']*'\)/i);
  return match?.[1]?.trim() || null;
}

export function parseOklahomaGuardianIeReportSearchRows(html: string): OklahomaGuardianIeReportSearchRow[] {
  const tableMatch = Array.from(html.matchAll(/<table\b[\s\S]*?<\/table>/gi)).find((match) =>
    tableContainsGuardianIeHeaders(match[0])
  );
  if (!tableMatch) {
    return [];
  }

  const rows: OklahomaGuardianIeReportSearchRow[] = [];
  for (const rowMatch of tableMatch[0].matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const rowHtml = rowMatch[1];
    if (/<th\b/i.test(rowHtml)) {
      continue;
    }
    const cells = Array.from(rowHtml.matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)).map((match) => match[1]);
    if (cells.length < 6) {
      continue;
    }
    const viewReportPostbackTarget = extractViewReportPostbackTarget(cells[5]);
    if (!viewReportPostbackTarget) {
      continue;
    }
    const row = {
      filerName: stripTags(cells[0]),
      reportDescription: stripTags(cells[1]),
      periodBegin: stripTags(cells[2]),
      periodEnd: stripTags(cells[3]),
      filedDate: stripTags(cells[4]),
      viewReportPostbackTarget,
    };
    if (row.filerName && row.reportDescription && row.periodBegin && row.periodEnd && row.filedDate) {
      rows.push(row);
    }
  }
  return rows;
}

function base64DecodedByteLength(value: string): number {
  const normalized = value.replace(/\s+/g, "");
  const padding = normalized.endsWith("==") ? 2 : normalized.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((normalized.length * 3) / 4) - padding);
}

export function parseOklahomaGuardianIeReportPdfArtifacts(html: string): OklahomaGuardianIeReportPdfArtifact[] {
  const artifacts: OklahomaGuardianIeReportPdfArtifact[] = [];
  for (const match of html.matchAll(/(?:href|src)=["'](data:application\/pdf;base64,([^"']+))["']/gi)) {
    const base64 = decodeHtmlEntities(match[2]).replace(/\s+/g, "");
    if (!/^[A-Za-z0-9+/]+={0,2}$/.test(base64)) {
      continue;
    }
    artifacts.push({
      mimeType: "application/pdf",
      dataUrl: `data:application/pdf;base64,${base64}`,
      base64Length: base64.length,
      byteLength: base64DecodedByteLength(base64),
    });
  }
  return artifacts;
}

export async function probeOklahomaGuardianIeReportDocument(
  input: OklahomaGuardianIeReportDocumentProbeInput,
  options: OklahomaGuardianIeReportClientOptions = {}
): Promise<OklahomaGuardianIeReportDocumentProbeResult> {
  const candidateName = normalizeCandidateName(input.candidateName);
  const { dateFrom, dateThrough } = normalizeSearchDates(input);
  const expenditureType = input.expenditureType ?? "independent_expenditure";
  const cookieJar: GuardianCookieJar = new Map();
  const searchPageResponse = await fetchGuardianSession(
    OKLAHOMA_GUARDIAN_IE_REPORT_SEARCH_URL,
    { method: "GET" },
    options,
    cookieJar
  );
  if (!searchPageResponse.ok) {
    throw new OklahomaGuardianIeReportClientError(
      "http_error",
      `Failed to fetch Oklahoma Guardian IE search page: ${searchPageResponse.status} ${searchPageResponse.statusText}`,
      searchPageResponse.status
    );
  }
  const searchPageHtml = await searchPageResponse.text();
  const searchRequest = buildOklahomaGuardianIeReportSearchRequest({
    searchPageHtml,
    search: { ...input, candidateName, dateFrom, dateThrough, expenditureType },
  });
  const searchResponse = await fetchGuardianSession(
    searchRequest.url,
    {
      method: searchRequest.method,
      headers: searchRequest.headers,
      body: searchRequest.body,
    },
    options,
    cookieJar
  );
  if (!searchResponse.ok) {
    throw new OklahomaGuardianIeReportClientError(
      "http_error",
      `Failed to search Oklahoma Guardian IE reports: ${searchResponse.status} ${searchResponse.statusText}`,
      searchResponse.status
    );
  }
  const searchResultsHtml = await searchResponse.text();
  const rows = parseOklahomaGuardianIeReportSearchRows(searchResultsHtml);
  const rowIndex = input.rowIndex ?? 0;
  if (!Number.isInteger(rowIndex) || rowIndex < 0) {
    throw new OklahomaGuardianIeReportClientError("invalid_request", `Invalid Oklahoma IE report row index: ${rowIndex}`);
  }
  const selectedRow = rows[rowIndex];
  if (!selectedRow) {
    throw new OklahomaGuardianIeReportClientError(
      "bad_response",
      `Oklahoma Guardian IE search did not return row index ${rowIndex}`
    );
  }
  const viewReportPostbackTarget = input.viewReportPostbackTarget?.trim() || selectedRow.viewReportPostbackTarget;
  const viewRequest = buildOklahomaGuardianIeReportViewRequest({
    searchResultsHtml,
    viewReportPostbackTarget,
  });
  const viewResponse = await fetchGuardianSession(
    viewRequest.url,
    {
      method: viewRequest.method,
      headers: viewRequest.headers,
      body: viewRequest.body,
    },
    options,
    cookieJar
  );
  if (!viewResponse.ok) {
    throw new OklahomaGuardianIeReportClientError(
      "http_error",
      `Failed to open Oklahoma Guardian IE report: ${viewResponse.status} ${viewResponse.statusText}`,
      viewResponse.status
    );
  }
  const reportPageHtml = await viewResponse.text();

  return {
    search: {
      candidateName,
      dateFrom,
      dateThrough,
      expenditureType,
      rows,
      sourceUrl: searchRequest.sourceUrl,
    },
    selectedRow,
    viewReportPostbackTarget,
    reportPageHtmlLength: reportPageHtml.length,
    pdfArtifacts: parseOklahomaGuardianIeReportPdfArtifacts(reportPageHtml),
    sourceUrl: viewRequest.sourceUrl,
  };
}

export async function searchOklahomaGuardianIeReports(
  input: OklahomaGuardianIeReportSearchInput,
  options: OklahomaGuardianIeReportClientOptions = {}
): Promise<OklahomaGuardianIeReportSearchResult> {
  const candidateName = normalizeCandidateName(input.candidateName);
  const { dateFrom, dateThrough } = normalizeSearchDates(input);
  const expenditureType = input.expenditureType ?? "independent_expenditure";
  const cookieJar: GuardianCookieJar = new Map();
  const searchPageResponse = await fetchGuardianSession(
    OKLAHOMA_GUARDIAN_IE_REPORT_SEARCH_URL,
    { method: "GET" },
    options,
    cookieJar
  );
  if (!searchPageResponse.ok) {
    throw new OklahomaGuardianIeReportClientError(
      "http_error",
      `Failed to fetch Oklahoma Guardian IE search page: ${searchPageResponse.status} ${searchPageResponse.statusText}`,
      searchPageResponse.status
    );
  }
  const searchPageHtml = await searchPageResponse.text();
  const request = buildOklahomaGuardianIeReportSearchRequest({
    searchPageHtml,
    search: { ...input, candidateName, dateFrom, dateThrough, expenditureType },
  });
  const resultResponse = await fetchGuardianSession(
    request.url,
    {
      method: request.method,
      headers: request.headers,
      body: request.body,
    },
    options,
    cookieJar
  );
  if (!resultResponse.ok) {
    throw new OklahomaGuardianIeReportClientError(
      "http_error",
      `Failed to search Oklahoma Guardian IE reports: ${resultResponse.status} ${resultResponse.statusText}`,
      resultResponse.status
    );
  }

  return {
    candidateName,
    dateFrom,
    dateThrough,
    expenditureType,
    rows: parseOklahomaGuardianIeReportSearchRows(await resultResponse.text()),
    sourceUrl: request.sourceUrl,
  };
}
