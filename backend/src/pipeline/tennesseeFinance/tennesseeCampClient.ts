export const TENNESSEE_CAMP_BASE_URL = "https://apps.tn.gov";
export const TENNESSEE_CAMP_PUBLIC_BASE_PATH = "/tncamp/public";
export const TENNESSEE_CAMP_DEFAULT_TIMEOUT_MS = 30_000;

export type TennesseeCampClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response";

export class TennesseeCampClientError extends Error {
  constructor(
    public readonly code: TennesseeCampClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "TennesseeCampClientError";
  }
}

export type TennesseeCampClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export type TennesseeCampCandidateSearchInput = {
  candidateName: string;
  electionYear: number;
  officeSelection?: string | null;
};

export type TennesseeCampCandidateRecord = {
  campCandidateId: string;
  ownerName: string;
  name: string;
  officeSought: string | null;
  district: string | null;
  electionYear: number | null;
  reportListUrl: string;
  sourceUrl: string;
};

export type TennesseeCampContributionSearchInput = {
  recipientName: string;
  electionYear: number;
  reportYear: number;
  electionYearSelection?: string | null;
  recipientType?: "candidate" | "pac";
};

export type TennesseeCampExpenditureSearchInput = {
  electionYear: number;
  reportYear: number;
  candidatePacName?: string | null;
  electionYearSelection?: string | null;
  expenditureType?: "all" | "independent";
};

export type TennesseeCampCsvRow = Record<string, string>;

export type TennesseeCampContributionRecord = {
  type: string | null;
  adjustment: string | null;
  amount: number;
  date: string | null;
  electionYear: number | null;
  reportName: string | null;
  recipientName: string | null;
  contributorName: string | null;
  contributorOccupation: string | null;
  contributorEmployer: string | null;
};

export type TennesseeCampExpenditureRecord = {
  type: string | null;
  adjustment: string | null;
  amount: number;
  date: string | null;
  electionYear: number | null;
  reportName: string | null;
  candidatePacName: string | null;
  vendorName: string | null;
  purpose: string | null;
  candidateFor: string | null;
  supportOpposeCode: string | null;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new TennesseeCampClientError("invalid_request", `${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number, fieldName = "election year"): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new TennesseeCampClientError("invalid_request", `Invalid Tennessee CAMP ${fieldName}: ${value}`);
  }
  return value;
}

function normalizeSelection(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed || null;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function decodeNumericHtmlEntity(match: string, code: string, radix: number): string {
  const parsed = Number.parseInt(code, radix);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 0x10ffff) {
    return match;
  }
  try {
    return String.fromCodePoint(parsed);
  } catch {
    return match;
  }
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&#(\d+);/g, (match, code: string) => decodeNumericHtmlEntity(match, code, 10))
    .replace(/&#x([0-9a-f]+);/gi, (match, code: string) => decodeNumericHtmlEntity(match, code, 16))
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function stripHtml(value: string): string {
  return decodeHtmlEntities(value.replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function absoluteTennesseeCampUrl(value: string): string {
  const url = new URL(decodeHtmlEntities(value), TENNESSEE_CAMP_BASE_URL);
  if (url.origin !== TENNESSEE_CAMP_BASE_URL || !url.pathname.startsWith(`${TENNESSEE_CAMP_PUBLIC_BASE_PATH}/`)) {
    throw new TennesseeCampClientError("bad_response", "Tennessee CAMP response linked outside the expected CAMP origin");
  }
  return url.toString();
}

function buildTennesseeCampPublicUrl(path: string): string {
  return `${TENNESSEE_CAMP_BASE_URL}${TENNESSEE_CAMP_PUBLIC_BASE_PATH}/${path}`;
}

export function buildTennesseeCampCandidateSearchUrl(): string {
  return buildTennesseeCampPublicUrl("cpsearch.htm");
}

export function buildTennesseeCampContributionSearchUrl(): string {
  return buildTennesseeCampPublicUrl("cesearch.htm");
}

function splitSetCookieHeader(value: string): string[] {
  return value.split(/,(?=\s*[^;,=\s]+=)/).map((cookie) => cookie.trim());
}

function getResponseSetCookies(headers: Headers): string[] {
  const maybeGetSetCookie = headers as Headers & { getSetCookie?: () => string[] };
  const direct = maybeGetSetCookie.getSetCookie?.() ?? [];
  if (direct.length > 0) {
    return direct.flatMap(splitSetCookieHeader);
  }
  const combined = headers.get("set-cookie");
  return combined ? splitSetCookieHeader(combined) : [];
}

function mergeCookies(existingCookieHeader: string | undefined, setCookies: string[]): string | undefined {
  const cookies = new Map<string, string>();
  for (const cookie of existingCookieHeader?.split(/;\s*/) ?? []) {
    const separatorIndex = cookie.indexOf("=");
    if (separatorIndex > 0) {
      cookies.set(cookie.slice(0, separatorIndex), cookie.slice(separatorIndex + 1));
    }
  }
  for (const setCookie of setCookies) {
    const [cookie] = setCookie.split(";", 1);
    const separatorIndex = cookie?.indexOf("=") ?? -1;
    if (cookie && separatorIndex > 0) {
      cookies.set(cookie.slice(0, separatorIndex), cookie.slice(separatorIndex + 1));
    }
  }
  const cookieHeader = [...cookies.entries()].map(([key, value]) => `${key}=${value}`).join("; ");
  return cookieHeader || undefined;
}

async function fetchTennesseeCamp(
  url: string,
  init: RequestInit,
  options: TennesseeCampClientOptions
): Promise<Response> {
  const timeoutMs = options.timeoutMs ?? TENNESSEE_CAMP_DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await (options.fetchImpl ?? fetch)(url, { ...init, signal: controller.signal });
  } catch (error) {
    if (isAbortError(error)) {
      throw new TennesseeCampClientError(
        "network_error",
        `Tennessee CAMP request timed out after ${timeoutMs}ms for ${url}`
      );
    }
    throw new TennesseeCampClientError(
      "network_error",
      `Tennessee CAMP request failed for ${url}: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    clearTimeout(timeout);
  }
}

async function assertOk(response: Response, context: string): Promise<void> {
  if (!response.ok) {
    throw new TennesseeCampClientError(
      "http_error",
      `Tennessee CAMP ${context} failed: ${response.status} ${response.statusText}`,
      response.status
    );
  }
}

function decodeTennesseeCampCsv(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  if (bytes.length >= 2 && bytes[0] === 0xff && bytes[1] === 0xfe) {
    return new TextDecoder("utf-16le").decode(bytes.subarray(2));
  }
  if (bytes.length >= 2 && bytes[0] === 0xfe && bytes[1] === 0xff) {
    return new TextDecoder("utf-16be").decode(bytes.subarray(2));
  }
  return new TextDecoder("utf-8").decode(bytes);
}

function normalizeHeader(value: string): string {
  return value
    .replace(/^\uFEFF/, "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
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
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }
    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }
    if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    if (char === "\r") {
      continue;
    }
    field += char;
  }

  if (inQuotes) {
    throw new TennesseeCampClientError("bad_response", "Tennessee CAMP CSV has an unterminated quoted field");
  }
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  return rows.filter((fields) => fields.some((fieldValue) => fieldValue.trim().length > 0));
}

export function parseTennesseeCampCsvRows(csv: string): TennesseeCampCsvRow[] {
  const rows = parseCsvRows(csv);
  const headerRow = rows[0];
  if (!headerRow || headerRow.length === 0) {
    return [];
  }
  const headers = headerRow.map(normalizeHeader);
  return rows.slice(1).map((fields) => {
    const row: TennesseeCampCsvRow = {};
    for (let index = 0; index < headers.length; index += 1) {
      const header = headers[index];
      if (header) {
        row[header] = fields[index]?.trim() ?? "";
      }
    }
    return row;
  });
}

function getString(row: TennesseeCampCsvRow, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = row[normalizeHeader(key)] ?? row[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return null;
}

function parseAmount(value: string | null): number | null {
  const normalized = (value ?? "").replace(/[$,()]/g, "").trim();
  if (!normalized || !/^-?\d+(?:\.\d+)?$/.test(normalized)) {
    return null;
  }
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return value?.includes("(") && value.includes(")") ? -parsed : parsed;
}

function parseInteger(value: string | null): number | null {
  const trimmed = value?.trim() ?? "";
  if (!/^\d+$/.test(trimmed)) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

export function tennesseeCampContributionRecordFromRow(
  row: TennesseeCampCsvRow
): TennesseeCampContributionRecord | null {
  const amount = parseAmount(getString(row, "amount"));
  if (amount === null) {
    return null;
  }
  return {
    type: getString(row, "type"),
    adjustment: getString(row, "adj", "adjustment"),
    amount,
    date: getString(row, "date"),
    electionYear: parseInteger(getString(row, "election_year")),
    reportName: getString(row, "report_name"),
    recipientName: getString(row, "recipient_name"),
    contributorName: getString(row, "contributor_name"),
    contributorOccupation: getString(row, "contributor_occupation"),
    contributorEmployer: getString(row, "contributor_employer"),
  };
}

export function tennesseeCampExpenditureRecordFromRow(
  row: TennesseeCampCsvRow
): TennesseeCampExpenditureRecord | null {
  const amount = parseAmount(getString(row, "amount"));
  if (amount === null) {
    return null;
  }
  return {
    type: getString(row, "type"),
    adjustment: getString(row, "adj", "adjustment"),
    amount,
    date: getString(row, "date"),
    electionYear: parseInteger(getString(row, "election_year")),
    reportName: getString(row, "report_name"),
    candidatePacName: getString(row, "candidate_pac_name"),
    vendorName: getString(row, "vendor_name"),
    purpose: getString(row, "purpose"),
    candidateFor: getString(row, "candidate_for"),
    supportOpposeCode: getString(row, "s_o"),
  };
}

function parseSelectOptions(html: string, selectName: string): Array<{ value: string; label: string }> {
  const selectMatch = new RegExp(`<select\\b[^>]*\\bname=["']${selectName}["'][^>]*>([\\s\\S]*?)</select>`, "i").exec(html);
  if (!selectMatch?.[1]) {
    return [];
  }
  const options: Array<{ value: string; label: string }> = [];
  const optionRegex = /<option\b[^>]*\bvalue=["']([^"']*)["'][^>]*>([\s\S]*?)<\/option>/gi;
  let match: RegExpExecArray | null;
  while ((match = optionRegex.exec(selectMatch[1]))) {
    const value = decodeHtmlEntities(match[1] ?? "").trim();
    const label = stripHtml(match[2] ?? "");
    if (value && label) {
      options.push({ value, label });
    }
  }
  return options;
}

export function findTennesseeCampElectionYearSelection(html: string, electionYear: number): string | null {
  const year = String(normalizeElectionYear(electionYear));
  const options = parseSelectOptions(html, "electionYearSelection");
  const exact = options.find((option) => option.label === year);
  return exact?.value ?? null;
}

function parseReportListLink(cellHtml: string): { campCandidateId: string; ownerName: string; url: string } | null {
  const match = /href=["']([^"']*replist\.htm\?[^"']*)["']/i.exec(cellHtml);
  if (!match?.[1]) {
    return null;
  }
  const url = new URL(decodeHtmlEntities(match[1]), TENNESSEE_CAMP_BASE_URL);
  const campCandidateId = url.searchParams.get("id")?.trim() ?? "";
  const ownerName = url.searchParams.get("owner")?.trim() ?? "";
  if (!campCandidateId || !ownerName) {
    return null;
  }
  return { campCandidateId, ownerName, url: url.toString() };
}

export function parseTennesseeCampCandidateRecords(
  html: string,
  sourceUrl = buildTennesseeCampCandidateSearchUrl()
): TennesseeCampCandidateRecord[] {
  const tableMatch = /<table\b[^>]*\bid=["']results["'][^>]*>([\s\S]*?)<\/table>/i.exec(html);
  if (!tableMatch?.[1]) {
    return [];
  }
  const rowRegex = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  const records: TennesseeCampCandidateRecord[] = [];
  let rowMatch: RegExpExecArray | null;
  while ((rowMatch = rowRegex.exec(tableMatch[1]))) {
    const rowHtml = rowMatch[1] ?? "";
    if (/<th\b/i.test(rowHtml)) {
      continue;
    }
    const cellHtmls: string[] = [];
    const cellRegex = /<td\b[^>]*>([\s\S]*?)<\/td>/gi;
    let cellMatch: RegExpExecArray | null;
    while ((cellMatch = cellRegex.exec(rowHtml))) {
      cellHtmls.push(cellMatch[1] ?? "");
    }
    if (cellHtmls.length < 5) {
      continue;
    }
    const reportList = parseReportListLink(cellHtmls[4] ?? "");
    if (!reportList) {
      continue;
    }
    records.push({
      campCandidateId: reportList.campCandidateId,
      ownerName: reportList.ownerName,
      name: stripHtml(cellHtmls[0] ?? ""),
      officeSought: stripHtml(cellHtmls[1] ?? "") || null,
      district: stripHtml(cellHtmls[2] ?? "") || null,
      electionYear: parseInteger(stripHtml(cellHtmls[3] ?? "")),
      reportListUrl: reportList.url,
      sourceUrl,
    });
  }
  return records;
}

function extractCsvExportUrl(html: string): string | null {
  // CAMP's DisplayTag export link encodes "export" as its hex field name: 6578706f7274=1.
  const match = /href=["']([^"']*(?:[?&]|&amp;)6578706f7274=1[^"']*)["']/i.exec(html);
  return match?.[1] ? absoluteTennesseeCampUrl(match[1]) : null;
}

export function buildTennesseeCampCandidateSearchFormBody(
  input: TennesseeCampCandidateSearchInput & { electionYearSelection: string }
): string {
  const params = new URLSearchParams();
  params.set("searchType", "candidate");
  params.set("name", requireNonEmpty(input.candidateName, "Tennessee CAMP candidate name"));
  if (input.officeSelection?.trim()) {
    params.set("officeSelection", input.officeSelection.trim());
  }
  params.set("electionYearSelection", requireNonEmpty(input.electionYearSelection, "Tennessee CAMP election year selection"));
  params.set("nameField", "true");
  params.set("officeField", "true");
  params.set("districtField", "true");
  params.set("electionYearField", "true");
  params.set("_continue", "Continue");
  return params.toString();
}

export function buildTennesseeCampContributionSearchFormBody(
  input: TennesseeCampContributionSearchInput & { electionYearSelection: string }
): string {
  const params = new URLSearchParams();
  const recipientType = input.recipientType ?? "candidate";
  params.set("searchType", "contributions");
  params.set("toType", recipientType);
  if (recipientType === "pac") {
    params.set("fromPAC", "true");
    params.set("fromOrganization", "true");
    params.set("fromIndividual", "true");
  } else {
    params.set("fromIndividual", "true");
  }
  params.set("electionYearSelection", requireNonEmpty(input.electionYearSelection, "Tennessee CAMP election year selection"));
  params.set("yearSelection", String(normalizeElectionYear(input.reportYear, "report year")));
  params.set("recipientName", requireNonEmpty(input.recipientName, "Tennessee CAMP recipient name"));
  params.set("typeOf", "all");
  params.set("typeField", "true");
  params.set("adjustmentField", "true");
  params.set("amountField", "true");
  params.set("dateField", "true");
  params.set("electionYearField", "true");
  params.set("reportNameField", "true");
  params.set("recipientNameField", "true");
  params.set("contributorNameField", "true");
  params.set("contributorOccupationField", "true");
  params.set("contributorEmployerField", "true");
  params.set("_continue", "Continue");
  return params.toString();
}

export function buildTennesseeCampExpenditureSearchFormBody(
  input: TennesseeCampExpenditureSearchInput & { electionYearSelection: string }
): string {
  const params = new URLSearchParams();
  params.set("searchType", "expenditures");
  params.set("toType", "both");
  params.set("toCandidate", "true");
  params.set("toPac", "true");
  params.set("toOther", "true");
  params.set("electionYearSelection", requireNonEmpty(input.electionYearSelection, "Tennessee CAMP election year selection"));
  params.set("yearSelection", String(normalizeElectionYear(input.reportYear, "report year")));
  const candidatePacName = normalizeSelection(input.candidatePacName);
  if (candidatePacName) {
    params.set("candName", candidatePacName);
  }
  params.set("typeOf", input.expenditureType ?? "all");
  params.set("typeField", "true");
  params.set("adjustmentField", "true");
  params.set("amountField", "true");
  params.set("dateField", "true");
  params.set("electionYearField", "true");
  params.set("reportNameField", "true");
  params.set("candidatePACNameField", "true");
  params.set("vendorNameField", "true");
  params.set("purposeField", "true");
  params.set("candidateForField", "true");
  params.set("soField", "true");
  params.set("_continue", "Continue");
  return params.toString();
}

async function postSessionedForm(input: {
  url: string;
  body: string;
  cookieHeader: string | undefined;
  options: TennesseeCampClientOptions;
  context: string;
}): Promise<{ html: string; cookieHeader: string | undefined }> {
  const headers = new Headers({
    accept: "text/html,*/*;q=0.8",
    "content-type": "application/x-www-form-urlencoded",
    "content-length": String(Buffer.byteLength(input.body)),
  });
  if (input.cookieHeader) {
    headers.set("cookie", input.cookieHeader);
  }
  const response = await fetchTennesseeCamp(
    input.url,
    {
      method: "POST",
      headers,
      body: input.body,
    },
    input.options
  );
  await assertOk(response, input.context);
  return {
    html: await response.text(),
    cookieHeader: mergeCookies(input.cookieHeader, getResponseSetCookies(response.headers)),
  };
}

async function getSessionLandingPage(input: {
  url: string;
  options: TennesseeCampClientOptions;
  context: string;
}): Promise<{ html: string; cookieHeader: string | undefined }> {
  const response = await fetchTennesseeCamp(input.url, { headers: { accept: "text/html" } }, input.options);
  await assertOk(response, input.context);
  return {
    html: await response.text(),
    cookieHeader: mergeCookies(undefined, getResponseSetCookies(response.headers)),
  };
}

export async function searchTennesseeCampCandidates(
  input: TennesseeCampCandidateSearchInput,
  options: TennesseeCampClientOptions = {}
): Promise<TennesseeCampCandidateRecord[]> {
  normalizeElectionYear(input.electionYear);
  const landing = await getSessionLandingPage({
    url: buildTennesseeCampCandidateSearchUrl(),
    options,
    context: "candidate search landing page request",
  });
  const electionYearSelection = findTennesseeCampElectionYearSelection(landing.html, input.electionYear);
  if (!electionYearSelection) {
    throw new TennesseeCampClientError(
      "invalid_request",
      `Tennessee CAMP election year ${input.electionYear} is not available in candidate search`
    );
  }
  const body = buildTennesseeCampCandidateSearchFormBody({ ...input, electionYearSelection });
  const result = await postSessionedForm({
    url: buildTennesseeCampCandidateSearchUrl(),
    body,
    cookieHeader: landing.cookieHeader,
    options,
    context: "candidate search request",
  });
  return parseTennesseeCampCandidateRecords(result.html, buildTennesseeCampCandidateSearchUrl());
}

export async function fetchTennesseeCampContributionCsvExport(
  input: TennesseeCampContributionSearchInput,
  options: TennesseeCampClientOptions = {}
): Promise<{ csv: string; sourceUrl: string | null }> {
  normalizeElectionYear(input.electionYear);
  normalizeElectionYear(input.reportYear, "report year");
  const landing = await getSessionLandingPage({
    url: buildTennesseeCampContributionSearchUrl(),
    options,
    context: "contribution search landing page request",
  });
  const electionYearSelection =
    normalizeSelection(input.electionYearSelection) ?? findTennesseeCampElectionYearSelection(landing.html, input.electionYear);
  if (!electionYearSelection) {
    throw new TennesseeCampClientError(
      "invalid_request",
      `Tennessee CAMP election year ${input.electionYear} is not available in contribution search`
    );
  }
  const body = buildTennesseeCampContributionSearchFormBody({ ...input, electionYearSelection });
  const result = await postSessionedForm({
    url: buildTennesseeCampContributionSearchUrl(),
    body,
    cookieHeader: landing.cookieHeader,
    options,
    context: "contribution search request",
  });
  const exportUrl = extractCsvExportUrl(result.html);
  if (!exportUrl) {
    if (/No results matched your criteria/i.test(result.html)) {
      return { csv: "", sourceUrl: buildTennesseeCampContributionSearchUrl() };
    }
    throw new TennesseeCampClientError("bad_response", "Tennessee CAMP contribution search did not expose a CSV export link");
  }
  const headers = new Headers({ accept: "text/csv,text/plain;q=0.9,*/*;q=0.1" });
  if (result.cookieHeader) {
    headers.set("cookie", result.cookieHeader);
  }
  const response = await fetchTennesseeCamp(exportUrl, { headers }, options);
  await assertOk(response, "contribution CSV export request");
  return {
    csv: decodeTennesseeCampCsv(await response.arrayBuffer()),
    sourceUrl: exportUrl,
  };
}

export async function fetchTennesseeCampContributionRecords(
  input: TennesseeCampContributionSearchInput,
  options: TennesseeCampClientOptions = {}
): Promise<{ records: TennesseeCampContributionRecord[]; sourceUrl: string | null }> {
  const exportResult = await fetchTennesseeCampContributionCsvExport(input, options);
  return {
    sourceUrl: exportResult.sourceUrl,
    records: parseTennesseeCampCsvRows(exportResult.csv)
      .map(tennesseeCampContributionRecordFromRow)
      .filter((record): record is TennesseeCampContributionRecord => record !== null),
  };
}

export async function fetchTennesseeCampPacContributionRecords(
  input: Omit<TennesseeCampContributionSearchInput, "recipientType">,
  options: TennesseeCampClientOptions = {}
): Promise<{ records: TennesseeCampContributionRecord[]; sourceUrl: string | null }> {
  return await fetchTennesseeCampContributionRecords({ ...input, recipientType: "pac" }, options);
}

export async function fetchTennesseeCampExpenditureCsvExport(
  input: TennesseeCampExpenditureSearchInput,
  options: TennesseeCampClientOptions = {}
): Promise<{ csv: string; sourceUrl: string | null }> {
  normalizeElectionYear(input.electionYear);
  normalizeElectionYear(input.reportYear, "report year");
  const landing = await getSessionLandingPage({
    url: buildTennesseeCampContributionSearchUrl(),
    options,
    context: "expenditure search landing page request",
  });
  const electionYearSelection =
    normalizeSelection(input.electionYearSelection) ?? findTennesseeCampElectionYearSelection(landing.html, input.electionYear);
  if (!electionYearSelection) {
    throw new TennesseeCampClientError(
      "invalid_request",
      `Tennessee CAMP election year ${input.electionYear} is not available in expenditure search`
    );
  }
  const body = buildTennesseeCampExpenditureSearchFormBody({ ...input, electionYearSelection });
  const result = await postSessionedForm({
    url: buildTennesseeCampContributionSearchUrl(),
    body,
    cookieHeader: landing.cookieHeader,
    options,
    context: "expenditure search request",
  });
  const exportUrl = extractCsvExportUrl(result.html);
  if (!exportUrl) {
    if (/No results matched your criteria/i.test(result.html)) {
      return { csv: "", sourceUrl: buildTennesseeCampContributionSearchUrl() };
    }
    throw new TennesseeCampClientError("bad_response", "Tennessee CAMP expenditure search did not expose a CSV export link");
  }
  const headers = new Headers({ accept: "text/csv,text/plain;q=0.9,*/*;q=0.1" });
  if (result.cookieHeader) {
    headers.set("cookie", result.cookieHeader);
  }
  const response = await fetchTennesseeCamp(exportUrl, { headers }, options);
  await assertOk(response, "expenditure CSV export request");
  return {
    csv: decodeTennesseeCampCsv(await response.arrayBuffer()),
    sourceUrl: exportUrl,
  };
}

export async function fetchTennesseeCampExpenditureRecords(
  input: TennesseeCampExpenditureSearchInput,
  options: TennesseeCampClientOptions = {}
): Promise<{ records: TennesseeCampExpenditureRecord[]; sourceUrl: string | null }> {
  const exportResult = await fetchTennesseeCampExpenditureCsvExport(input, options);
  return {
    sourceUrl: exportResult.sourceUrl,
    records: parseTennesseeCampCsvRows(exportResult.csv)
      .map(tennesseeCampExpenditureRecordFromRow)
      .filter((record): record is TennesseeCampExpenditureRecord => record !== null),
  };
}
