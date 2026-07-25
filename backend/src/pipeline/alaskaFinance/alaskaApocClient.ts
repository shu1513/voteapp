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

export type AlaskaApocCampaignIncomeRow = {
  reportYear: number | null;
  filerId: string;
  filerName: string;
  filerType: string;
  name: string;
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

export type AlaskaApocFinanceCsvBundleFetchOptions = AlaskaApocCsvFetchOptions & {
  incomeUrl?: string;
  independentExpenditureUrl?: string;
  independentContributionUrl?: string;
  includeIndependentExpenditures?: boolean;
  includeIndependentContributions?: boolean;
  requestSpacingMs?: number;
};

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

  const fetchOptions: AlaskaApocCsvFetchOptions = {
    fetchFn: options.fetchFn,
    timeoutMs: options.timeoutMs,
    retryCount: options.retryCount,
    retryDelayMs: options.retryDelayMs,
    logger: options.logger,
  };
  const incomeCsv = await fetchAlaskaApocCsv(incomeSourceUrl, fetchOptions);
  let independentExpenditureCsv: string | null = null;
  let independentContributionCsv: string | null = null;

  if (independentExpenditureSourceUrl) {
    await sleep(requestSpacingMs);
    independentExpenditureCsv = await fetchAlaskaApocCsv(independentExpenditureSourceUrl, fetchOptions);
  }
  if (independentContributionSourceUrl) {
    await sleep(requestSpacingMs);
    independentContributionCsv = await fetchAlaskaApocCsv(independentContributionSourceUrl, fetchOptions);
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
      date,
      type: getString(record, "Type", "Transaction Type"),
      contributor: getString(record, "Contributor/Vendor", "Contributor", "Vendor", "Last/Business Name"),
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
