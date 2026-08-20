export const NEW_HAMPSHIRE_CFS_API_BASE_URL = "https://cfsapi.sos.nh.gov/api";

export const NEW_HAMPSHIRE_CFS_ENDPOINTS = {
  bulkExport: "ExportData/GetExportPublicDownloadData",
  electionCycles: "Lookup/GetElectionLookupData",
  receipts: "PublicTransactionDetails/GetPublicContributionDetails",
  expenditures: "PublicTransactionDetails/GetPublicExpenditureDetails",
} as const;

export type NewHampshireCfsTransactionTypeCode = "TCON" | "TEXP";

export type NewHampshireCfsClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response";

export class NewHampshireCfsClientError extends Error {
  constructor(
    public readonly code: NewHampshireCfsClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "NewHampshireCfsClientError";
  }
}

export type NewHampshireCfsClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  userAgent?: string;
};

export type NewHampshireElectionCycle = {
  value: number;
  name: string;
  dueDate: string | null;
};

export type NewHampshireReceiptRow = {
  transactionId: number;
  transactionVersionId: number;
  guid: string;
  filerReportId: number;
  filerReportVersionId: number;
  filerEntityId: number;
  filerName: string;
  transactionAmount: number;
  transactionDate: string;
  transactionSubType: string | null;
  reportName: string;
  reportVersion: boolean;
  reportVersionFilter: string;
  reportVersionDescription: string | null;
  isAmended: boolean;
  electionCycle: string | null;
  employerName: string | null;
  occupation: string | null;
};

export type NewHampshireIndependentExpenditureRow = {
  transactionId: number;
  transactionVersionId: number;
  guid: string;
  filerReportId: number;
  filerReportVersionId: number;
  filerEntityId: number;
  filerName: string;
  transactionAmount: number;
  transactionDate: string;
  reportName: string;
  reportVersion: boolean;
  reportVersionFilter: string;
  isAmended: boolean;
  transactionTypeCode: string;
  transactionSubTypeCode: string;
  candidateMeasure: string | null;
  stance: string | null;
  electionCycle: string | null;
  transactionCategory: string | null;
};

export type NewHampshireCfsPage<T> = {
  items: T[];
  totalItems: number;
};

export type NewHampshireReceiptSearchInput = {
  pageNumber?: number;
  pageSize?: number;
  filerName: string;
  electionCycleId: number;
};

export type NewHampshireIndependentExpenditureSearchInput = {
  pageNumber?: number;
  pageSize?: number;
  electionCycleId: number;
};

const DEFAULT_TIMEOUT_MS = 120_000;
// Akamai currently rejects Node's default and descriptive bot user agents.
// This minimal compatibility token plus the public SPA Origin/Referer is the
// request shape the anonymous API accepts (verified live 2026-08-19).
const DEFAULT_USER_AGENT = "Mozilla/5.0";
const DEFAULT_PAGE_SIZE = 200;
const MAX_PAGE_SIZE = 1_000;
const MAX_JSON_RESPONSE_BYTES = 32 * 1024 * 1024;
const MAX_CSV_RESPONSE_BYTES = 128 * 1024 * 1024;

function requirePositiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new NewHampshireCfsClientError("invalid_request", `Invalid ${label}: ${value}`);
  }
  return value;
}

function requirePageSize(value: number | undefined): number {
  const pageSize = value ?? DEFAULT_PAGE_SIZE;
  requirePositiveInteger(pageSize, "New Hampshire CFS page size");
  if (pageSize > MAX_PAGE_SIZE) {
    throw new NewHampshireCfsClientError(
      "invalid_request",
      `New Hampshire CFS page size exceeds ${MAX_PAGE_SIZE}: ${pageSize}`
    );
  }
  return pageSize;
}

function requireFilingYear(value: number): number {
  if (!Number.isInteger(value) || value < 2016 || value > 2100) {
    throw new NewHampshireCfsClientError("invalid_request", `Invalid New Hampshire CFS filing year: ${value}`);
  }
  return value;
}

function objectValue(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new NewHampshireCfsClientError("bad_response", `Invalid New Hampshire CFS ${label}`);
  }
  return value as Record<string, unknown>;
}

function requiredString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new NewHampshireCfsClientError("bad_response", `Invalid New Hampshire CFS ${label}`);
  }
  return value.trim();
}

function nullableString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function requiredNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new NewHampshireCfsClientError("bad_response", `Invalid New Hampshire CFS ${label}`);
  }
  return value;
}

function requiredInteger(value: unknown, label: string): number {
  const parsed = requiredNumber(value, label);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new NewHampshireCfsClientError("bad_response", `Invalid New Hampshire CFS ${label}`);
  }
  return parsed;
}

function booleanValue(value: unknown): boolean {
  return value === true;
}

function buildUrl(endpoint: string): string {
  if (!/^[A-Za-z][A-Za-z0-9/]+$/.test(endpoint)) {
    throw new NewHampshireCfsClientError("invalid_request", `Invalid New Hampshire CFS endpoint: ${endpoint}`);
  }
  return `${NEW_HAMPSHIRE_CFS_API_BASE_URL}/${endpoint}`;
}

async function post(input: {
  endpoint: string;
  body: Record<string, unknown>;
  expectedContentType: "json" | "csv";
  options?: NewHampshireCfsClientOptions;
}): Promise<Uint8Array> {
  const fetchImpl = input.options?.fetchImpl ?? fetch;
  const timeoutMs = input.options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const userAgent = input.options?.userAgent?.trim() || DEFAULT_USER_AGENT;
  requirePositiveInteger(timeoutMs, "New Hampshire CFS timeout");

  let response: Response;
  try {
    response = await fetchImpl(buildUrl(input.endpoint), {
      method: "POST",
      headers: {
        Accept: input.expectedContentType === "json" ? "application/json, text/plain, */*" : "text/csv, */*",
        "Content-Type": "application/json",
        Origin: "https://cfs.sos.nh.gov",
        Referer: "https://cfs.sos.nh.gov/",
        "User-Agent": userAgent,
      },
      body: JSON.stringify(input.body),
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new NewHampshireCfsClientError("network_error", `New Hampshire CFS request failed: ${message}`);
  }

  if (!response.ok) {
    throw new NewHampshireCfsClientError(
      "http_error",
      `New Hampshire CFS request returned HTTP ${response.status}: ${input.endpoint}`,
      response.status
    );
  }

  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  const validContentType =
    input.expectedContentType === "json" ? contentType.includes("application/json") : contentType.includes("text/csv");
  if (!validContentType) {
    throw new NewHampshireCfsClientError(
      "bad_response",
      `New Hampshire CFS ${input.endpoint} returned unexpected content type: ${contentType || "missing"}`
    );
  }

  const bytes = new Uint8Array(await response.arrayBuffer());
  const maxBytes = input.expectedContentType === "json" ? MAX_JSON_RESPONSE_BYTES : MAX_CSV_RESPONSE_BYTES;
  if (bytes.byteLength > maxBytes) {
    throw new NewHampshireCfsClientError(
      "bad_response",
      `New Hampshire CFS ${input.endpoint} exceeded ${maxBytes} bytes`
    );
  }
  return bytes;
}

async function postJson(
  endpoint: string,
  body: Record<string, unknown>,
  options?: NewHampshireCfsClientOptions
): Promise<Record<string, unknown>> {
  const bytes = await post({ endpoint, body, expectedContentType: "json", options });
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new NewHampshireCfsClientError("bad_response", `New Hampshire CFS ${endpoint} returned invalid JSON`);
  }
  const envelope = objectValue(parsed, "response envelope");
  if (envelope.succeeded !== true) {
    const error = typeof envelope.error === "object" && envelope.error !== null
      ? (envelope.error as Record<string, unknown>).message
      : null;
    throw new NewHampshireCfsClientError(
      "bad_response",
      `New Hampshire CFS ${endpoint} failed${typeof error === "string" ? `: ${error}` : ""}`
    );
  }
  return envelope;
}

function parsePage<T>(envelope: Record<string, unknown>, parseItem: (value: unknown) => T): NewHampshireCfsPage<T> {
  const data = objectValue(envelope.data, "page data");
  if (!Array.isArray(data.items)) {
    throw new NewHampshireCfsClientError("bad_response", "Invalid New Hampshire CFS page items");
  }
  const totalItems = requiredNumber(data.totalItems, "page totalItems");
  if (!Number.isInteger(totalItems) || totalItems < 0) {
    throw new NewHampshireCfsClientError("bad_response", "Invalid New Hampshire CFS page totalItems");
  }
  return { items: data.items.map(parseItem), totalItems };
}

function parseReceiptRow(value: unknown): NewHampshireReceiptRow {
  const row = objectValue(value, "receipt row");
  return {
    transactionId: requiredInteger(row.transactionId, "receipt transactionId"),
    transactionVersionId: requiredInteger(row.transactionVersionId, "receipt transactionVersionId"),
    guid: requiredString(row.guid, "receipt guid"),
    filerReportId: requiredInteger(row.filerReportId, "receipt filerReportId"),
    filerReportVersionId: requiredInteger(row.filerReportVersionId, "receipt filerReportVersionId"),
    filerEntityId: requiredInteger(row.filerEntityId, "receipt filerEntityId"),
    filerName: requiredString(row.filerName, "receipt filerName"),
    transactionAmount: requiredNumber(row.transactionAmount, "receipt transactionAmount"),
    transactionDate: requiredString(row.transactionDate, "receipt transactionDate"),
    transactionSubType: nullableString(row.transactionSubType),
    reportName: requiredString(row.reportName, "receipt reportName"),
    reportVersion: booleanValue(row.reportVersion),
    reportVersionFilter: requiredString(row.reportVersionFilter, "receipt reportVersionFilter"),
    reportVersionDescription: nullableString(row.reportVersionDesc),
    isAmended: booleanValue(row.isAmended),
    electionCycle: nullableString(row.electionCycle),
    employerName: nullableString(row.employerName),
    occupation: nullableString(row.occupation),
  };
}

function parseIndependentExpenditureRow(value: unknown): NewHampshireIndependentExpenditureRow {
  const row = objectValue(value, "independent-expenditure row");
  return {
    transactionId: requiredInteger(row.transactionId, "IE transactionId"),
    transactionVersionId: requiredInteger(row.transactionVersionId, "IE transactionVersionId"),
    guid: requiredString(row.guid, "IE guid"),
    filerReportId: requiredInteger(row.filerReportId, "IE filerReportId"),
    filerReportVersionId: requiredInteger(row.filerReportVersionId, "IE filerReportVersionId"),
    filerEntityId: requiredInteger(row.filerEntityId, "IE filerEntityId"),
    filerName: requiredString(row.filerName, "IE filerName"),
    transactionAmount: requiredNumber(row.transactionAmount, "IE transactionAmount"),
    transactionDate: requiredString(row.transactionDate, "IE transactionDate"),
    reportName: requiredString(row.reportName, "IE reportName"),
    reportVersion: booleanValue(row.reportVersion),
    reportVersionFilter: requiredString(row.reportVersionFilter, "IE reportVersionFilter"),
    isAmended: booleanValue(row.isAmended),
    transactionTypeCode: requiredString(row.transactionTypeCode, "IE transactionTypeCode"),
    transactionSubTypeCode: requiredString(row.transactionSubTypeCode, "IE transactionSubTypeCode"),
    candidateMeasure: nullableString(row.candidateMeasure),
    stance: nullableString(row.stance),
    electionCycle: nullableString(row.electionCycle),
    transactionCategory: nullableString(row.transactionCategory),
  };
}

export async function downloadNewHampshireCfsBulkCsv(
  input: { filingYear: number; transactionTypeCode: NewHampshireCfsTransactionTypeCode },
  options?: NewHampshireCfsClientOptions
): Promise<string> {
  const bytes = await post({
    endpoint: NEW_HAMPSHIRE_CFS_ENDPOINTS.bulkExport,
    body: {
      type: "CSV",
      filingYear: requireFilingYear(input.filingYear),
      transactionTypeCode: input.transactionTypeCode,
    },
    expectedContentType: "csv",
    options,
  });
  return new TextDecoder().decode(bytes);
}

export async function getNewHampshireElectionCycles(
  options?: NewHampshireCfsClientOptions
): Promise<NewHampshireElectionCycle[]> {
  const envelope = await postJson(NEW_HAMPSHIRE_CFS_ENDPOINTS.electionCycles, { key: "" }, options);
  if (!Array.isArray(envelope.data)) {
    throw new NewHampshireCfsClientError("bad_response", "Invalid New Hampshire CFS election-cycle data");
  }
  return envelope.data.map((value) => {
    const row = objectValue(value, "election-cycle row");
    return {
      value: requiredInteger(row.value, "election-cycle value"),
      name: requiredString(row.name, "election-cycle name"),
      dueDate: nullableString(row.dueDate),
    };
  });
}

export async function getNewHampshireReceiptPage(
  input: NewHampshireReceiptSearchInput,
  options?: NewHampshireCfsClientOptions
): Promise<NewHampshireCfsPage<NewHampshireReceiptRow>> {
  const filerName = input.filerName.trim();
  if (!filerName) {
    throw new NewHampshireCfsClientError("invalid_request", "New Hampshire receipt search requires filerName");
  }
  const envelope = await postJson(
    NEW_HAMPSHIRE_CFS_ENDPOINTS.receipts,
    {
      pageNumber: requirePositiveInteger(input.pageNumber ?? 1, "New Hampshire CFS page number"),
      pageSize: requirePageSize(input.pageSize),
      sortBy: null,
      sortType: null,
      transactionTypeCode: "TCON",
      filerName,
      electionCycle: String(requirePositiveInteger(input.electionCycleId, "New Hampshire election-cycle ID")),
    },
    options
  );
  return parsePage(envelope, parseReceiptRow);
}

export async function getNewHampshireIndependentExpenditurePage(
  input: NewHampshireIndependentExpenditureSearchInput,
  options?: NewHampshireCfsClientOptions
): Promise<NewHampshireCfsPage<NewHampshireIndependentExpenditureRow>> {
  const envelope = await postJson(
    NEW_HAMPSHIRE_CFS_ENDPOINTS.expenditures,
    {
      pageNumber: requirePositiveInteger(input.pageNumber ?? 1, "New Hampshire CFS page number"),
      pageSize: requirePageSize(input.pageSize),
      sortBy: null,
      sortType: null,
      transactionTypeCode: "TEXP",
      transactionSearch: "TIE",
      electionCycle: String(requirePositiveInteger(input.electionCycleId, "New Hampshire election-cycle ID")),
    },
    options
  );
  return parsePage(envelope, parseIndependentExpenditureRow);
}

async function getAllPages<T>(input: {
  pageSize: number;
  getPage: (pageNumber: number) => Promise<NewHampshireCfsPage<T>>;
}): Promise<T[]> {
  const items: T[] = [];
  let expectedTotal: number | null = null;
  for (let pageNumber = 1; ; pageNumber += 1) {
    const page = await input.getPage(pageNumber);
    if (expectedTotal === null) {
      expectedTotal = page.totalItems;
    } else if (page.totalItems !== expectedTotal) {
      throw new NewHampshireCfsClientError(
        "bad_response",
        `New Hampshire CFS totalItems changed during pagination: ${expectedTotal} -> ${page.totalItems}`
      );
    }
    items.push(...page.items);
    if (items.length >= expectedTotal) {
      if (items.length !== expectedTotal) {
        throw new NewHampshireCfsClientError(
          "bad_response",
          `New Hampshire CFS pagination returned ${items.length} rows for totalItems ${expectedTotal}`
        );
      }
      return items;
    }
    if (page.items.length === 0 || page.items.length > input.pageSize) {
      throw new NewHampshireCfsClientError("bad_response", "New Hampshire CFS pagination ended inconsistently");
    }
  }
}

export async function getAllNewHampshireReceipts(
  input: Omit<NewHampshireReceiptSearchInput, "pageNumber">,
  options?: NewHampshireCfsClientOptions
): Promise<NewHampshireReceiptRow[]> {
  const pageSize = requirePageSize(input.pageSize);
  return getAllPages({
    pageSize,
    getPage: (pageNumber) => getNewHampshireReceiptPage({ ...input, pageNumber, pageSize }, options),
  });
}

export async function getAllNewHampshireIndependentExpenditures(
  input: Omit<NewHampshireIndependentExpenditureSearchInput, "pageNumber">,
  options?: NewHampshireCfsClientOptions
): Promise<NewHampshireIndependentExpenditureRow[]> {
  const pageSize = requirePageSize(input.pageSize);
  return getAllPages({
    pageSize,
    getPage: (pageNumber) =>
      getNewHampshireIndependentExpenditurePage({ ...input, pageNumber, pageSize }, options),
  });
}
