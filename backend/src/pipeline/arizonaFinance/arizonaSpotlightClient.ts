export const ARIZONA_SPOTLIGHT_BASE_URL = "https://seethemoney.az.gov";
export const ARIZONA_SPOTLIGHT_ADVANCED_SEARCH_PATH = "/Reporting/AdvancedSearch/";
export const ARIZONA_SPOTLIGHT_SOURCE = "ARIZONA_SPOTLIGHT" as const;
export const ARIZONA_SPOTLIGHT_DEFAULT_TIMEOUT_MS = 30_000;
export const ARIZONA_SPOTLIGHT_DEFAULT_PAGE_LENGTH = 100;
export const ARIZONA_SPOTLIGHT_MAX_PAGE_LENGTH = 1_000;
export const ARIZONA_SPOTLIGHT_DEFAULT_MAX_PAGES = 50;

export type ArizonaSpotlightClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response";

export class ArizonaSpotlightClientError extends Error {
  constructor(
    public readonly code: ArizonaSpotlightClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "ArizonaSpotlightClientError";
  }
}

export type ArizonaSpotlightClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  pageLength?: number;
  maxPages?: number;
};

export type ArizonaSpotlightSupportOppose = "Support" | "Oppose";
export type ArizonaSpotlightPosition = ArizonaSpotlightSupportOppose | "Both";
export type ArizonaSpotlightAdvancedSearchCategory =
  | "Income"
  | "Expenditures"
  | "IndependentExpenditures"
  | "BallotMeasures"
  | "Reports";

export type ArizonaSpotlightCycle = {
  electionYear: number;
  cycleId: string;
  startDate: string;
  endDate: string;
};

export type ArizonaSpotlightAdvancedSearchInput = {
  categoryType: ArizonaSpotlightAdvancedSearchCategory;
  electionYear?: number | null;
  cycleId?: string | null;
  jurisdictionId?: number | string | null;
  commiteeReportId?: string | null;
  filerId?: string | null;
  filerName?: string | null;
  ballotName?: string | null;
  ballotMeasureId?: string | null;
  filerTypeId?: number | string | null;
  officeTypeId?: number | string | null;
  officeId?: number | string | null;
  partyId?: number | string | null;
  contributorName?: string | null;
  vendorName?: string | null;
  stateId?: number | string | null;
  city?: string | null;
  employer?: string | null;
  occupation?: string | null;
  candidateName?: string | null;
  candidateFilerId?: string | null;
  position?: ArizonaSpotlightPosition | null;
  lowAmount?: number | string | null;
  highAmount?: number | string | null;
  startDate?: string | null;
  endDate?: string | null;
};

export type ArizonaSpotlightTransactionSearchInput = Omit<
  ArizonaSpotlightAdvancedSearchInput,
  "categoryType"
> & {
  limit?: number | null;
};

export type ArizonaSpotlightIncomeTransaction = {
  transactionDate?: string;
  committeeId: string;
  committeeName: string;
  amount: number;
  transactionName?: string;
  transactionType?: string;
  occupation?: string;
  employer?: string;
  city?: string;
  state?: string;
  zipCode?: string;
  filerName?: string;
  sourceUrl: string;
};

export type ArizonaSpotlightCandidateCommitteeLookupInput = {
  candidateName: string;
  officeName?: string | null;
  electionYear: number;
  limit?: number | null;
};

export type ArizonaSpotlightCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  amount: number;
  rowCount: number;
  sourceUrl: string | null;
};

export type ArizonaSpotlightIndependentExpenditure = {
  transactionDate?: string;
  committeeId: string;
  committeeName: string;
  amount: number;
  transactionName?: string;
  transactionType?: string;
  memo?: string;
  city?: string;
  state?: string;
  zipCode?: string;
  supportOppose?: ArizonaSpotlightSupportOppose;
  sourceUrl: string;
};

type ArizonaSpotlightDatatablesPayload = {
  draw: number | null;
  recordsTotal: number | null;
  recordsFiltered: number | null;
  data: unknown[];
};

type ArizonaSpotlightColumn = {
  data: string;
  name?: string;
  searchable?: boolean;
  orderable?: boolean;
  searchValue?: string;
  searchRegex?: boolean;
};

export const ARIZONA_SPOTLIGHT_CYCLES: readonly ArizonaSpotlightCycle[] = [
  {
    electionYear: 2026,
    cycleId: "44~1/1/2025 12:00:00 AM~12/31/2026 11:59:59 PM",
    startDate: "2025-01-01",
    endDate: "2026-12-31",
  },
  {
    electionYear: 2024,
    cycleId: "43~1/1/2023 12:00:00 AM~12/31/2024 11:59:59 PM",
    startDate: "2023-01-01",
    endDate: "2024-12-31",
  },
  {
    electionYear: 2022,
    cycleId: "39~1/1/2021 12:00:00 AM~12/31/2022 11:59:59 PM",
    startDate: "2021-01-01",
    endDate: "2022-12-31",
  },
  {
    electionYear: 2020,
    cycleId: "30~1/1/2019 12:00:00 AM~12/31/2020 11:59:59 PM",
    startDate: "2019-01-01",
    endDate: "2020-12-31",
  },
  {
    electionYear: 2018,
    cycleId: "29~11/9/2016 12:00:00 AM~12/31/2018 11:59:59 PM",
    startDate: "2016-11-09",
    endDate: "2018-12-31",
  },
  {
    electionYear: 2016,
    cycleId: "28~11/25/2014 12:00:00 AM~11/8/2016 11:59:59 PM",
    startDate: "2014-11-25",
    endDate: "2016-11-08",
  },
  {
    electionYear: 2014,
    cycleId: "27~11/27/2012 12:00:00 AM~11/24/2014 11:59:59 PM",
    startDate: "2012-11-27",
    endDate: "2014-11-24",
  },
  {
    electionYear: 2012,
    cycleId: "26~11/23/2010 12:00:00 AM~11/26/2012 11:59:59 PM",
    startDate: "2010-11-23",
    endDate: "2012-11-26",
  },
  {
    electionYear: 2010,
    cycleId: "25~11/25/2008 12:00:00 AM~11/22/2010 11:59:59 PM",
    startDate: "2008-11-25",
    endDate: "2010-11-22",
  },
  {
    electionYear: 2008,
    cycleId: "8~11/28/2006 12:00:00 AM~11/24/2008 11:59:59 PM",
    startDate: "2006-11-28",
    endDate: "2008-11-24",
  },
  {
    electionYear: 2006,
    cycleId: "7~11/23/2004 12:00:00 AM~11/27/2006 11:59:59 PM",
    startDate: "2004-11-23",
    endDate: "2006-11-27",
  },
  {
    electionYear: 2004,
    cycleId: "6~11/26/2002 12:00:00 AM~11/22/2004 11:59:59 PM",
    startDate: "2002-11-26",
    endDate: "2004-11-22",
  },
  {
    electionYear: 2002,
    cycleId: "5~11/28/2000 12:00:00 AM~11/25/2002 11:59:59 PM",
    startDate: "2000-11-28",
    endDate: "2002-11-25",
  },
] as const;

const INCOME_COLUMNS: readonly ArizonaSpotlightColumn[] = [
  { data: "TransactionDate" },
  { data: "CommitteeName" },
  { data: "Amount" },
  { data: "TransactionName" },
  { data: "TransactionType" },
  { data: "Occupation" },
  { data: "Employer" },
  { data: "City" },
  { data: "State" },
  { data: "ZipCode" },
] as const;

const INDEPENDENT_EXPENDITURE_COLUMNS: readonly ArizonaSpotlightColumn[] = [
  { data: "TransactionDate" },
  { data: "CommitteeName" },
  { data: "Amount" },
  { data: "TransactionName" },
  { data: "TransactionType" },
  { data: "Memo" },
] as const;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function normalizeOptionalText(value: string | number | null | undefined): string | undefined {
  const normalized = String(value ?? "").trim().replace(/\s+/g, " ");
  return normalized.length > 0 ? normalized : undefined;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2002 || value > 2100) {
    throw new ArizonaSpotlightClientError("invalid_request", `Invalid Arizona election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(
  value: number | null | undefined,
  fieldName: string,
  defaultValue: number,
  maxValue?: number
): number {
  const normalized = value ?? defaultValue;
  if (!Number.isInteger(normalized) || normalized <= 0 || (maxValue !== undefined && normalized > maxValue)) {
    const suffix = maxValue === undefined ? "" : ` and at most ${maxValue}`;
    throw new ArizonaSpotlightClientError(
      "invalid_request",
      `Arizona Spotlight ${fieldName} must be a positive integer${suffix}`
    );
  }
  return normalized;
}

function normalizeAmount(value: number | string | null | undefined, fieldName: string): string | undefined {
  if (value === null || value === undefined || value === "") {
    return undefined;
  }
  const normalized = typeof value === "number" ? value : Number(value.replace(/[$,]/g, ""));
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new ArizonaSpotlightClientError("invalid_request", `Arizona Spotlight ${fieldName} must be a nonnegative amount`);
  }
  return String(normalized);
}

function normalizeCycle(input: ArizonaSpotlightAdvancedSearchInput): string {
  const explicitCycle = normalizeOptionalText(input.cycleId);
  if (explicitCycle) {
    return explicitCycle;
  }
  if (input.electionYear === null || input.electionYear === undefined) {
    return "";
  }
  return arizonaSpotlightCycleForElectionYear(input.electionYear).cycleId;
}

function normalizeCategoryType(value: ArizonaSpotlightAdvancedSearchCategory): ArizonaSpotlightAdvancedSearchCategory {
  const allowed = new Set<ArizonaSpotlightAdvancedSearchCategory>([
    "Income",
    "Expenditures",
    "IndependentExpenditures",
    "BallotMeasures",
    "Reports",
  ]);
  if (!allowed.has(value)) {
    throw new ArizonaSpotlightClientError("invalid_request", `Unsupported Arizona Spotlight category: ${value}`);
  }
  return value;
}

function normalizePosition(value: ArizonaSpotlightPosition | null | undefined): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (value !== "Support" && value !== "Oppose" && value !== "Both") {
    throw new ArizonaSpotlightClientError("invalid_request", `Unsupported Arizona Spotlight position: ${value}`);
  }
  return value;
}

function supportOpposeFromPosition(
  value: ArizonaSpotlightPosition | null | undefined
): ArizonaSpotlightSupportOppose | undefined {
  return value === "Support" || value === "Oppose" ? value : undefined;
}

function getString(row: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const key of keys) {
    const value = row[key];
    const normalized = normalizeOptionalText(typeof value === "string" || typeof value === "number" ? value : undefined);
    if (normalized) {
      return normalized;
    }
  }
  return undefined;
}

function getNumber(row: Record<string, unknown>, ...keys: string[]): number | undefined {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim().length > 0) {
      const parsed = Number(value.replace(/[$,]/g, ""));
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return undefined;
}

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function parseArizonaSpotlightDate(value: unknown): string | undefined {
  if (typeof value !== "string" || value.trim().length === 0) {
    return undefined;
  }

  const jsonDateMatch = /\/Date\((-?\d+)\)\//.exec(value);
  if (jsonDateMatch) {
    const timestamp = Number(jsonDateMatch[1]);
    if (Number.isFinite(timestamp)) {
      return new Date(timestamp).toISOString().slice(0, 10);
    }
  }

  const isoDate = value.trim().slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) {
    return isoDate;
  }

  const slashDateMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})/.exec(value.trim());
  if (slashDateMatch) {
    const month = slashDateMatch[1].padStart(2, "0");
    const day = slashDateMatch[2].padStart(2, "0");
    return `${slashDateMatch[3]}-${month}-${day}`;
  }

  return undefined;
}

function parseArizonaSpotlightDatatablesPayload(payload: unknown): ArizonaSpotlightDatatablesPayload {
  if (!isRecord(payload) || !Array.isArray(payload.data)) {
    throw new ArizonaSpotlightClientError("bad_response", "Arizona Spotlight response is missing a data array");
  }
  return {
    draw: getNumber(payload, "draw") ?? null,
    recordsTotal: getNumber(payload, "recordsTotal") ?? null,
    recordsFiltered: getNumber(payload, "recordsFiltered") ?? null,
    data: payload.data,
  };
}

function parseIncomeRow(row: unknown, sourceUrl: string): ArizonaSpotlightIncomeTransaction | null {
  if (!isRecord(row)) {
    return null;
  }

  const committeeId = getString(row, "CommitteeID", "committeeId");
  const committeeName = getString(row, "CommitteeName", "committeeName");
  const amount = getNumber(row, "Amount", "amount");
  if (!committeeId || !committeeName || amount === undefined || amount <= 0) {
    return null;
  }

  const transactionDate = parseArizonaSpotlightDate(row.TransactionDate);
  const transactionName = getString(row, "TransactionName", "transactionName");
  const transactionType = getString(row, "TransactionType", "transactionType");
  const occupation = getString(row, "Occupation", "occupation");
  const employer = getString(row, "Employer", "employer");
  const city = getString(row, "City", "city");
  const state = getString(row, "State", "state");
  const zipCode = getString(row, "ZipCode", "zipCode");
  const filerName = getString(row, "FilerName", "filerName");

  return {
    ...(transactionDate ? { transactionDate } : {}),
    committeeId,
    committeeName,
    amount: roundCurrency(amount),
    ...(transactionName ? { transactionName } : {}),
    ...(transactionType ? { transactionType } : {}),
    ...(occupation ? { occupation } : {}),
    ...(employer ? { employer } : {}),
    ...(city ? { city } : {}),
    ...(state ? { state } : {}),
    ...(zipCode ? { zipCode } : {}),
    ...(filerName ? { filerName } : {}),
    sourceUrl,
  };
}

function parseIndependentExpenditureRow(
  row: unknown,
  sourceUrl: string,
  supportOppose: ArizonaSpotlightSupportOppose | undefined
): ArizonaSpotlightIndependentExpenditure | null {
  if (!isRecord(row)) {
    return null;
  }

  const committeeId = getString(row, "CommitteeID", "committeeId");
  const committeeName = getString(row, "CommitteeName", "committeeName");
  const amount = getNumber(row, "Amount", "amount");
  if (!committeeId || !committeeName || amount === undefined || amount <= 0) {
    return null;
  }

  const transactionDate = parseArizonaSpotlightDate(row.TransactionDate);
  const transactionName = getString(row, "TransactionName", "transactionName");
  const transactionType = getString(row, "TransactionType", "transactionType");
  const memo = getString(row, "Memo", "memo");
  const city = getString(row, "City", "city");
  const state = getString(row, "State", "state");
  const zipCode = getString(row, "ZipCode", "zipCode");

  return {
    ...(transactionDate ? { transactionDate } : {}),
    committeeId,
    committeeName,
    amount: roundCurrency(amount),
    ...(transactionName ? { transactionName } : {}),
    ...(transactionType ? { transactionType } : {}),
    ...(memo ? { memo } : {}),
    ...(city ? { city } : {}),
    ...(state ? { state } : {}),
    ...(zipCode ? { zipCode } : {}),
    ...(supportOppose ? { supportOppose } : {}),
    sourceUrl,
  };
}

function datatablesBody(columns: readonly ArizonaSpotlightColumn[], start: number, length: number, draw: number): URLSearchParams {
  const params = new URLSearchParams();
  params.set("draw", String(draw));
  columns.forEach((column, index) => {
    params.set(`columns[${index}][data]`, column.data);
    params.set(`columns[${index}][name]`, column.name ?? "");
    params.set(`columns[${index}][searchable]`, String(column.searchable ?? true));
    params.set(`columns[${index}][orderable]`, String(column.orderable ?? true));
    params.set(`columns[${index}][search][value]`, column.searchValue ?? "");
    params.set(`columns[${index}][search][regex]`, String(column.searchRegex ?? false));
  });
  params.set("order[0][column]", "0");
  params.set("order[0][dir]", "asc");
  params.set("start", String(start));
  params.set("length", String(length));
  params.set("search[value]", "");
  params.set("search[regex]", "false");
  return params;
}

function normalizeFetchLimit(inputLimit: number | null | undefined, options: ArizonaSpotlightClientOptions): number {
  return normalizePositiveInteger(
    inputLimit,
    "limit",
    normalizePositiveInteger(options.pageLength, "pageLength", ARIZONA_SPOTLIGHT_DEFAULT_PAGE_LENGTH, ARIZONA_SPOTLIGHT_MAX_PAGE_LENGTH)
  );
}

async function fetchArizonaSpotlightJson(
  url: string,
  body: URLSearchParams,
  options: ArizonaSpotlightClientOptions
): Promise<unknown> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const timeoutMs = options.timeoutMs ?? ARIZONA_SPOTLIGHT_DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetchImpl(url, {
      method: "POST",
      headers: {
        accept: "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
        "user-agent": "Mozilla/5.0",
      },
      body,
      signal: controller.signal,
    });

    if (!response.ok) {
      throw new ArizonaSpotlightClientError(
        "http_error",
        `Arizona Spotlight request failed: ${response.status} ${response.statusText}`,
        response.status
      );
    }

    return await response.json();
  } catch (error) {
    if (error instanceof ArizonaSpotlightClientError) {
      throw error;
    }
    if (isAbortError(error)) {
      throw new ArizonaSpotlightClientError("network_error", `Arizona Spotlight request timed out: ${url}`);
    }
    throw new ArizonaSpotlightClientError(
      "network_error",
      `Arizona Spotlight request failed: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    clearTimeout(timeout);
  }
}

async function searchArizonaSpotlightPages<T>(
  input: ArizonaSpotlightTransactionSearchInput,
  categoryType: ArizonaSpotlightAdvancedSearchCategory,
  columns: readonly ArizonaSpotlightColumn[],
  parseRow: (row: unknown, sourceUrl: string) => T | null,
  options: ArizonaSpotlightClientOptions
): Promise<T[]> {
  const sourceUrl = buildArizonaSpotlightAdvancedSearchUrl({ ...input, categoryType });
  const limit = normalizeFetchLimit(input.limit, options);
  const pageLength = Math.min(
    normalizePositiveInteger(options.pageLength, "pageLength", ARIZONA_SPOTLIGHT_DEFAULT_PAGE_LENGTH, ARIZONA_SPOTLIGHT_MAX_PAGE_LENGTH),
    limit
  );
  const maxPages = normalizePositiveInteger(options.maxPages, "maxPages", ARIZONA_SPOTLIGHT_DEFAULT_MAX_PAGES);
  const results: T[] = [];
  let start = 0;

  for (let page = 0; page < maxPages && results.length < limit; page += 1) {
    const length = Math.min(pageLength, limit - results.length);
    const payload = parseArizonaSpotlightDatatablesPayload(
      await fetchArizonaSpotlightJson(sourceUrl, datatablesBody(columns, start, length, page + 1), options)
    );

    for (const row of payload.data) {
      const parsed = parseRow(row, sourceUrl);
      if (parsed) {
        results.push(parsed);
        if (results.length >= limit) {
          break;
        }
      }
    }

    start += length;
    const serverRowCount = payload.data.length;
    if (
      serverRowCount < length ||
      (payload.recordsFiltered !== null && start >= payload.recordsFiltered) ||
      (payload.recordsTotal !== null && start >= payload.recordsTotal)
    ) {
      return results;
    }
  }

  if (results.length >= limit) {
    return results;
  }

  throw new ArizonaSpotlightClientError(
    "bad_response",
    `Arizona Spotlight pagination exceeded ${maxPages} page(s) before reaching the requested limit`
  );
}

export function arizonaSpotlightCycleForElectionYear(electionYear: number): ArizonaSpotlightCycle {
  const normalized = normalizeElectionYear(electionYear);
  const cycle = ARIZONA_SPOTLIGHT_CYCLES.find((item) => item.electionYear === normalized);
  if (!cycle) {
    throw new ArizonaSpotlightClientError(
      "invalid_request",
      `Arizona Spotlight cycle is not configured for election year ${normalized}`
    );
  }
  return cycle;
}

export function buildArizonaSpotlightAdvancedSearchUrl(input: ArizonaSpotlightAdvancedSearchInput): string {
  const url = new URL(ARIZONA_SPOTLIGHT_ADVANCED_SEARCH_PATH, ARIZONA_SPOTLIGHT_BASE_URL);
  const cycleId = normalizeCycle(input);
  const cycleParts = cycleId.split("~");
  const cycleStartDate = parseArizonaSpotlightDate(cycleParts[1]);
  const cycleEndDate = parseArizonaSpotlightDate(cycleParts[2]);

  url.searchParams.set("JurisdictionId", normalizeOptionalText(input.jurisdictionId) ?? "0");
  url.searchParams.set("CommiteeReportId", normalizeOptionalText(input.commiteeReportId) ?? "");
  url.searchParams.set("CategoryType", normalizeCategoryType(input.categoryType));
  url.searchParams.set("CycleId", cycleId);
  url.searchParams.set("StartDate", normalizeOptionalText(input.startDate) ?? cycleStartDate ?? "");
  url.searchParams.set("EndDate", normalizeOptionalText(input.endDate) ?? cycleEndDate ?? "");
  url.searchParams.set("FilerId", normalizeOptionalText(input.filerId) ?? "");
  url.searchParams.set("FilerName", normalizeOptionalText(input.filerName) ?? "");
  url.searchParams.set("BallotName", normalizeOptionalText(input.ballotName) ?? "");
  url.searchParams.set("BallotMeasureId", normalizeOptionalText(input.ballotMeasureId) ?? "");
  url.searchParams.set("FilerTypeId", normalizeOptionalText(input.filerTypeId) ?? "");
  url.searchParams.set("OfficeTypeId", normalizeOptionalText(input.officeTypeId) ?? "");
  url.searchParams.set("OfficeId", normalizeOptionalText(input.officeId) ?? "");
  url.searchParams.set("PartyId", normalizeOptionalText(input.partyId) ?? "");
  url.searchParams.set("ContributorName", normalizeOptionalText(input.contributorName) ?? "");
  url.searchParams.set("VendorName", normalizeOptionalText(input.vendorName) ?? "");
  url.searchParams.set("StateId", normalizeOptionalText(input.stateId) ?? "");
  url.searchParams.set("City", normalizeOptionalText(input.city) ?? "");
  url.searchParams.set("Employer", normalizeOptionalText(input.employer) ?? "");
  url.searchParams.set("Occupation", normalizeOptionalText(input.occupation) ?? "");
  url.searchParams.set("CandidateName", normalizeOptionalText(input.candidateName) ?? "");
  url.searchParams.set("CandidateFilerId", normalizeOptionalText(input.candidateFilerId) ?? "");
  url.searchParams.set("Position", normalizePosition(input.position));
  url.searchParams.set("LowAmount", normalizeAmount(input.lowAmount, "lowAmount") ?? "");
  url.searchParams.set("HighAmount", normalizeAmount(input.highAmount, "highAmount") ?? "");
  return url.toString();
}

export function buildArizonaSpotlightDataTablesBody(input: {
  categoryType: "Income" | "IndependentExpenditures";
  start?: number;
  length?: number;
  draw?: number;
}): URLSearchParams {
  const columns = input.categoryType === "Income" ? INCOME_COLUMNS : INDEPENDENT_EXPENDITURE_COLUMNS;
  return datatablesBody(
    columns,
    input.start ?? 0,
    input.length ?? ARIZONA_SPOTLIGHT_DEFAULT_PAGE_LENGTH,
    input.draw ?? 1
  );
}

export async function searchArizonaSpotlightIncomeTransactions(
  input: ArizonaSpotlightTransactionSearchInput,
  options: ArizonaSpotlightClientOptions = {}
): Promise<ArizonaSpotlightIncomeTransaction[]> {
  return searchArizonaSpotlightPages(
    input,
    "Income",
    INCOME_COLUMNS,
    (row, sourceUrl) => parseIncomeRow(row, sourceUrl),
    options
  );
}

export async function searchArizonaSpotlightCandidateCommittees(
  input: ArizonaSpotlightCandidateCommitteeLookupInput,
  options: ArizonaSpotlightClientOptions = {}
): Promise<ArizonaSpotlightCandidateCommitteeMatch[]> {
  const candidateName = normalizeOptionalText(input.candidateName);
  if (!candidateName) {
    throw new ArizonaSpotlightClientError("invalid_request", "Arizona Spotlight candidateName is required");
  }
  const rows = await searchArizonaSpotlightIncomeTransactions(
    {
      electionYear: input.electionYear,
      filerName: candidateName,
      limit: input.limit,
    },
    options
  );
  const matches = new Map<string, ArizonaSpotlightCandidateCommitteeMatch>();
  for (const row of rows) {
    const committeeId = row.committeeId.trim();
    const committeeName = row.committeeName.trim();
    if (!committeeId || !committeeName) {
      continue;
    }
    const existing = matches.get(committeeId);
    if (!existing) {
      matches.set(committeeId, {
        committeeId,
        committeeName,
        amount: row.amount,
        rowCount: 1,
        sourceUrl: row.sourceUrl ?? null,
      });
      continue;
    }
    existing.amount = roundCurrency(existing.amount + row.amount);
    existing.rowCount += 1;
    existing.sourceUrl ??= row.sourceUrl ?? null;
  }
  return [...matches.values()].sort(
    (left, right) => right.amount - left.amount || left.committeeName.localeCompare(right.committeeName)
  );
}

export async function searchArizonaSpotlightIndependentExpenditures(
  input: ArizonaSpotlightTransactionSearchInput,
  options: ArizonaSpotlightClientOptions = {}
): Promise<ArizonaSpotlightIndependentExpenditure[]> {
  const supportOppose = supportOpposeFromPosition(input.position);
  return searchArizonaSpotlightPages(
    input,
    "IndependentExpenditures",
    INDEPENDENT_EXPENDITURE_COLUMNS,
    (row, sourceUrl) => parseIndependentExpenditureRow(row, sourceUrl, supportOppose),
    options
  );
}
