export const WISCONSIN_SUNSHINE_BASE_URL = "https://campaignfinance.wi.gov";
export const WISCONSIN_SUNSHINE_TRPC_URL = `${WISCONSIN_SUNSHINE_BASE_URL}/api/trpc`;
export const WISCONSIN_SUNSHINE_TRANSACTIONS_URL = `${WISCONSIN_SUNSHINE_BASE_URL}/browse-data/transactions`;
export const WISCONSIN_SUNSHINE_DEFAULT_TIMEOUT_MS = 30_000;
export const WISCONSIN_SUNSHINE_DEFAULT_PAGE_LIMIT = 100;
export const WISCONSIN_SUNSHINE_MAX_PAGE_LIMIT = 1_000;
export const WISCONSIN_SUNSHINE_DEFAULT_MAX_PAGES = 50;
export const WISCONSIN_SUNSHINE_DEFAULT_MAX_DATE_SPLIT_DEPTH = 10;
export const WISCONSIN_SUNSHINE_INDEPENDENT_EXPENDITURE_CATEGORY_IDS = [33, 35] as const;

export type WisconsinSunshineClientErrorCode =
  | "invalid_request"
  | "network_error"
  | "http_error"
  | "bad_response"
  | "page_overflow";

export class WisconsinSunshineClientError extends Error {
  constructor(
    public readonly code: WisconsinSunshineClientErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "WisconsinSunshineClientError";
  }
}

export type WisconsinSunshineClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  pageLimit?: number;
  maxPages?: number;
  /** How many times an over-full date window may be bisected before giving up. 0 disables splitting. */
  maxDateSplitDepth?: number;
};

export type WisconsinSunshineCommitteeSearchInput = {
  searchTerm: string;
  limit?: number;
};

export type WisconsinSunshineCommittee = {
  entityId: string;
  committeeId?: string;
  assignedCommitteeId?: string;
  committeeName: string;
  committeeType?: string;
  committeeStatus?: string;
  committeeStatusSlug?: string;
  candidateNames: string[];
  sourceUrl?: string;
};

export type WisconsinSunshineCommitteeTransactionInput = {
  entityId: string | number;
  electionYear: number;
  dateFrom?: string | null;
  dateTo?: string | null;
  limit?: number;
};

export type WisconsinSunshineAggregate = {
  categoryName: string;
  amount: number;
  count: number;
  sourceUrl?: string;
};

export type WisconsinSunshineIndependentSpendingGroupInput = {
  candidateCommitteeName: string;
  electionYear: number;
  office?: string | null;
  district?: string | null;
  categoryIds?: readonly number[];
  limit?: number;
  dateFrom?: string | null;
  dateTo?: string | null;
};

export type WisconsinSunshineIndependentSpendingGroup = {
  sponsorId: string;
  sponsorName: string;
  supportOppose: "support" | "oppose";
  amount: number;
  expenditureCount: number;
  sourceUrl?: string;
};

export type WisconsinSunshineOutsideSpenderFundersInput = WisconsinSunshineCommitteeTransactionInput;

export type WisconsinSunshineOffice = {
  id: string;
  name: string;
  isActive: boolean | null;
};

export type WisconsinSunshineTransactionCategory = {
  id: number;
  code?: string;
  label: string;
  requiresRelatedEntity: boolean | null;
  requiresSupportStance: boolean | null;
  requiresOffice: boolean | null;
};

type TransactionWindow = {
  dateFrom: string;
  dateTo: string;
};

type AggregateAccumulator = {
  categoryName: string;
  amount: number;
  count: number;
  sourceUrl?: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function normalizeNonEmptyString(value: string, fieldName: string): string {
  const normalized = value.trim().replace(/\s+/g, " ");
  if (normalized.length === 0) {
    throw new WisconsinSunshineClientError("invalid_request", `${fieldName} is required`);
  }
  return normalized;
}

function optionalString(value: string | null | undefined): string | undefined {
  const normalized = value?.trim().replace(/\s+/g, " ");
  return normalized && normalized.length > 0 ? normalized : undefined;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new WisconsinSunshineClientError("invalid_request", `Invalid Wisconsin Sunshine election year: ${value}`);
  }
  return value;
}

function normalizeLimit(value: number | undefined, defaultValue: number): number {
  const normalized = value ?? defaultValue;
  if (!Number.isInteger(normalized) || normalized <= 0 || normalized > WISCONSIN_SUNSHINE_MAX_PAGE_LIMIT) {
    throw new WisconsinSunshineClientError(
      "invalid_request",
      `Wisconsin Sunshine limit must be an integer between 1 and ${WISCONSIN_SUNSHINE_MAX_PAGE_LIMIT}`
    );
  }
  return normalized;
}

function normalizePageLimit(value: number | undefined): number {
  return normalizeLimit(value, WISCONSIN_SUNSHINE_DEFAULT_PAGE_LIMIT);
}

function normalizeMaxPages(value: number | undefined): number {
  const normalized = value ?? WISCONSIN_SUNSHINE_DEFAULT_MAX_PAGES;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new WisconsinSunshineClientError("invalid_request", "Wisconsin Sunshine maxPages must be a positive integer");
  }
  return normalized;
}

function normalizeEntityId(value: string | number, fieldName: string): number {
  const raw = String(value).trim();
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new WisconsinSunshineClientError("invalid_request", `${fieldName} must be a positive numeric entity id`);
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    throw new WisconsinSunshineClientError("invalid_request", `${fieldName} is too large`);
  }
  return parsed;
}

function normalizeDateInput(value: string | null | undefined, fieldName: string): string | undefined {
  if (value == null || value.trim().length === 0) {
    return undefined;
  }
  const normalized = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new WisconsinSunshineClientError("invalid_request", `${fieldName} must be YYYY-MM-DD`);
  }
  return normalized;
}

function toTransactionWindow(input: {
  electionYear: number;
  dateFrom?: string | null;
  dateTo?: string | null;
}): TransactionWindow {
  const electionYear = normalizeElectionYear(input.electionYear);
  return {
    dateFrom: normalizeDateInput(input.dateFrom, "dateFrom") ?? `${electionYear - 1}-01-01`,
    dateTo: normalizeDateInput(input.dateTo, "dateTo") ?? `${electionYear}-12-31`,
  };
}

function getObject(row: Record<string, unknown>, key: string): Record<string, unknown> | undefined {
  const value = row[key];
  return isRecord(value) ? value : undefined;
}

function getString(row: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim().replace(/\s+/g, " ");
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return String(value);
    }
  }
  return undefined;
}

function getNestedString(row: Record<string, unknown>, key: string, nestedKey: string): string | undefined {
  const nested = getObject(row, key);
  return nested ? getString(nested, nestedKey) : undefined;
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

function getBoolean(row: Record<string, unknown>, ...keys: string[]): boolean | null {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "boolean") {
      return value;
    }
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (["true", "yes", "y", "1"].includes(normalized)) {
        return true;
      }
      if (["false", "no", "n", "0"].includes(normalized)) {
        return false;
      }
    }
  }
  return null;
}

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function normalizeMatchText(value: string | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^a-zA-Z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function normalizeCategoryText(value: string | undefined): string | undefined {
  const normalized = value?.trim().replace(/\s+/g, " ");
  return normalized && normalized.length > 0 ? normalized.toUpperCase() : undefined;
}

function supportOpposeFromSunshine(value: string | undefined): "support" | "oppose" | null {
  const normalized = value?.trim().toUpperCase();
  if (normalized === "FOR" || normalized === "SUPPORT" || normalized === "SUPPORTING") {
    return "support";
  }
  if (normalized === "AGAINST" || normalized === "OPPOSE" || normalized === "OPPOSING") {
    return "oppose";
  }
  return null;
}

function contributionSizeBucket(amount: number): string {
  if (amount >= 5_000) return "5000_plus";
  if (amount >= 1_000) return "1000_4999";
  if (amount >= 500) return "500_999";
  if (amount >= 250) return "250_499";
  if (amount >= 100) return "100_249";
  return "under_100";
}

function addAggregate(
  map: Map<string, AggregateAccumulator>,
  categoryName: string | undefined,
  amount: number | undefined,
  sourceUrl?: string
): void {
  if (!categoryName || amount == null || !Number.isFinite(amount) || amount <= 0) {
    return;
  }
  const existing = map.get(categoryName) ?? { categoryName, amount: 0, count: 0 };
  existing.amount = roundCurrency(existing.amount + amount);
  existing.count += 1;
  if (!existing.sourceUrl && sourceUrl) {
    existing.sourceUrl = sourceUrl;
  }
  map.set(categoryName, existing);
}

function sortAggregates(map: Map<string, AggregateAccumulator>, limit: number): WisconsinSunshineAggregate[] {
  return Array.from(map.values())
    .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
    .slice(0, limit)
    .map((row) => ({
      categoryName: row.categoryName,
      amount: roundCurrency(row.amount),
      count: row.count,
      ...(row.sourceUrl ? { sourceUrl: row.sourceUrl } : {}),
    }));
}

function isIndividualContributor(row: Record<string, unknown>): boolean {
  const entityType = getObject(getObject(row, "from_entity") ?? {}, "entityType");
  const entityTypeName = getString(entityType ?? {}, "name");
  const entityTypeOf = getString(entityType ?? {}, "entityTypeOf");
  if (!entityTypeName && !entityTypeOf) {
    return true;
  }
  return normalizeMatchText(entityTypeName) === "INDIVIDUAL" || normalizeMatchText(entityTypeOf) === "INDIVIDUAL";
}

function isOrganizationContributor(row: Record<string, unknown>): boolean {
  const entityType = getObject(getObject(row, "from_entity") ?? {}, "entityType");
  const entityTypeName = normalizeMatchText(getString(entityType ?? {}, "name"));
  const entityTypeOf = normalizeMatchText(getString(entityType ?? {}, "entityTypeOf"));
  if (!entityTypeName && !entityTypeOf) {
    return false;
  }
  if (entityTypeName === "INDIVIDUAL" || entityTypeOf === "INDIVIDUAL") {
    return false;
  }
  return (
    ["BUSINESS", "REGISTRANT", "COMMITTEE", "ORGANIZATION", "PAC", "PARTY"].includes(entityTypeName) ||
    ["COMMITTEE", "ORGANIZATION"].includes(entityTypeOf)
  );
}

function extractCandidateNames(row: Record<string, unknown>): string[] {
  const names = new Set<string>();
  const connections = row.entityConnections;
  if (Array.isArray(connections)) {
    for (const connection of connections) {
      if (!isRecord(connection)) continue;
      const entity = getObject(connection, "entity");
      const name = entity ? getString(entity, "name") : undefined;
      if (name) names.add(name);
    }
  }
  return Array.from(names).sort((left, right) => left.localeCompare(right));
}

function parseCommittee(row: unknown): WisconsinSunshineCommittee | null {
  if (!isRecord(row)) {
    return null;
  }
  const entity = getObject(row, "entity");
  const entityId = entity ? getString(entity, "id") : undefined;
  const committeeName = entity ? getString(entity, "name") : undefined;
  if (!entityId || !committeeName) {
    return null;
  }
  const committeeType = getObject(row, "committeeType");
  const committeeStatus = getObject(row, "committeeStatus");
  return {
    entityId,
    ...(getString(row, "id") ? { committeeId: getString(row, "id") } : {}),
    ...(getString(row, "assignedCommitteeId") ? { assignedCommitteeId: getString(row, "assignedCommitteeId") } : {}),
    committeeName,
    ...(getString(committeeType ?? {}, "name") ? { committeeType: getString(committeeType ?? {}, "name") } : {}),
    ...(getString(committeeStatus ?? {}, "name") ? { committeeStatus: getString(committeeStatus ?? {}, "name") } : {}),
    ...(getString(committeeStatus ?? {}, "statusSlug")
      ? { committeeStatusSlug: getString(committeeStatus ?? {}, "statusSlug") }
      : {}),
    candidateNames: extractCandidateNames(row),
    sourceUrl: `${WISCONSIN_SUNSHINE_BASE_URL}/browse-data/registrants/${entityId}`,
  };
}

function parseTransactionRows(payload: unknown): Record<string, unknown>[] {
  if (!isRecord(payload) || !Array.isArray(payload.results)) {
    throw new WisconsinSunshineClientError("bad_response", "Wisconsin Sunshine response did not include results[]");
  }
  return payload.results.filter(isRecord);
}

export function buildWisconsinSunshineTrpcUrl(procedure: string, input: unknown): string {
  if (!/^[A-Za-z0-9_.]+$/.test(procedure)) {
    throw new WisconsinSunshineClientError("invalid_request", `Invalid Wisconsin Sunshine procedure: ${procedure}`);
  }
  const url = new URL(`${WISCONSIN_SUNSHINE_TRPC_URL}/${procedure}`);
  url.searchParams.set("batch", "1");
  url.searchParams.set("input", JSON.stringify({ "0": { json: input } }));
  return url.toString();
}

export function buildWisconsinSunshineCommitteeSearchInput(
  input: WisconsinSunshineCommitteeSearchInput
): Record<string, unknown> {
  return {
    searchTerm: normalizeNonEmptyString(input.searchTerm, "searchTerm"),
    take: normalizeLimit(input.limit, 20),
    skip: 0,
    sortBy: "createdAt",
    sortDirection: "desc",
  };
}

export function buildWisconsinSunshineContributionTransactionInput(
  input: WisconsinSunshineCommitteeTransactionInput,
  paging?: { take?: number; skip?: number }
): Record<string, unknown> {
  const window = toTransactionWindow(input);
  return {
    createdByEntityId: [normalizeEntityId(input.entityId, "entityId")],
    transactionType: [1],
    dateFrom: window.dateFrom,
    dateTo: window.dateTo,
    take: paging?.take ?? normalizePageLimit(undefined),
    skip: paging?.skip ?? 0,
    sortBy: "date",
    sortDirection: "desc",
  };
}

export function buildWisconsinSunshineIndependentExpenditureTransactionInput(
  input: WisconsinSunshineIndependentSpendingGroupInput,
  paging?: { take?: number; skip?: number }
): Record<string, unknown> {
  const window = toTransactionWindow(input);
  const categoryIds = input.categoryIds?.length
    ? input.categoryIds.map((value) => {
        if (!Number.isInteger(value) || value <= 0) {
          throw new WisconsinSunshineClientError("invalid_request", `Invalid Wisconsin Sunshine category id: ${value}`);
        }
        return value;
      })
    : [...WISCONSIN_SUNSHINE_INDEPENDENT_EXPENDITURE_CATEGORY_IDS];
  return {
    transactionCategory: categoryIds,
    dateFrom: window.dateFrom,
    dateTo: window.dateTo,
    take: paging?.take ?? normalizePageLimit(undefined),
    skip: paging?.skip ?? 0,
    sortBy: "date",
    sortDirection: "desc",
  };
}

async function fetchWisconsinSunshineTrpcJson(
  procedure: string,
  input: unknown,
  options: WisconsinSunshineClientOptions = {}
): Promise<unknown> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), options.timeoutMs ?? WISCONSIN_SUNSHINE_DEFAULT_TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetchImpl(buildWisconsinSunshineTrpcUrl(procedure, input), {
      headers: new Headers({ accept: "application/json" }),
      signal: controller.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new WisconsinSunshineClientError("network_error", `Wisconsin Sunshine request timed out: ${procedure}`);
    }
    throw new WisconsinSunshineClientError(
      "network_error",
      `Wisconsin Sunshine request failed: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    clearTimeout(timeoutId);
  }

  if (!response.ok) {
    throw new WisconsinSunshineClientError(
      "http_error",
      `Wisconsin Sunshine request failed: ${response.status} ${response.statusText}`,
      response.status
    );
  }

  let payload: unknown;
  try {
    payload = await response.json();
  } catch (error) {
    throw new WisconsinSunshineClientError(
      "bad_response",
      `Wisconsin Sunshine response was not valid JSON: ${error instanceof Error ? error.message : String(error)}`
    );
  }

  if (!Array.isArray(payload) || payload.length === 0 || !isRecord(payload[0])) {
    throw new WisconsinSunshineClientError("bad_response", "Wisconsin Sunshine response was not a tRPC batch array");
  }
  const first = payload[0];
  if (isRecord(first.error)) {
    const message = getString(first.error, "message") ?? "Wisconsin Sunshine tRPC error";
    throw new WisconsinSunshineClientError("bad_response", message);
  }
  const result = getObject(first, "result");
  const data = result ? getObject(result, "data") : undefined;
  if (!data || !("json" in data)) {
    throw new WisconsinSunshineClientError("bad_response", "Wisconsin Sunshine response did not include data.json");
  }
  return data.json;
}

async function fetchWisconsinSunshinePagedRows(
  procedure: string,
  baseInput: Record<string, unknown>,
  options: WisconsinSunshineClientOptions = {}
): Promise<Record<string, unknown>[]> {
  const pageLimit = normalizePageLimit(options.pageLimit);
  const maxPages = normalizeMaxPages(options.maxPages);
  const rows: Record<string, unknown>[] = [];

  for (let page = 0; page < maxPages; page += 1) {
    const payload = await fetchWisconsinSunshineTrpcJson(
      procedure,
      { ...baseInput, take: pageLimit, skip: page * pageLimit },
      options
    );
    const pageRows = parseTransactionRows(payload);
    rows.push(...pageRows);
    if (pageRows.length < pageLimit) {
      return rows;
    }
  }

  throw new WisconsinSunshineClientError(
    "page_overflow",
    `Wisconsin Sunshine paged read exceeded maxPages=${maxPages}; narrow the query before retrying`
  );
}

function normalizeMaxDateSplitDepth(value: number | undefined): number {
  const normalized = value ?? WISCONSIN_SUNSHINE_DEFAULT_MAX_DATE_SPLIT_DEPTH;
  if (!Number.isInteger(normalized) || normalized < 0) {
    throw new WisconsinSunshineClientError(
      "invalid_request",
      "Wisconsin Sunshine maxDateSplitDepth must be a non-negative integer"
    );
  }
  return normalized;
}

function parseWindowDate(value: unknown): Date | null {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return null;
  }
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatWindowDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function addDays(value: Date, days: number): Date {
  return new Date(value.getTime() + days * 86_400_000);
}

function dedupeRowsById(rows: readonly Record<string, unknown>[]): Record<string, unknown>[] {
  const seenIds = new Set<string>();
  const deduped: Record<string, unknown>[] = [];
  for (const row of rows) {
    const id = getString(row, "id");
    if (id) {
      if (seenIds.has(id)) {
        continue;
      }
      seenIds.add(id);
    }
    deduped.push(row);
  }
  return deduped;
}

/**
 * Paged read that survives filers whose transactions exceed maxPages: on
 * overflow the [dateFrom, dateTo] window is bisected and both halves are
 * fetched independently, recursively, then merged (deduped by row id). A
 * single-day window that still overflows — or exhausted split depth — rethrows.
 */
async function fetchWisconsinSunshinePagedRowsWithDateSplit(
  procedure: string,
  baseInput: Record<string, unknown>,
  options: WisconsinSunshineClientOptions = {},
  depth = 0
): Promise<Record<string, unknown>[]> {
  const maxDateSplitDepth = normalizeMaxDateSplitDepth(options.maxDateSplitDepth);
  try {
    return await fetchWisconsinSunshinePagedRows(procedure, baseInput, options);
  } catch (error) {
    if (!(error instanceof WisconsinSunshineClientError) || error.code !== "page_overflow") {
      throw error;
    }
    const dateFrom = parseWindowDate(baseInput.dateFrom);
    const dateTo = parseWindowDate(baseInput.dateTo);
    if (depth >= maxDateSplitDepth || !dateFrom || !dateTo || dateFrom.getTime() >= dateTo.getTime()) {
      throw error;
    }
    const midpoint = new Date(
      dateFrom.getTime() + Math.floor((dateTo.getTime() - dateFrom.getTime()) / (2 * 86_400_000)) * 86_400_000
    );
    const firstHalf = await fetchWisconsinSunshinePagedRowsWithDateSplit(
      procedure,
      { ...baseInput, dateFrom: formatWindowDate(dateFrom), dateTo: formatWindowDate(midpoint) },
      options,
      depth + 1
    );
    const secondHalf = await fetchWisconsinSunshinePagedRowsWithDateSplit(
      procedure,
      { ...baseInput, dateFrom: formatWindowDate(addDays(midpoint, 1)), dateTo: formatWindowDate(dateTo) },
      options,
      depth + 1
    );
    return dedupeRowsById([...firstHalf, ...secondHalf]);
  }
}

export async function searchWisconsinSunshineCommittees(
  input: WisconsinSunshineCommitteeSearchInput,
  options: WisconsinSunshineClientOptions = {}
): Promise<WisconsinSunshineCommittee[]> {
  const payload = await fetchWisconsinSunshineTrpcJson(
    "publicFrontendApi.getCommittees",
    buildWisconsinSunshineCommitteeSearchInput(input),
    options
  );
  const rows = parseTransactionRows(payload);
  return rows.map(parseCommittee).filter((row): row is WisconsinSunshineCommittee => row !== null);
}

async function fetchContributionRows(
  input: WisconsinSunshineCommitteeTransactionInput,
  options: WisconsinSunshineClientOptions = {}
): Promise<Record<string, unknown>[]> {
  return await fetchWisconsinSunshinePagedRowsWithDateSplit(
    "publicFrontendApi.getTransactions",
    buildWisconsinSunshineContributionTransactionInput(input, { take: normalizePageLimit(options.pageLimit), skip: 0 }),
    options
  );
}

export async function getWisconsinSunshineDirectOccupationAggregates(
  input: WisconsinSunshineCommitteeTransactionInput,
  options: WisconsinSunshineClientOptions = {}
): Promise<WisconsinSunshineAggregate[]> {
  const limit = normalizeLimit(input.limit, 10);
  const rows = await fetchContributionRows(input, options);
  const aggregates = new Map<string, AggregateAccumulator>();
  for (const row of rows) {
    if (!isIndividualContributor(row)) {
      continue;
    }
    addAggregate(
      aggregates,
      normalizeCategoryText(getString(row, "fromOccupationTitle")),
      getNumber(row, "amount"),
      WISCONSIN_SUNSHINE_TRANSACTIONS_URL
    );
  }
  return sortAggregates(aggregates, limit);
}

export async function getWisconsinSunshineContributionSizeAggregates(
  input: WisconsinSunshineCommitteeTransactionInput,
  options: WisconsinSunshineClientOptions = {}
): Promise<WisconsinSunshineAggregate[]> {
  const limit = normalizeLimit(input.limit, 10);
  const rows = await fetchContributionRows(input, options);
  const aggregates = new Map<string, AggregateAccumulator>();
  for (const row of rows) {
    if (!isIndividualContributor(row)) {
      continue;
    }
    const amount = getNumber(row, "amount");
    addAggregate(
      aggregates,
      amount != null && amount > 0 ? contributionSizeBucket(amount) : undefined,
      amount,
      WISCONSIN_SUNSHINE_TRANSACTIONS_URL
    );
  }
  return sortAggregates(aggregates, limit);
}

export async function getWisconsinSunshineIndependentExpenditureGroups(
  input: WisconsinSunshineIndependentSpendingGroupInput,
  options: WisconsinSunshineClientOptions = {}
): Promise<WisconsinSunshineIndependentSpendingGroup[]> {
  const limit = normalizeLimit(input.limit, 10);
  const candidateName = normalizeNonEmptyString(input.candidateCommitteeName, "candidateCommitteeName");
  const office = optionalString(input.office);
  const district = optionalString(input.district);
  const rows = await fetchWisconsinSunshinePagedRowsWithDateSplit(
    "publicFrontendApi.getTransactions",
    buildWisconsinSunshineIndependentExpenditureTransactionInput(input, {
      take: normalizePageLimit(options.pageLimit),
      skip: 0,
    }),
    options
  );
  const groups = new Map<string, WisconsinSunshineIndependentSpendingGroup>();

  for (const row of rows) {
    const amount = getNumber(row, "amount");
    const supportOppose = supportOpposeFromSunshine(getString(row, "supportStance"));
    const relatedEntityName = getNestedString(row, "relatedEntity", "name");
    const relatedOfficeName = getNestedString(row, "relatedOffice", "name");
    const relatedDistrictName = getNestedString(row, "relatedDistrict", "name");
    if (!amount || amount <= 0 || !supportOppose) {
      continue;
    }
    if (normalizeMatchText(relatedEntityName) !== normalizeMatchText(candidateName)) {
      continue;
    }
    if (office && normalizeMatchText(relatedOfficeName) !== normalizeMatchText(office)) {
      continue;
    }
    if (district && normalizeMatchText(relatedDistrictName) !== normalizeMatchText(district)) {
      continue;
    }

    const sponsor = getObject(row, "createdByEntity") ?? getObject(row, "from_entity");
    const sponsorId = sponsor ? getString(sponsor, "id") : undefined;
    const sponsorName = sponsor ? getString(sponsor, "name") : undefined;
    if (!sponsorId || !sponsorName) {
      continue;
    }
    const key = `${sponsorId}:${supportOppose}`;
    const existing = groups.get(key) ?? {
      sponsorId,
      sponsorName,
      supportOppose,
      amount: 0,
      expenditureCount: 0,
      sourceUrl: WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
    };
    existing.amount = roundCurrency(existing.amount + amount);
    existing.expenditureCount += 1;
    groups.set(key, existing);
  }

  return Array.from(groups.values())
    .sort((left, right) => right.amount - left.amount || left.sponsorName.localeCompare(right.sponsorName))
    .slice(0, limit);
}

export async function getWisconsinSunshineOutsideSpenderOrganizationFunders(
  input: WisconsinSunshineOutsideSpenderFundersInput,
  options: WisconsinSunshineClientOptions = {}
): Promise<WisconsinSunshineAggregate[]> {
  const limit = normalizeLimit(input.limit, 20);
  const rows = await fetchContributionRows(input, options);
  const aggregates = new Map<string, AggregateAccumulator>();
  for (const row of rows) {
    if (!isOrganizationContributor(row)) {
      continue;
    }
    const contributor = getObject(row, "from_entity");
    addAggregate(aggregates, normalizeCategoryText(getString(contributor ?? {}, "name")), getNumber(row, "amount"), WISCONSIN_SUNSHINE_TRANSACTIONS_URL);
  }
  return sortAggregates(aggregates, limit);
}

export async function getWisconsinSunshineOffices(
  options: WisconsinSunshineClientOptions = {}
): Promise<WisconsinSunshineOffice[]> {
  const payload = await fetchWisconsinSunshineTrpcJson("office.getOffices", { take: 200 }, options);
  const rows = Array.isArray(payload) ? payload.filter(isRecord) : parseTransactionRows(payload);
  return rows
    .map((row) => {
      const id = getString(row, "id");
      const name = getString(row, "name");
      if (!id || !name) {
        return null;
      }
      return { id, name, isActive: getBoolean(row, "isActive", "active") };
    })
    .filter((row): row is WisconsinSunshineOffice => row !== null);
}

export async function getWisconsinSunshineTransactionCategories(
  options: WisconsinSunshineClientOptions = {}
): Promise<WisconsinSunshineTransactionCategory[]> {
  const payload = await fetchWisconsinSunshineTrpcJson("transactionMeta.getTransactionCategories", {}, options);
  const rows = Array.isArray(payload) ? payload.filter(isRecord) : parseTransactionRows(payload);
  return rows
    .map((row) => {
      const id = getNumber(row, "id");
      const label = getString(row, "label", "name");
      if (!Number.isInteger(id) || !label) {
        return null;
      }
      return {
        id,
        ...(getString(row, "code") ? { code: getString(row, "code") } : {}),
        label,
        requiresRelatedEntity: getBoolean(row, "requiresRelatedEntity"),
        requiresSupportStance: getBoolean(row, "requiresSupportStance"),
        requiresOffice: getBoolean(row, "requiresOffice"),
      };
    })
    .filter((row): row is WisconsinSunshineTransactionCategory => row !== null);
}
