export const CALIFORNIA_POWER_SEARCH_BASE_URL = "https://powersearch.sos.ca.gov";
export const CALIFORNIA_POWER_SEARCH_IE_BASE_URL = "https://powersearch.sos.ca.gov:3000";

const DEFAULT_TIMEOUT_MS = 30_000;

export type CaliforniaPowerSearchClientOptions = {
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
};

export type CaliforniaPowerSearchErrorCode = "invalid_request" | "network_error" | "http_error" | "bad_response";

export class CaliforniaPowerSearchClientError extends Error {
  constructor(
    public readonly code: CaliforniaPowerSearchErrorCode,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "CaliforniaPowerSearchClientError";
  }
}

export type CaliforniaSupportOppose = "support" | "oppose";

export type CaliforniaIndependentExpenditure = {
  candidateName: string;
  candidateOffice?: string;
  expenderId: string;
  expenderName: string;
  supportOppose: CaliforniaSupportOppose;
  amount: number;
  expenditureDate?: string;
  description?: string;
  payeeName?: string;
  sourceUrl: string;
};

export type CaliforniaIndependentSpendingGroup = {
  expenderId: string;
  expenderName: string;
  supportOppose: CaliforniaSupportOppose;
  amount: number;
  count: number;
  sourceUrl: string;
};

export type CaliforniaIndependentExpenditureSearchResult = {
  candidateName: string;
  electionCycle: number;
  reportedRowCount: number | null;
  expenditures: CaliforniaIndependentExpenditure[];
  sourceUrl: string;
};

export type CaliforniaIndependentSpendingSummary = {
  candidateName: string;
  electionCycle: number;
  supportTotal: number;
  opposeTotal: number;
  groups: CaliforniaIndependentSpendingGroup[];
  sourceUrl: string;
};

export type CaliforniaCandidateContributionSearchInput = {
  candidateName: string;
  electionYear: number;
  officeName?: string;
  contributorState?: string;
};

export type CaliforniaCandidateContributionSearchRequest = {
  url: string;
  method: "POST";
  headers: Record<string, string>;
  body: URLSearchParams;
  sourceUrl: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeCandidateName(value: string): string {
  const normalized = value.trim().replace(/\s+/g, " ");
  if (normalized.length === 0) {
    throw new CaliforniaPowerSearchClientError("invalid_request", "California candidate name is required");
  }
  return normalized;
}

function candidateNameMatchKeys(value: string): Set<string> {
  const compact = normalizeCandidateName(value)
    .toUpperCase()
    .replace(/[.'"]/g, "")
    .replace(/[^A-Z0-9, ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const keys = new Set<string>([compact.replace(/,/g, "").replace(/\s+/g, " ").trim()]);
  const commaParts = compact
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);

  if (commaParts.length === 2) {
    keys.add(`${commaParts[1]} ${commaParts[0]}`.replace(/\s+/g, " ").trim());
  }

  return keys;
}

function independentExpenditureSearchNames(value: string): string[] {
  const normalized = normalizeCandidateName(value);
  const names = [normalized];
  if (normalized.includes(",")) {
    return names;
  }

  const parts = normalized.split(" ").filter(Boolean);
  if (parts.length >= 2) {
    const lastName = parts[parts.length - 1];
    const firstNames = parts.slice(0, -1).join(" ");
    names.push(`${lastName}, ${firstNames}`);
  }

  return names;
}

function candidateNamesMatch(left: string, right: string): boolean {
  const leftKeys = candidateNameMatchKeys(left);
  for (const key of candidateNameMatchKeys(right)) {
    if (leftKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2001 || value > 2100) {
    throw new CaliforniaPowerSearchClientError("invalid_request", `Invalid California election year: ${value}`);
  }
  return value;
}

export function toCaliforniaElectionCycle(electionYear: number): number {
  const normalized = normalizeElectionYear(electionYear);
  return normalized % 2 === 0 ? normalized - 1 : normalized;
}

function normalizeOfficeName(value: string | undefined): string {
  const normalized = value?.trim().replace(/\s+/g, " ");
  return normalized && normalized.length > 0 ? normalized : "All Offices";
}

function normalizeContributorState(value: string | undefined): string {
  const normalized = value?.trim().toUpperCase();
  if (!normalized || normalized.length === 0) {
    return "ALL";
  }
  if (normalized !== "ALL" && !/^[A-Z]{2}$/.test(normalized)) {
    throw new CaliforniaPowerSearchClientError("invalid_request", `Invalid contributor state: ${value}`);
  }
  return normalized;
}

function getString(row: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
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

function supportOpposeFromCaliforniaPosition(value: string | undefined): CaliforniaSupportOppose | null {
  const normalized = value?.trim().toUpperCase();
  if (normalized === "S" || normalized === "SUPPORT") {
    return "support";
  }
  if (normalized === "O" || normalized === "OPPOSE") {
    return "oppose";
  }
  return null;
}

function formatDateOnly(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  const dateOnly = value.trim().slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(dateOnly) ? dateOnly : undefined;
}

export function buildCaliforniaIndependentExpenditureSearchUrl(input: {
  candidateName: string;
  electionYear: number;
}): string {
  const candidateName = normalizeCandidateName(input.candidateName);
  const electionCycle = toCaliforniaElectionCycle(input.electionYear);
  const url = new URL("/ie/search", CALIFORNIA_POWER_SEARCH_IE_BASE_URL);
  url.searchParams.set("candidatename", candidateName);
  url.searchParams.set("electioncycle", String(electionCycle));
  return url.toString();
}

export function buildCaliforniaCandidateContributionSearchRequest(
  input: CaliforniaCandidateContributionSearchInput
): CaliforniaCandidateContributionSearchRequest {
  const candidateName = normalizeCandidateName(input.candidateName);
  const electionCycle = toCaliforniaElectionCycle(input.electionYear);
  const url = new URL("/advanced.php", CALIFORNIA_POWER_SEARCH_BASE_URL);
  const body = new URLSearchParams();

  body.set("contrib_select", "all");
  body.set("state_list", normalizeContributorState(input.contributorState));
  body.set("contrib_types", "search_candidates");
  body.set("search_candidates", candidateName);
  body.set("match_candidate", "no");
  body.set("office_list", normalizeOfficeName(input.officeName));
  body.set("date_select", "cycle");
  body.append("cycles[]", String(electionCycle));
  body.set("search_btn", "Search");

  return {
    url: url.toString(),
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      accept: "text/html,application/xhtml+xml",
    },
    body,
    sourceUrl: url.toString(),
  };
}

function parseIndependentExpenditureRow(row: unknown, sourceUrl: string): CaliforniaIndependentExpenditure | null {
  if (!isRecord(row)) {
    return null;
  }

  const candidateName = getString(row, "TargetCandidateName", "targetCandidateName");
  const expenderId = getString(row, "ExpenderID", "expenderId");
  const expenderName = getString(row, "ExpenderName", "expenderName");
  const supportOppose = supportOpposeFromCaliforniaPosition(getString(row, "ExpenderPosition", "expenderPosition"));
  const amount = getNumber(row, "Amount", "amount");

  if (!candidateName || !expenderId || !expenderName || !supportOppose || amount === undefined || amount < 0) {
    return null;
  }

  return {
    candidateName,
    ...(getString(row, "TargetCandidateOffice", "targetCandidateOffice")
      ? { candidateOffice: getString(row, "TargetCandidateOffice", "targetCandidateOffice") }
      : {}),
    expenderId,
    expenderName,
    supportOppose,
    amount,
    ...(formatDateOnly(getString(row, "DateRange", "DateStart", "dateRange", "dateStart"))
      ? { expenditureDate: formatDateOnly(getString(row, "DateRange", "DateStart", "dateRange", "dateStart")) }
      : {}),
    ...(getString(row, "ExpenditureDscr", "expenditureDescription")
      ? { description: getString(row, "ExpenditureDscr", "expenditureDescription") }
      : {}),
    ...(getString(row, "PayeeName", "payeeName") ? { payeeName: getString(row, "PayeeName", "payeeName") } : {}),
    sourceUrl,
  };
}

function parseIndependentExpenditurePayload(
  payload: unknown,
  input: { candidateName: string; electionYear: number; sourceUrl: string }
): CaliforniaIndependentExpenditureSearchResult {
  if (!isRecord(payload) || !Array.isArray(payload.payload)) {
    throw new CaliforniaPowerSearchClientError("bad_response", "California Power Search IE response is missing payload array");
  }

  const candidateName = normalizeCandidateName(input.candidateName);
  const electionCycle = toCaliforniaElectionCycle(input.electionYear);
  const reportedRowCount = getNumber(payload, "amount");
  const expenditures = payload.payload
    .map((row) => parseIndependentExpenditureRow(row, input.sourceUrl))
    .filter(
      (row): row is CaliforniaIndependentExpenditure =>
        row !== null && candidateNamesMatch(row.candidateName, candidateName)
    );

  return {
    candidateName,
    electionCycle,
    reportedRowCount: reportedRowCount === undefined ? null : Math.trunc(reportedRowCount),
    expenditures,
    sourceUrl: input.sourceUrl,
  };
}

async function fetchJson(url: string, options: CaliforniaPowerSearchClientOptions): Promise<unknown> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetchImpl(url, {
      headers: {
        accept: "application/json,text/plain;q=0.9,*/*;q=0.1",
      },
      signal: controller.signal,
    });

    if (!response.ok) {
      throw new CaliforniaPowerSearchClientError(
        "http_error",
        `California Power Search request failed: ${response.status} ${response.statusText}`,
        response.status
      );
    }

    return await response.json();
  } catch (error) {
    if (error instanceof CaliforniaPowerSearchClientError) {
      throw error;
    }
    if (error instanceof Error && error.name === "AbortError") {
      throw new CaliforniaPowerSearchClientError("network_error", `California Power Search request timed out: ${url}`);
    }
    throw new CaliforniaPowerSearchClientError(
      "network_error",
      `California Power Search request failed: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    clearTimeout(timeout);
  }
}

export async function searchCaliforniaIndependentExpenditures(
  input: { candidateName: string; electionYear: number },
  options: CaliforniaPowerSearchClientOptions = {}
): Promise<CaliforniaIndependentExpenditureSearchResult> {
  let emptyResult: CaliforniaIndependentExpenditureSearchResult | null = null;

  for (const candidateName of independentExpenditureSearchNames(input.candidateName)) {
    const sourceUrl = buildCaliforniaIndependentExpenditureSearchUrl({
      ...input,
      candidateName,
    });
    const payload = await fetchJson(sourceUrl, options);
    const parsed = parseIndependentExpenditurePayload(payload, {
      ...input,
      sourceUrl,
    });
    if (parsed.expenditures.length > 0) {
      return parsed;
    }
    emptyResult ??= parsed;
  }

  return emptyResult!;
}

export async function summarizeCaliforniaIndependentSpendingByCandidate(
  input: { candidateName: string; electionYear: number },
  options: CaliforniaPowerSearchClientOptions = {}
): Promise<CaliforniaIndependentSpendingSummary> {
  const result = await searchCaliforniaIndependentExpenditures(input, options);
  const groupsByKey = new Map<string, CaliforniaIndependentSpendingGroup>();
  let supportTotal = 0;
  let opposeTotal = 0;

  for (const expenditure of result.expenditures) {
    if (expenditure.supportOppose === "support") {
      supportTotal += expenditure.amount;
    } else {
      opposeTotal += expenditure.amount;
    }

    const key = `${expenditure.expenderId}\u0000${expenditure.expenderName}\u0000${expenditure.supportOppose}`;
    const existing = groupsByKey.get(key);
    if (!existing) {
      groupsByKey.set(key, {
        expenderId: expenditure.expenderId,
        expenderName: expenditure.expenderName,
        supportOppose: expenditure.supportOppose,
        amount: expenditure.amount,
        count: 1,
        sourceUrl: result.sourceUrl,
      });
      continue;
    }
    groupsByKey.set(key, {
      ...existing,
      amount: existing.amount + expenditure.amount,
      count: existing.count + 1,
    });
  }

  return {
    candidateName: result.candidateName,
    electionCycle: result.electionCycle,
    supportTotal,
    opposeTotal,
    groups: [...groupsByKey.values()].sort((left, right) => right.amount - left.amount),
    sourceUrl: result.sourceUrl,
  };
}
