export const OPEN_FEC_API_BASE_URL = "https://api.open.fec.gov/v1";
export const DEFAULT_OPEN_FEC_TIMEOUT_MS = 30_000;
export const DEFAULT_OPEN_FEC_PER_PAGE = 20;
export const MAX_OPEN_FEC_PER_PAGE = 100;

const OPEN_FEC_TIMEOUT_RETRY_DELAY_MS = 500;

export type OpenFecClientErrorCode =
  | "configuration_error"
  | "invalid_request"
  | "http_error"
  | "bad_response"
  | "timeout";

export class OpenFecClientError extends Error {
  readonly code: OpenFecClientErrorCode;

  constructor(code: OpenFecClientErrorCode, message: string) {
    super(message);
    this.name = "OpenFecClientError";
    this.code = code;
  }
}

export type OpenFecPrincipalCommittee = {
  committeeId: string;
  name: string;
  designation?: string;
  organizationType?: string;
  state?: string;
};

export type OpenFecPresidentialCandidate = {
  fecCandidateId: string;
  name: string;
  party?: string;
  partyFull?: string;
  office?: string;
  officeFull?: string;
  electionYears: number[];
  firstFileDate?: string;
  lastFileDate?: string;
  principalCommittees: OpenFecPrincipalCommittee[];
  fecCandidateUrl: string;
};

export type OpenFecClientOptions = {
  apiKeys: readonly string[];
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
};

export type OpenFecCandidateSearchInput = {
  electionYear: number;
  name: string;
  partyCode?: string;
  perPage?: number;
};

const OPEN_FEC_HOSTNAME = "api.open.fec.gov";

function truncate(text: string, max = 500): string {
  const trimmed = text.trim();
  if (trimmed.length <= max) {
    return trimmed;
  }
  return `${trimmed.slice(0, max)}...`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function assertPresidentialElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100 || electionYear % 4 !== 0) {
    throw new OpenFecClientError("invalid_request", `Invalid presidential election year: ${electionYear}`);
  }
}

function normalizeNonEmpty(value: string, fieldName: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new OpenFecClientError("invalid_request", `${fieldName} must not be empty`);
  }
  return normalized;
}

function normalizeFecCandidateId(value: string): string {
  const normalized = normalizeNonEmpty(value, "fecCandidateId").toUpperCase();
  if (!/^P\d{8}$/.test(normalized)) {
    throw new OpenFecClientError("invalid_request", `Invalid presidential FEC candidate ID: ${value}`);
  }
  return normalized;
}

function normalizePartyCode(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const normalized = value.trim().toUpperCase();
  if (!/^[A-Z]{3}$/.test(normalized)) {
    throw new OpenFecClientError("invalid_request", `Invalid OpenFEC party code: ${value}`);
  }
  return normalized;
}

function normalizePerPage(value: number | undefined): number {
  const normalized = value ?? DEFAULT_OPEN_FEC_PER_PAGE;
  if (!Number.isInteger(normalized) || normalized <= 0 || normalized > MAX_OPEN_FEC_PER_PAGE) {
    throw new OpenFecClientError(
      "invalid_request",
      `OpenFEC perPage must be an integer between 1 and ${MAX_OPEN_FEC_PER_PAGE}`
    );
  }
  return normalized;
}

function normalizeTimeoutMs(value: number | undefined): number {
  const normalized = value ?? DEFAULT_OPEN_FEC_TIMEOUT_MS;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new OpenFecClientError("invalid_request", `OpenFEC timeoutMs must be a positive integer`);
  }
  return normalized;
}

function normalizeApiKeys(apiKeys: readonly string[]): string[] {
  const normalized = apiKeys.map((key) => key.trim()).filter((key) => key.length > 0);
  const unique = [...new Set(normalized)];
  if (unique.length === 0) {
    throw new OpenFecClientError(
      "configuration_error",
      "No OpenFEC API keys configured. Set FEC_API_KEY_1 (and optionally FEC_API_KEY_2/FEC_API_KEY_3)."
    );
  }
  return unique;
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

function getNumberArray(row: Record<string, unknown>, ...keys: string[]): number[] {
  const values: number[] = [];
  const seen = new Set<number>();
  for (const key of keys) {
    const raw = row[key];
    if (!Array.isArray(raw)) {
      continue;
    }
    for (const item of raw) {
      const parsed = typeof item === "number" ? item : Number.parseInt(String(item), 10);
      if (!Number.isInteger(parsed) || seen.has(parsed)) {
        continue;
      }
      seen.add(parsed);
      values.push(parsed);
    }
  }
  return values.sort((left, right) => left - right);
}

function parsePrincipalCommittees(raw: unknown): OpenFecPrincipalCommittee[] {
  if (!Array.isArray(raw)) {
    return [];
  }

  const committees: OpenFecPrincipalCommittee[] = [];
  const seen = new Set<string>();
  for (const item of raw) {
    if (!isRecord(item)) {
      continue;
    }
    const committeeId = getString(item, "committee_id", "committeeId");
    const name = getString(item, "name", "committee_name", "committeeName");
    if (!committeeId || !name || seen.has(committeeId)) {
      continue;
    }
    seen.add(committeeId);
    committees.push({
      committeeId,
      name,
      ...(getString(item, "designation") ? { designation: getString(item, "designation") } : {}),
      ...(getString(item, "organization_type", "organizationType")
        ? { organizationType: getString(item, "organization_type", "organizationType") }
        : {}),
      ...(getString(item, "state") ? { state: getString(item, "state") } : {}),
    });
  }
  return committees;
}

function toFecCandidateUrl(fecCandidateId: string): string {
  return `https://www.fec.gov/data/candidate/${encodeURIComponent(fecCandidateId)}`;
}

function parseCandidateRow(row: unknown): OpenFecPresidentialCandidate | null {
  if (!isRecord(row)) {
    return null;
  }

  const fecCandidateId = getString(row, "candidate_id", "candidateId");
  const name = getString(row, "name", "candidate_name", "candidateName");
  if (!fecCandidateId || !name || !/^P\d{8}$/i.test(fecCandidateId)) {
    return null;
  }

  const office = getString(row, "office");
  if (office && office.toUpperCase() !== "P") {
    return null;
  }

  const normalizedFecCandidateId = fecCandidateId.toUpperCase();
  return {
    fecCandidateId: normalizedFecCandidateId,
    name,
    ...(getString(row, "party") ? { party: getString(row, "party") } : {}),
    ...(getString(row, "party_full", "partyFull") ? { partyFull: getString(row, "party_full", "partyFull") } : {}),
    ...(office ? { office: office.toUpperCase() } : {}),
    ...(getString(row, "office_full", "officeFull") ? { officeFull: getString(row, "office_full", "officeFull") } : {}),
    electionYears: getNumberArray(row, "election_years", "electionYears"),
    ...(getString(row, "first_file_date", "firstFileDate")
      ? { firstFileDate: getString(row, "first_file_date", "firstFileDate") }
      : {}),
    ...(getString(row, "last_file_date", "lastFileDate")
      ? { lastFileDate: getString(row, "last_file_date", "lastFileDate") }
      : {}),
    principalCommittees: parsePrincipalCommittees(row.principal_committees),
    fecCandidateUrl: toFecCandidateUrl(normalizedFecCandidateId),
  };
}

function parseCandidateResults(payload: unknown): OpenFecPresidentialCandidate[] {
  if (!isRecord(payload) || !Array.isArray(payload.results)) {
    throw new OpenFecClientError("bad_response", "OpenFEC response is missing results array");
  }
  return payload.results
    .map((row) => parseCandidateRow(row))
    .filter((row): row is OpenFecPresidentialCandidate => row !== null);
}

function shouldRotateForStatus(status: number): boolean {
  return status === 401 || status === 403 || status === 429 || status >= 500;
}

function withOpenFecApiKey(baseUrl: string, apiKey: string): string {
  const parsed = new URL(baseUrl);
  if (parsed.hostname !== OPEN_FEC_HOSTNAME) {
    throw new OpenFecClientError(
      "invalid_request",
      `Expected OpenFEC hostname ${OPEN_FEC_HOSTNAME}, got ${parsed.hostname}`
    );
  }
  parsed.searchParams.set("api_key", apiKey);
  return parsed.toString();
}

export function readOpenFecApiKeysFromEnv(env: NodeJS.ProcessEnv = process.env): string[] {
  const rawCandidates = [env.FEC_API_KEY_1, env.FEC_API_KEY_2, env.FEC_API_KEY_3, env.FEC_API_KEY];
  const normalized = rawCandidates
    .map((value) => value?.trim())
    .filter((value): value is string => Boolean(value && value.length > 0));
  return [...new Set(normalized)];
}

export function buildOpenFecPresidentialCandidateSearchUrl(input: OpenFecCandidateSearchInput): string {
  assertPresidentialElectionYear(input.electionYear);
  const name = normalizeNonEmpty(input.name, "name");
  const partyCode = normalizePartyCode(input.partyCode);
  const perPage = normalizePerPage(input.perPage);

  const url = new URL(`${OPEN_FEC_API_BASE_URL}/candidates/search/`);
  url.searchParams.set("office", "P");
  url.searchParams.set("election_year", String(input.electionYear));
  url.searchParams.set("q", name);
  url.searchParams.set("per_page", String(perPage));
  if (partyCode) {
    url.searchParams.set("party", partyCode);
  }
  return url.toString();
}

export function buildOpenFecPresidentialCandidateByIdUrl(fecCandidateId: string): string {
  const normalized = normalizeFecCandidateId(fecCandidateId);
  const url = new URL(`${OPEN_FEC_API_BASE_URL}/candidate/${encodeURIComponent(normalized)}/`);
  url.searchParams.set("office", "P");
  return url.toString();
}

async function fetchOpenFecJsonWithKeyRotationOnce(
  baseUrl: string,
  options: OpenFecClientOptions
): Promise<unknown> {
  const apiKeys = normalizeApiKeys(options.apiKeys);
  const timeoutMs = normalizeTimeoutMs(options.timeoutMs);
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  let lastError: Error | null = null;

  for (let index = 0; index < apiKeys.length; index += 1) {
    const apiKey = apiKeys[index]!;
    const attemptLabel = `key_${index + 1}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetchImpl(withOpenFecApiKey(baseUrl, apiKey), {
        method: "GET",
        redirect: "follow",
        signal: controller.signal,
      });
      const bodyText = await response.text();

      if (!response.ok) {
        const error = new OpenFecClientError(
          "http_error",
          `OpenFEC request failed via ${attemptLabel}: status=${response.status} ${response.statusText}; body=${truncate(bodyText)}`
        );
        if (shouldRotateForStatus(response.status) && index < apiKeys.length - 1) {
          lastError = error;
          continue;
        }
        throw error;
      }

      try {
        return JSON.parse(bodyText) as unknown;
      } catch {
        const error = new OpenFecClientError(
          "bad_response",
          `OpenFEC returned non-JSON via ${attemptLabel}; body=${truncate(bodyText)}`
        );
        if (index < apiKeys.length - 1) {
          lastError = error;
          continue;
        }
        throw error;
      }
    } catch (error) {
      if (isAbortError(error)) {
        throw new OpenFecClientError("timeout", `OpenFEC request timed out after ${timeoutMs}ms`);
      }
      const normalized = error instanceof Error ? error : new Error(String(error));
      if (normalized instanceof OpenFecClientError) {
        throw normalized;
      }
      if (index < apiKeys.length - 1) {
        lastError = normalized;
        continue;
      }
      throw normalized;
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError ?? new OpenFecClientError("http_error", "OpenFEC request failed across all configured keys.");
}

export async function fetchOpenFecJsonWithKeyRotation(
  baseUrl: string,
  options: OpenFecClientOptions
): Promise<unknown> {
  try {
    return await fetchOpenFecJsonWithKeyRotationOnce(baseUrl, options);
  } catch (error) {
    if (!(error instanceof OpenFecClientError) || error.code !== "timeout") {
      throw error;
    }
  }

  // A timeout is not key-specific, so retry once without consuming the next key's quota.
  await sleep(OPEN_FEC_TIMEOUT_RETRY_DELAY_MS);
  return fetchOpenFecJsonWithKeyRotationOnce(baseUrl, options);
}

export async function searchPresidentialCandidatesByName(
  input: OpenFecCandidateSearchInput,
  options: OpenFecClientOptions
): Promise<OpenFecPresidentialCandidate[]> {
  const payload = await fetchOpenFecJsonWithKeyRotation(
    buildOpenFecPresidentialCandidateSearchUrl(input),
    options
  );
  return parseCandidateResults(payload);
}

export async function getPresidentialCandidateByFecId(
  fecCandidateId: string,
  options: OpenFecClientOptions
): Promise<OpenFecPresidentialCandidate | null> {
  const payload = await fetchOpenFecJsonWithKeyRotation(
    buildOpenFecPresidentialCandidateByIdUrl(fecCandidateId),
    options
  );
  const candidates = parseCandidateResults(payload);
  return candidates[0] ?? null;
}
