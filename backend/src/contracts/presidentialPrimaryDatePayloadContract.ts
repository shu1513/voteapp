import { getStateNameByFips } from "../constants/usStates.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type PresidentialPrimaryDateResultStatus = "official_found" | "not_official_yet";

export type PresidentialPrimaryDatePayloadRow = {
  state_fips: string;
  state_name: string;
  status: PresidentialPrimaryDateResultStatus;
  primary_date: string | null;
  sources: string[];
  notes: string;
};

export type PresidentialPrimaryDatePayload = {
  results: PresidentialPrimaryDatePayloadRow[];
};

export type PresidentialPrimaryDatePayloadRowFailure = {
  state_fips: string;
  reason: string;
};

export type PresidentialPrimaryDatePayloadPartialParseResult = {
  payload: PresidentialPrimaryDatePayload;
  failedRows: PresidentialPrimaryDatePayloadRowFailure[];
  ignoredRowReasons: string[];
  reviewFeedbackLines: string[];
  reason: string | null;
};

type ParseOptions = {
  electionYear: number;
  expectedStateFips: readonly string[];
};

const STATUS_SET = new Set<string>(["official_found", "not_official_yet"]);

function assertPresidentialElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100 || electionYear % 4 !== 0) {
    throw new Error(`Invalid presidential primary date election year: ${electionYear}`);
  }
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeStatus(value: unknown): PresidentialPrimaryDateResultStatus | null {
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return STATUS_SET.has(normalized) ? (normalized as PresidentialPrimaryDateResultStatus) : null;
}

function normalizeStateFips(value: unknown): string | null {
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim();
  return /^[0-9]{2}$/.test(normalized) ? normalized : null;
}

function normalizeSources(value: unknown): string[] | null {
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }
  const sources: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") {
      return null;
    }
    const url = normalizeHttpUrl(item);
    if (!url) {
      return null;
    }
    if (seen.has(url)) {
      continue;
    }
    seen.add(url);
    sources.push(url);
  }
  return sources.length > 0 ? sources : null;
}

function normalizeNotes(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function parsePrimaryDate(value: unknown, electionYear: number): string | null {
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    return null;
  }
  if (!normalized.startsWith(`${electionYear}-`)) {
    return null;
  }
  const date = new Date(`${normalized}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== normalized) {
    return null;
  }
  return normalized;
}

function normalizeExpectedStateFips(values: readonly string[]): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const fips = normalizeStateFips(value);
    if (!fips) {
      throw new Error(`Invalid expected presidential primary date state_fips: ${value}`);
    }
    getStateNameByFips(fips);
    if (seen.has(fips)) {
      continue;
    }
    seen.add(fips);
    normalized.push(fips);
  }
  if (normalized.length === 0) {
    throw new Error("Presidential primary date payload parser requires at least one expected state_fips");
  }
  return normalized;
}

function parseRow(
  value: unknown,
  electionYear: number,
  expectedFipsSet: ReadonlySet<string>
): { ok: true; row: PresidentialPrimaryDatePayloadRow } | { ok: false; reason: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ok: false, reason: "results entries must be objects" };
  }

  const input = value as Record<string, unknown>;
  const stateFips = normalizeStateFips(input.state_fips);
  if (!stateFips) {
    return { ok: false, reason: "state_fips must be exactly two digits" };
  }
  if (!expectedFipsSet.has(stateFips)) {
    return { ok: false, reason: `state_fips is outside provided states: ${stateFips}` };
  }

  const expectedStateName = getStateNameByFips(stateFips);
  if (!isNonEmptyString(input.state_name) || input.state_name.trim() !== expectedStateName) {
    return { ok: false, reason: `state_name does not match state_fips ${stateFips}` };
  }

  const status = normalizeStatus(input.status);
  if (!status) {
    return { ok: false, reason: "status must be official_found or not_official_yet" };
  }

  const sources = normalizeSources(input.sources);
  if (!sources) {
    return { ok: false, reason: "sources must be a non-empty array of valid http(s) URLs" };
  }

  const primaryDate = parsePrimaryDate(input.primary_date, electionYear);
  if (status === "official_found" && !primaryDate) {
    return { ok: false, reason: "official_found requires primary_date in election_year" };
  }
  if (status === "not_official_yet" && input.primary_date !== null) {
    return { ok: false, reason: "not_official_yet requires primary_date null" };
  }

  return {
    ok: true,
    row: {
      state_fips: stateFips,
      state_name: expectedStateName,
      status,
      primary_date: status === "official_found" ? primaryDate : null,
      sources,
      notes: normalizeNotes(input.notes),
    },
  };
}

function rowStateFips(value: unknown): string | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  return normalizeStateFips((value as Record<string, unknown>).state_fips);
}

function makePartialReviewFeedback(
  failedRows: readonly PresidentialPrimaryDatePayloadRowFailure[],
  ignoredRowReasons: readonly string[]
): string[] {
  const lines: string[] = [];
  if (failedRows.length > 0) {
    lines.push("Some presidential primary date rows were invalid or missing.");
    lines.push(
      ...failedRows.map((failure) => `Fix state_fips=${failure.state_fips}: ${failure.reason}`)
    );
  }
  if (ignoredRowReasons.length > 0) {
    lines.push("Some returned rows were ignored because they did not map to a provided state.");
    lines.push(...ignoredRowReasons);
  }
  return lines;
}

export function parsePresidentialPrimaryDatePayloadPartial(
  payload: unknown,
  options: ParseOptions
): PresidentialPrimaryDatePayloadPartialParseResult {
  assertPresidentialElectionYear(options.electionYear);
  const expectedStateFips = normalizeExpectedStateFips(options.expectedStateFips);
  const expectedFipsSet = new Set(expectedStateFips);

  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    const failedRows = expectedStateFips.map((stateFips) => ({
      state_fips: stateFips,
      reason: "payload must be an object",
    }));
    return {
      payload: { results: [] },
      failedRows,
      ignoredRowReasons: [],
      reviewFeedbackLines: makePartialReviewFeedback(failedRows, []),
      reason: "payload must be an object",
    };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.results)) {
    const failedRows = expectedStateFips.map((stateFips) => ({
      state_fips: stateFips,
      reason: "payload.results must be array",
    }));
    return {
      payload: { results: [] },
      failedRows,
      ignoredRowReasons: [],
      reviewFeedbackLines: makePartialReviewFeedback(failedRows, []),
      reason: "payload.results must be array",
    };
  }

  const validRowsByFips = new Map<string, PresidentialPrimaryDatePayloadRow>();
  const failedRowsByFips = new Map<string, PresidentialPrimaryDatePayloadRowFailure>();
  const ignoredRowReasons: string[] = [];

  for (const value of input.results) {
    const stateFips = rowStateFips(value);
    if (!stateFips) {
      ignoredRowReasons.push("Ignored row without valid two-digit state_fips");
      continue;
    }
    if (!expectedFipsSet.has(stateFips)) {
      ignoredRowReasons.push(`Ignored row with state_fips outside provided states: ${stateFips}`);
      continue;
    }
    if (failedRowsByFips.has(stateFips)) {
      continue;
    }

    const parsed = parseRow(value, options.electionYear, expectedFipsSet);
    if (!parsed.ok) {
      failedRowsByFips.set(stateFips, {
        state_fips: stateFips,
        reason: parsed.reason,
      });
      validRowsByFips.delete(stateFips);
      continue;
    }
    if (validRowsByFips.has(stateFips)) {
      failedRowsByFips.set(stateFips, {
        state_fips: stateFips,
        reason: `results contains duplicate state_fips: ${stateFips}`,
      });
      validRowsByFips.delete(stateFips);
      continue;
    }

    validRowsByFips.set(stateFips, parsed.row);
  }

  for (const stateFips of expectedStateFips) {
    if (!validRowsByFips.has(stateFips) && !failedRowsByFips.has(stateFips)) {
      failedRowsByFips.set(stateFips, {
        state_fips: stateFips,
        reason: `results missing state_fips: ${stateFips}`,
      });
    }
  }

  const rows = expectedStateFips.flatMap((stateFips) => {
    const row = validRowsByFips.get(stateFips);
    return row ? [row] : [];
  });
  const failedRows = expectedStateFips.flatMap((stateFips) => {
    const failure = failedRowsByFips.get(stateFips);
    return failure ? [failure] : [];
  });
  const reviewFeedbackLines = makePartialReviewFeedback(failedRows, ignoredRowReasons);

  return {
    payload: { results: rows },
    failedRows,
    ignoredRowReasons,
    reviewFeedbackLines,
    reason:
      failedRows.length > 0
        ? `presidential primary date payload has ${failedRows.length} invalid or missing row(s)`
        : ignoredRowReasons.length > 0
        ? `presidential primary date payload had ${ignoredRowReasons.length} ignored row(s)`
        : null,
  };
}

export function parsePresidentialPrimaryDatePayload(
  payload: unknown,
  options: ParseOptions
):
  | { ok: true; payload: PresidentialPrimaryDatePayload }
  | { ok: false; reason: string } {
  assertPresidentialElectionYear(options.electionYear);

  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.results)) {
    return { ok: false, reason: "payload.results must be array" };
  }

  const expectedStateFips = normalizeExpectedStateFips(options.expectedStateFips);
  const expectedFipsSet = new Set(expectedStateFips);
  const seen = new Set<string>();
  const rows: PresidentialPrimaryDatePayloadRow[] = [];

  for (const value of input.results) {
    const parsed = parseRow(value, options.electionYear, expectedFipsSet);
    if (!parsed.ok) {
      return parsed;
    }
    if (seen.has(parsed.row.state_fips)) {
      return { ok: false, reason: `results contains duplicate state_fips: ${parsed.row.state_fips}` };
    }
    seen.add(parsed.row.state_fips);
    rows.push(parsed.row);
  }

  const missingStateFips = expectedStateFips.filter((stateFips) => !seen.has(stateFips));
  if (missingStateFips.length > 0) {
    return { ok: false, reason: `results missing state_fips: ${missingStateFips.join(", ")}` };
  }

  return {
    ok: true,
    payload: {
      results: rows.sort((a, b) => a.state_fips.localeCompare(b.state_fips)),
    },
  };
}
