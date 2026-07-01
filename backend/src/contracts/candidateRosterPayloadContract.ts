import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type CandidateRosterEntry = {
  display_name: string;
  party?: string;
  is_incumbent?: boolean;
  fec_ids?: string[];
  state_filing_ids?: string[];
  sources: string[];
};

export type CandidateRosterPayload = {
  candidates: CandidateRosterEntry[];
};

type CandidateRosterParseOptions = {
  requireFecIds?: boolean;
  allowFecIds?: boolean;
};

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeSources(value: unknown): string[] | null {
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }

  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") {
      return null;
    }
    const url = normalizeHttpUrl(item);
    if (!url) {
      return null;
    }
    if (!seen.has(url)) {
      seen.add(url);
      normalized.push(url);
    }
  }

  return normalized.length > 0 ? normalized : null;
}

function normalizeOptionalStringArray(value: unknown): string[] | null | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    return null;
  }
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (!isNonEmptyString(item)) {
      return null;
    }
    const text = item.trim();
    if (!seen.has(text)) {
      seen.add(text);
      normalized.push(text);
    }
  }
  return normalized;
}

function parseEntry(
  value: unknown,
  options: CandidateRosterParseOptions
): { ok: true; entry: CandidateRosterEntry } | { ok: false; reason: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ok: false, reason: "row must be an object" };
  }

  const input = value as Record<string, unknown>;
  if (!isNonEmptyString(input.display_name)) {
    return { ok: false, reason: "row.display_name must be a non-empty string" };
  }

  const sources = normalizeSources(input.sources);
  if (!sources) {
    return { ok: false, reason: "row.sources must contain at least one valid URL" };
  }

  let party: string | undefined;
  if (input.party !== undefined && input.party !== null) {
    if (!isNonEmptyString(input.party)) {
      return { ok: false, reason: "row.party must be a non-empty string when provided" };
    }
    party = input.party.trim();
  }

  let isIncumbent: boolean | undefined;
  if (input.is_incumbent !== undefined && input.is_incumbent !== null) {
    if (typeof input.is_incumbent !== "boolean") {
      return { ok: false, reason: "row.is_incumbent must be a boolean when provided" };
    }
    isIncumbent = input.is_incumbent;
  }

  const allowFecIds = options.allowFecIds !== false;
  const requireFecIds = options.requireFecIds === true;
  if (!allowFecIds && input.fec_ids !== undefined && input.fec_ids !== null) {
    return { ok: false, reason: "row.fec_ids is not allowed for this election context" };
  }
  const fecIds = allowFecIds ? normalizeOptionalStringArray(input.fec_ids) : undefined;
  if (allowFecIds && fecIds === null) {
    return { ok: false, reason: "row.fec_ids must be an array of non-empty strings when provided" };
  }
  const normalizedFecIds = fecIds ?? undefined;
  if (requireFecIds && (!normalizedFecIds || normalizedFecIds.length === 0)) {
    return { ok: false, reason: "row.fec_ids is required for this election context" };
  }

  const stateFilingIds = normalizeOptionalStringArray(input.state_filing_ids);
  if (stateFilingIds === null) {
    return { ok: false, reason: "row.state_filing_ids must be an array of non-empty strings when provided" };
  }
  const normalizedStateFilingIds = stateFilingIds ?? undefined;

  return {
    ok: true,
    entry: {
      display_name: input.display_name.trim(),
      ...(party ? { party } : {}),
      ...(isIncumbent !== undefined ? { is_incumbent: isIncumbent } : {}),
      ...(normalizedFecIds !== undefined ? { fec_ids: normalizedFecIds } : {}),
      ...(normalizedStateFilingIds !== undefined ? { state_filing_ids: normalizedStateFilingIds } : {}),
      sources,
    },
  };
}

export function parseCandidateRosterPayload(
  payload: unknown,
  options: CandidateRosterParseOptions = {}
):
  | { ok: true; payload: CandidateRosterPayload }
  | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.candidates)) {
    return { ok: false, reason: "payload.candidates must be array" };
  }

  const candidates: CandidateRosterEntry[] = [];
  for (const [index, row] of input.candidates.entries()) {
    const parsed = parseEntry(row, options);
    if (!parsed.ok) {
      return { ok: false, reason: `payload.candidates[${index}]: ${parsed.reason}` };
    }
    candidates.push(parsed.entry);
  }

  return { ok: true, payload: { candidates } };
}
