import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type PresidentialRosterStatus = "active" | "withdrawn" | "unknown";

export type PresidentialRosterStatusCandidate = {
  candidate_id: string;
  status: PresidentialRosterStatus;
  sources: string[];
  notes: string;
};

export type PresidentialRosterStatusPayload = {
  candidates: PresidentialRosterStatusCandidate[];
};

export type PresidentialRosterStatusPayloadParseOptions = {
  expectedCandidateIds: readonly string[];
};

const STATUS_SET = new Set<string>(["active", "withdrawn", "unknown"]);

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeStatus(value: unknown): PresidentialRosterStatus | null {
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return STATUS_SET.has(normalized) ? (normalized as PresidentialRosterStatus) : null;
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
    if (seen.has(url)) {
      continue;
    }
    seen.add(url);
    normalized.push(url);
  }

  return normalized.length > 0 ? normalized : null;
}

function normalizeExpectedCandidateIds(values: readonly string[]): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const candidateId = value.trim();
    if (candidateId.length === 0 || seen.has(candidateId)) {
      continue;
    }
    seen.add(candidateId);
    normalized.push(candidateId);
  }
  return normalized;
}

function parseCandidate(
  value: unknown,
  allowedCandidateIds: ReadonlySet<string>
): { ok: true; candidate: PresidentialRosterStatusCandidate } | { ok: false; reason: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ok: false, reason: "payload.candidates contains invalid row" };
  }

  const input = value as Record<string, unknown>;
  if (!isNonEmptyString(input.candidate_id)) {
    return { ok: false, reason: "candidate.candidate_id must be non-empty string" };
  }
  const candidateId = input.candidate_id.trim();
  if (!allowedCandidateIds.has(candidateId)) {
    return { ok: false, reason: `candidate_id ${candidateId} was not provided for verification` };
  }

  const status = normalizeStatus(input.status);
  if (!status) {
    return { ok: false, reason: "candidate.status must be active, withdrawn, or unknown" };
  }

  const sources = normalizeSources(input.sources);
  if (!sources) {
    return { ok: false, reason: "candidate.sources must contain valid URL strings" };
  }

  if (!isNonEmptyString(input.notes)) {
    return { ok: false, reason: "candidate.notes must be non-empty string" };
  }

  return {
    ok: true,
    candidate: {
      candidate_id: candidateId,
      status,
      sources,
      notes: input.notes.trim(),
    },
  };
}

export function parsePresidentialRosterStatusPayload(
  payload: unknown,
  options: PresidentialRosterStatusPayloadParseOptions
):
  | { ok: true; payload: PresidentialRosterStatusPayload }
  | { ok: false; reason: string } {
  const expectedCandidateIds = normalizeExpectedCandidateIds(options.expectedCandidateIds);
  if (expectedCandidateIds.length === 0) {
    return { ok: false, reason: "expectedCandidateIds must contain at least one candidate ID" };
  }

  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.candidates)) {
    return { ok: false, reason: "payload.candidates must be array" };
  }

  const allowedCandidateIds = new Set(expectedCandidateIds);
  const byCandidateId = new Map<string, PresidentialRosterStatusCandidate>();
  for (const row of input.candidates) {
    const parsed = parseCandidate(row, allowedCandidateIds);
    if (!parsed.ok) {
      return { ok: false, reason: parsed.reason };
    }
    if (byCandidateId.has(parsed.candidate.candidate_id)) {
      return { ok: false, reason: `duplicate candidate_id ${parsed.candidate.candidate_id}` };
    }
    byCandidateId.set(parsed.candidate.candidate_id, parsed.candidate);
  }

  const missing = expectedCandidateIds.filter((candidateId) => !byCandidateId.has(candidateId));
  if (missing.length > 0) {
    return { ok: false, reason: `payload is missing candidate_id rows: ${missing.join(", ")}` };
  }

  return {
    ok: true,
    payload: {
      candidates: expectedCandidateIds.map((candidateId) => byCandidateId.get(candidateId)!),
    },
  };
}
