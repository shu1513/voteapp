import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type PresidentialRosterCandidateStatus = "active" | "withdrawn";

export type PresidentialRosterCandidate = {
  display_name: string;
  party: string;
  fec_candidate_id?: string;
  sources: string[];
  status: PresidentialRosterCandidateStatus;
};

export type PresidentialRosterPayload = {
  candidates: PresidentialRosterCandidate[];
};

export type PresidentialRosterPayloadParseOptions = {
  expectedParty?: string | null;
};

const STATUS_SET = new Set<string>(["active", "withdrawn"]);

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizePartyKey(value: string): string {
  const normalized = value.toLowerCase().replace(/\s+/g, " ").trim();
  if (normalized === "democratic" || normalized === "democrat" || normalized === "democratic party") {
    return "democratic";
  }
  if (normalized === "republican" || normalized === "gop" || normalized === "republican party") {
    return "republican";
  }
  return normalized;
}

function partyMatchesExpected(party: string, expectedParty: string | null | undefined): boolean {
  const expected = expectedParty?.trim();
  if (!expected) {
    return true;
  }
  return normalizePartyKey(party) === normalizePartyKey(expected);
}

function normalizeStatus(value: unknown): PresidentialRosterCandidateStatus | null {
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return STATUS_SET.has(normalized) ? (normalized as PresidentialRosterCandidateStatus) : null;
}

function normalizeFecCandidateId(value: unknown): string | null | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim().toUpperCase();
  return /^P[0-9A-Z]+$/.test(normalized) ? normalized : null;
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

function parseCandidate(
  value: unknown,
  options: PresidentialRosterPayloadParseOptions
): { ok: true; candidate: PresidentialRosterCandidate } | { ok: false; reason: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ok: false, reason: "payload.candidates contains invalid row" };
  }

  const input = value as Record<string, unknown>;
  if (!isNonEmptyString(input.display_name)) {
    return { ok: false, reason: "candidate.display_name must be non-empty string" };
  }
  if (!isNonEmptyString(input.party)) {
    return { ok: false, reason: "candidate.party must be non-empty string" };
  }

  const party = input.party.trim();
  if (!partyMatchesExpected(party, options.expectedParty)) {
    return {
      ok: false,
      reason: `candidate.party does not match expected party ${options.expectedParty}`,
    };
  }

  const status = normalizeStatus(input.status);
  if (!status) {
    return { ok: false, reason: "candidate.status must be active or withdrawn" };
  }

  const sources = normalizeSources(input.sources);
  if (!sources) {
    return { ok: false, reason: "candidate.sources must contain valid URL strings" };
  }

  const fecCandidateId = normalizeFecCandidateId(input.fec_candidate_id);
  if (fecCandidateId === null) {
    return { ok: false, reason: "candidate.fec_candidate_id must be a presidential FEC ID when present" };
  }

  return {
    ok: true,
    candidate: {
      display_name: input.display_name.trim(),
      party,
      ...(fecCandidateId ? { fec_candidate_id: fecCandidateId } : {}),
      sources,
      status,
    },
  };
}

export function parsePresidentialRosterPayload(
  payload: unknown,
  options: PresidentialRosterPayloadParseOptions = {}
):
  | { ok: true; payload: PresidentialRosterPayload }
  | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.candidates)) {
    return { ok: false, reason: "payload.candidates must be array" };
  }

  const candidates: PresidentialRosterCandidate[] = [];
  for (const row of input.candidates) {
    const parsed = parseCandidate(row, options);
    if (!parsed.ok) {
      return { ok: false, reason: parsed.reason };
    }
    candidates.push(parsed.candidate);
  }

  return { ok: true, payload: { candidates } };
}
