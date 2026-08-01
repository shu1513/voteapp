import { findBlockedSourceReason } from "../pipeline/candidates/candidateRecordSourcePolicy.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type PresidentialNomineePayload =
  | {
      nominee_found: true;
      candidate_name: string;
      fec_candidate_id?: string;
      sources: string[];
    }
  | {
      nominee_found: false;
      sources: string[];
    };

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeFecCandidateId(value: unknown): string | null | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim().toUpperCase();
  return /^P\d{8}$/.test(normalized) ? normalized : null;
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

export function parsePresidentialNomineePayload(
  payload: unknown
): { ok: true; payload: PresidentialNomineePayload } | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (typeof input.nominee_found !== "boolean") {
    return { ok: false, reason: "payload.nominee_found must be boolean" };
  }

  const sources = normalizeSources(input.sources);
  if (!sources) {
    return { ok: false, reason: "payload.sources must contain valid URL strings" };
  }
  // Same domain policy as candidate records: UGC/social platforms, generated
  // candidate directories, and bot-check interstitials are never citation
  // evidence for a nominee determination.
  const blockedSourceReason = findBlockedSourceReason(sources);
  if (blockedSourceReason) {
    return { ok: false, reason: `payload.sources: ${blockedSourceReason}` };
  }

  if (!input.nominee_found) {
    return {
      ok: true,
      payload: {
        nominee_found: false,
        sources,
      },
    };
  }

  if (!isNonEmptyString(input.candidate_name)) {
    return { ok: false, reason: "payload.candidate_name must be non-empty string when nominee_found is true" };
  }

  const fecCandidateId = normalizeFecCandidateId(input.fec_candidate_id);
  if (fecCandidateId === null) {
    return { ok: false, reason: "payload.fec_candidate_id must be a presidential FEC ID when present" };
  }

  return {
    ok: true,
    payload: {
      nominee_found: true,
      candidate_name: input.candidate_name.trim(),
      ...(fecCandidateId ? { fec_candidate_id: fecCandidateId } : {}),
      sources,
    },
  };
}
