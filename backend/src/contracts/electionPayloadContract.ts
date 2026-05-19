import type {
  ElectionDistrictType,
  ElectionEnrichedPayload,
  ElectionEntryPayload,
} from "../types/election.js";
import {
  ELECTION_ALLOWED_DISTRICT_TYPES,
  ELECTION_RACE_TYPES,
} from "./electionEnrichmentContract.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

type ParseResult =
  | { ok: true; payload: ElectionEnrichedPayload }
  | { ok: false; reason: string };

type ParseAiEntriesResult =
  | {
      ok: true;
      payload: {
        entries: ElectionEntryPayload[];
        review_decision?: "approve" | "reject";
        review_reason?: string;
      };
    }
  | { ok: false; reason: string };

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }

  const [year, month, day] = value.split("-").map((part) => Number.parseInt(part, 10));
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }

  const parsed = new Date(Date.UTC(year, month - 1, day));
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() === month - 1 &&
    parsed.getUTCDate() === day
  );
}

function isDistrictType(value: unknown): value is ElectionDistrictType {
  return typeof value === "string" && ELECTION_ALLOWED_DISTRICT_TYPES.includes(value as ElectionDistrictType);
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

function parseEntry(value: unknown): ElectionEntryPayload | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }

  const input = value as Record<string, unknown>;
  if (!isNonEmptyString(input.official_ballot_title)) {
    return null;
  }
  if (!isNonEmptyString(input.election_date) || !isIsoDate(input.election_date.trim())) {
    return null;
  }
  const impactText = isNonEmptyString(input.office_or_measure_impact)
    ? input.office_or_measure_impact.trim()
    : (isNonEmptyString(input.description) ? input.description.trim() : null);
  if (!impactText) {
    return null;
  }
  if (typeof input.race_type !== "string" || !ELECTION_RACE_TYPES.includes(input.race_type as "office" | "ballot_measure")) {
    return null;
  }
  const sources = normalizeSources(input.sources);
  if (!sources) {
    return null;
  }

  return {
    official_ballot_title: input.official_ballot_title.trim(),
    election_date: input.election_date.trim(),
    // Canonical payload keeps historical "description" field; AI prompt now uses office_or_measure_impact.
    description: impactText,
    race_type: input.race_type as "office" | "ballot_measure",
    sources,
  };
}

function parseReviewDecisionAndReason(input: Record<string, unknown>): {
  ok: true;
  review_decision?: "approve" | "reject";
  review_reason?: string;
} | {
  ok: false;
  reason: string;
} {
  let reviewDecision: "approve" | "reject" | undefined;
  if (input.review_decision !== undefined) {
    if (input.review_decision !== "approve" && input.review_decision !== "reject") {
      return { ok: false, reason: "payload.review_decision must be approve|reject when present" };
    }
    reviewDecision = input.review_decision;
  }

  let reviewReason: string | undefined;
  if (input.review_reason !== undefined) {
    if (!isNonEmptyString(input.review_reason)) {
      return { ok: false, reason: "payload.review_reason must be non-empty string when present" };
    }
    reviewReason = input.review_reason.trim();
  }

  return {
    ok: true,
    ...(reviewDecision ? { review_decision: reviewDecision } : {}),
    ...(reviewReason ? { review_reason: reviewReason } : {}),
  };
}

export function parseAiElectionEntriesPayload(payload: unknown): ParseAiEntriesResult {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.entries)) {
    return { ok: false, reason: "payload.entries must be array" };
  }

  const entries: ElectionEntryPayload[] = [];
  for (const row of input.entries) {
    const parsed = parseEntry(row);
    if (!parsed) {
      return { ok: false, reason: "payload.entries contains invalid row" };
    }
    entries.push(parsed);
  }

  const review = parseReviewDecisionAndReason(input);
  if (!review.ok) {
    return review;
  }

  return {
    ok: true,
    payload: {
      entries,
      ...(review.review_decision ? { review_decision: review.review_decision } : {}),
      ...(review.review_reason ? { review_reason: review.review_reason } : {}),
    },
  };
}

export function parseCanonicalElectionPayload(payload: unknown): ParseResult {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!isNonEmptyString(input.district_id)) {
    return { ok: false, reason: "payload.district_id must be non-empty string" };
  }
  if (!isNonEmptyString(input.district_name)) {
    return { ok: false, reason: "payload.district_name must be non-empty string" };
  }
  if (!isDistrictType(input.district_type)) {
    return { ok: false, reason: "payload.district_type is invalid" };
  }
  if (!isNonEmptyString(input.state)) {
    return { ok: false, reason: "payload.state must be non-empty string" };
  }

  if (!Array.isArray(input.entries)) {
    return { ok: false, reason: "payload.entries must be array" };
  }

  const entries: ElectionEntryPayload[] = [];
  for (const row of input.entries) {
    const parsed = parseEntry(row);
    if (!parsed) {
      return { ok: false, reason: "payload.entries contains invalid row" };
    }
    entries.push(parsed);
  }

  const review = parseReviewDecisionAndReason(input);
  if (!review.ok) {
    return review;
  }

  return {
    ok: true,
    payload: {
      district_id: input.district_id.trim(),
      district_name: input.district_name.trim(),
      district_type: input.district_type,
      state: input.state.trim(),
      entries,
      ...(review.review_decision ? { review_decision: review.review_decision } : {}),
      ...(review.review_reason ? { review_reason: review.review_reason } : {}),
    },
  };
}
