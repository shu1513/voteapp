import type {
  ElectionDistrictType,
  ElectionEnrichedPayload,
  ElectionEntryPayload,
} from "../types/election.js";
import {
  ELECTION_ALLOWED_DISTRICT_TYPES,
  ELECTION_RACE_TYPES,
  ELECTION_SENATE_CLASSES,
  ELECTION_STAGES,
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

function isElectionStage(value: unknown): value is NonNullable<ElectionEntryPayload["election_stage"]> {
  return typeof value === "string" && ELECTION_STAGES.includes(value as NonNullable<ElectionEntryPayload["election_stage"]>);
}

function isElectionSenateClass(value: unknown): value is NonNullable<ElectionEntryPayload["senate_class"]> {
  return typeof value === "string" && ELECTION_SENATE_CLASSES.includes(value as NonNullable<ElectionEntryPayload["senate_class"]>);
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

function parseEntry(
  value: unknown,
  options?: { allowDescriptionField: boolean }
): ElectionEntryPayload | null {
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
  const impactText = isNonEmptyString(input.impact)
    ? input.impact.trim()
    : options?.allowDescriptionField && isNonEmptyString(input.description)
      ? input.description.trim()
      : null;
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

  let electionStage: ElectionEntryPayload["election_stage"] | undefined;
  if (input.election_stage !== undefined) {
    if (input.race_type !== "office") {
      return null;
    }
    if (!isElectionStage(input.election_stage)) {
      return null;
    }
    electionStage = input.election_stage;
  }

  let senateClass: ElectionEntryPayload["senate_class"] | undefined;
  if (input.senate_class !== undefined && input.senate_class !== null) {
    if (input.race_type !== "office") {
      return null;
    }
    if (!isElectionSenateClass(input.senate_class)) {
      return null;
    }
    senateClass = input.senate_class;
  }

  let termEndYear: string | undefined;
  if (input.term_end_year !== undefined && input.term_end_year !== null) {
    if (input.race_type !== "office") {
      return null;
    }
    if (!isNonEmptyString(input.term_end_year)) {
      return null;
    }
    const normalized = input.term_end_year.trim();
    if (!/^\d{4}$/.test(normalized)) {
      return null;
    }
    termEndYear = normalized;
  }

  let isPartisan: boolean | undefined;
  if (input.is_partisan !== undefined && input.is_partisan !== null) {
    if (typeof input.is_partisan !== "boolean") {
      return null;
    }
    if (input.race_type === "ballot_measure" && input.is_partisan) {
      isPartisan = false;
    } else {
      isPartisan = input.is_partisan;
    }
  }

  return {
    official_ballot_title: input.official_ballot_title.trim(),
    election_date: input.election_date.trim(),
    // Canonical payload keeps historical "description" field; AI prompt now uses "impact".
    description: impactText,
    race_type: input.race_type as "office" | "ballot_measure",
    ...(isPartisan !== undefined ? { is_partisan: isPartisan } : {}),
    ...(electionStage ? { election_stage: electionStage } : {}),
    ...(senateClass ? { senate_class: senateClass } : {}),
    ...(termEndYear ? { term_end_year: termEndYear } : {}),
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
    const parsed = parseEntry(row, { allowDescriptionField: false });
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
    const parsed = parseEntry(row, { allowDescriptionField: true });
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
