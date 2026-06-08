import type { ElectionResultContext } from "../pipeline/electionResults/electionResultContextLoader.js";
import { matchCandidateElectionResultWinners } from "../pipeline/electionResults/candidateElectionResultMatcher.js";
import {
  BALLOT_MEASURE_RESULT_OUTCOMES,
  ELECTION_RESULT_MATCH_STATUSES,
  ELECTION_RESULT_OUTCOMES,
  ELECTION_RESULT_SOURCE_TYPES,
  ELECTION_RESULT_STATUSES,
  type BallotMeasureResultOutcome,
  type ElectionResultMatchStatus,
  type ElectionResultOutcome,
  type ElectionResultPassType,
  type ElectionResultSourceType,
  type ElectionResultStatus,
} from "../types/electionResults.js";
import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type ElectionResultWinnerPayload = {
  candidate_election_id?: string;
  candidate_id?: string;
  candidate_name?: string;
  party?: string;
};

export type ParsedElectionResultPayloadRow = {
  election_id: string;
  result_status: ElectionResultStatus;
  outcome: ElectionResultOutcome | BallotMeasureResultOutcome;
  winners: ElectionResultWinnerPayload[];
  match_status: ElectionResultMatchStatus;
  source_url: string;
  source_type: ElectionResultSourceType;
  notes: string;
};

export type ElectionResultPayload = {
  results: ParsedElectionResultPayloadRow[];
};

type ParseOptions = {
  passType: ElectionResultPassType;
  contexts: readonly ElectionResultContext[];
};

const ELECTION_RESULT_STATUS_SET = new Set<string>(ELECTION_RESULT_STATUSES);
const ELECTION_RESULT_OUTCOME_SET = new Set<string>(ELECTION_RESULT_OUTCOMES);
const BALLOT_MEASURE_RESULT_OUTCOME_SET = new Set<string>(BALLOT_MEASURE_RESULT_OUTCOMES);
const ELECTION_RESULT_SOURCE_TYPE_SET = new Set<string>(ELECTION_RESULT_SOURCE_TYPES);
const ELECTION_RESULT_MATCH_STATUS_SET = new Set<string>(ELECTION_RESULT_MATCH_STATUSES);

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeStatus(value: unknown): ElectionResultStatus | null {
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return ELECTION_RESULT_STATUS_SET.has(normalized) ? (normalized as ElectionResultStatus) : null;
}

function normalizeSourceType(value: unknown): ElectionResultSourceType | null {
  if (!isNonEmptyString(value)) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  return ELECTION_RESULT_SOURCE_TYPE_SET.has(normalized) ? (normalized as ElectionResultSourceType) : null;
}

function isOfficeOutcome(value: string): value is ElectionResultOutcome {
  return ELECTION_RESULT_OUTCOME_SET.has(value);
}

function isBallotMeasureOutcome(value: string): value is BallotMeasureResultOutcome {
  return BALLOT_MEASURE_RESULT_OUTCOME_SET.has(value);
}

function isTerminalMissingStatus(status: ElectionResultStatus): boolean {
  return status === "not_found" || status === "not_final_yet";
}

function sourceTypeIsCompatible(status: ElectionResultStatus, sourceType: ElectionResultSourceType): boolean {
  if (sourceType === "ap" || sourceType === "news") {
    return status === "projected";
  }
  if (
    status === "unofficial" ||
    status === "certified" ||
    status === "not_final_yet"
  ) {
    return sourceType === "official";
  }
  if (sourceType === "other") {
    return status === "not_found";
  }
  return true;
}

function statusIsCompatibleWithPass(status: ElectionResultStatus, passType: ElectionResultPassType): boolean {
  if (passType === "election_night") {
    return status !== "certified";
  }
  return status !== "projected";
}

function deriveMatchStatus(
  status: ElectionResultStatus,
  context: ElectionResultContext,
  winners: readonly ElectionResultWinnerPayload[]
): ElectionResultMatchStatus {
  if (context.raceType === "ballot_measure") {
    return "not_applicable";
  }
  if (status === "not_found") {
    return "not_found";
  }
  if (winners.length === 0) {
    return "unmatched";
  }
  const matched = winners.filter((winner) => winner.candidate_election_id).length;
  if (matched === winners.length) {
    return "matched";
  }
  return matched > 0 ? "partial" : "unmatched";
}

function parseWinners(
  value: unknown,
  context: ElectionResultContext
): { ok: true; winners: ElectionResultWinnerPayload[] } | { ok: false; reason: string } {
  if (value === undefined || value === null) {
    return { ok: true, winners: [] };
  }
  if (!Array.isArray(value)) {
    return { ok: false, reason: "winners must be array" };
  }

  const candidateElectionIds = new Map(
    context.candidates.map((candidate) => [candidate.candidateElectionId, candidate])
  );
  const rawWinners: ElectionResultWinnerPayload[] = [];

  for (const row of value) {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      return { ok: false, reason: "winners entries must be objects" };
    }
    const input = row as Record<string, unknown>;
    const candidateElectionId = normalizeOptionalString(input.candidate_election_id);
    const candidateName = normalizeOptionalString(input.candidate_name);
    const party = normalizeOptionalString(input.party);
    if (!candidateElectionId && !candidateName) {
      return { ok: false, reason: "winner candidate_name must be non-empty string when candidate_election_id is absent" };
    }

    if (candidateElectionId) {
      const rosterCandidate = candidateElectionIds.get(candidateElectionId);
      if (!rosterCandidate) {
        return { ok: false, reason: `winner candidate_election_id is not in provided roster: ${candidateElectionId}` };
      }
    }

    rawWinners.push({
      ...(candidateElectionId ? { candidate_election_id: candidateElectionId } : {}),
      candidate_name: candidateName ?? "",
      ...(party ? { party } : {}),
    });
  }

  const matchedWinners = matchCandidateElectionResultWinners(rawWinners, context.candidates);
  const seen = new Set<string>();
  const winners: ElectionResultWinnerPayload[] = [];
  for (const match of matchedWinners) {
    const key =
      match.winner.candidate_election_id ??
      match.winner.candidate_id ??
      `${(match.winner.candidate_name ?? "").toLowerCase()}::${match.winner.party?.toLowerCase() ?? ""}`;
    if (seen.has(key)) {
      return { ok: false, reason: "winners contains duplicate candidate" };
    }
    seen.add(key);
    winners.push(match.winner);
  }

  return { ok: true, winners };
}

function parseRow(
  value: unknown,
  context: ElectionResultContext,
  options: ParseOptions
): { ok: true; row: ParsedElectionResultPayloadRow } | { ok: false; reason: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ok: false, reason: "results entries must be objects" };
  }
  const input = value as Record<string, unknown>;
  if (input.election_id !== context.electionId) {
    return { ok: false, reason: `result election_id does not match context: ${String(input.election_id)}` };
  }

  const resultStatus = normalizeStatus(input.result_status);
  if (!resultStatus) {
    return { ok: false, reason: "result_status is invalid" };
  }
  if (!statusIsCompatibleWithPass(resultStatus, options.passType)) {
    return { ok: false, reason: `result_status ${resultStatus} is incompatible with pass_type ${options.passType}` };
  }

  const sourceType = normalizeSourceType(input.source_type);
  if (!sourceType) {
    return { ok: false, reason: "source_type is invalid" };
  }
  if (!sourceTypeIsCompatible(resultStatus, sourceType)) {
    return {
      ok: false,
      reason: `source_type ${sourceType} is incompatible with result_status ${resultStatus}`,
    };
  }

  if (!isNonEmptyString(input.source_url)) {
    return { ok: false, reason: "source_url must be non-empty string" };
  }
  const sourceUrl = normalizeHttpUrl(input.source_url);
  if (!sourceUrl) {
    return { ok: false, reason: "source_url must be valid http(s) URL" };
  }

  const rawOutcome = normalizeOptionalString(input.outcome)?.toLowerCase();
  if (!rawOutcome) {
    return { ok: false, reason: "outcome is required" };
  }

  const winners = parseWinners(input.winners, context);
  if (!winners.ok) {
    return winners;
  }

  let outcome: ElectionResultOutcome | BallotMeasureResultOutcome;
  if (context.raceType === "ballot_measure") {
    if (!isBallotMeasureOutcome(rawOutcome)) {
      return { ok: false, reason: "ballot measure outcome is invalid" };
    }
    if (winners.winners.length > 0) {
      return { ok: false, reason: "ballot measure results must not include winners" };
    }
    outcome = rawOutcome;
  } else {
    if (!isOfficeOutcome(rawOutcome)) {
      return { ok: false, reason: "office outcome is invalid" };
    }
    outcome = rawOutcome;
  }

  if (isTerminalMissingStatus(resultStatus)) {
    if (outcome !== "unknown") {
      return { ok: false, reason: `${resultStatus} must use outcome=unknown` };
    }
    if (winners.winners.length > 0) {
      return { ok: false, reason: `${resultStatus} must not include winners` };
    }
  }

  const matchStatus = deriveMatchStatus(resultStatus, context, winners.winners);
  if (!ELECTION_RESULT_MATCH_STATUS_SET.has(matchStatus)) {
    return { ok: false, reason: "derived match_status is invalid" };
  }

  return {
    ok: true,
    row: {
      election_id: context.electionId,
      result_status: resultStatus,
      outcome,
      winners: winners.winners,
      match_status: matchStatus,
      source_url: sourceUrl,
      source_type: sourceType,
      notes: normalizeOptionalString(input.notes) ?? "",
    },
  };
}

export function parseElectionResultPayload(
  payload: unknown,
  options: ParseOptions
): { ok: true; payload: ElectionResultPayload } | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }
  if (options.contexts.length === 0) {
    return { ok: false, reason: "contexts must contain at least one election" };
  }
  if (options.contexts.length > 10) {
    return { ok: false, reason: "contexts must contain at most 10 elections" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.results)) {
    return { ok: false, reason: "payload.results must be array" };
  }

  const contextsById = new Map(options.contexts.map((context) => [context.electionId, context]));
  const seenElectionIds = new Set<string>();
  const rows: ParsedElectionResultPayloadRow[] = [];

  for (const rawRow of input.results) {
    if (typeof rawRow !== "object" || rawRow === null || Array.isArray(rawRow)) {
      return { ok: false, reason: "payload.results contains invalid row" };
    }
    const electionId = (rawRow as Record<string, unknown>).election_id;
    if (!isNonEmptyString(electionId)) {
      return { ok: false, reason: "result election_id must be non-empty string" };
    }
    const context = contextsById.get(electionId.trim());
    if (!context) {
      return { ok: false, reason: `result contains election_id outside provided context: ${electionId}` };
    }
    if (seenElectionIds.has(context.electionId)) {
      return { ok: false, reason: `payload.results contains duplicate election_id: ${context.electionId}` };
    }
    const parsed = parseRow(rawRow, context, options);
    if (!parsed.ok) {
      return parsed;
    }
    seenElectionIds.add(context.electionId);
    rows.push(parsed.row);
  }

  for (const context of options.contexts) {
    if (!seenElectionIds.has(context.electionId)) {
      return { ok: false, reason: `payload.results is missing election_id: ${context.electionId}` };
    }
  }

  return { ok: true, payload: { results: rows } };
}
