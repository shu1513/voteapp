import type { ElectionResultCandidateContext } from "./electionResultContextLoader.js";
import type { ElectionResultWinnerPayload } from "../../contracts/electionResultPayloadContract.js";
import { normalizeCandidateName } from "../../utils/candidateIdentity.js";

export type CandidateElectionResultMatchMethod =
  | "exact_candidate_election_id"
  | "exact_name_party"
  | "exact_name"
  | "fuzzy_name"
  | "unmatched";

export type CandidateElectionResultMatch = {
  winner: ElectionResultWinnerPayload;
  method: CandidateElectionResultMatchMethod;
  confidence: number;
};

const FUZZY_NAME_MIN_CONFIDENCE = 0.88;
const FUZZY_NAME_MIN_MARGIN = 0.12;

function normalizeParty(value: string | undefined): string {
  const normalized = (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (normalized === "democrat" || normalized === "democratic party" || normalized === "democratic") {
    return "democratic";
  }
  if (normalized === "republican party" || normalized === "gop" || normalized === "republican") {
    return "republican";
  }
  if (
    normalized === "no party preference" ||
    normalized === "npp" ||
    normalized === "nonpartisan" ||
    normalized === "no party"
  ) {
    return "no party preference";
  }
  if (normalized === "independent" || normalized === "ind") {
    return "independent";
  }
  return normalized;
}

function normalizeNameForMatch(value: string): string {
  return normalizeCandidateName(value)
    .split(" ")
    .filter((token) => !["jr", "sr", "ii", "iii", "iv"].includes(token))
    .join(" ");
}

function toTokenSet(value: string): Set<string> {
  return new Set(value.split(" ").filter((token) => token.length > 0));
}

function tokenF1(left: string, right: string): number {
  const leftTokens = toTokenSet(left);
  const rightTokens = toTokenSet(right);
  if (leftTokens.size === 0 || rightTokens.size === 0) {
    return 0;
  }
  let intersection = 0;
  for (const token of leftTokens) {
    if (rightTokens.has(token)) {
      intersection += 1;
    }
  }
  if (intersection === 0) {
    return 0;
  }
  const precision = intersection / leftTokens.size;
  const recall = intersection / rightTokens.size;
  return (2 * precision * recall) / (precision + recall);
}

function levenshteinDistance(left: string, right: string): number {
  if (left === right) {
    return 0;
  }
  if (left.length === 0) {
    return right.length;
  }
  if (right.length === 0) {
    return left.length;
  }

  const previous = Array.from({ length: right.length + 1 }, (_, index) => index);
  const current = new Array<number>(right.length + 1);

  for (let i = 1; i <= left.length; i += 1) {
    current[0] = i;
    for (let j = 1; j <= right.length; j += 1) {
      const substitutionCost = left[i - 1] === right[j - 1] ? 0 : 1;
      current[j] = Math.min(
        previous[j]! + 1,
        current[j - 1]! + 1,
        previous[j - 1]! + substitutionCost
      );
    }
    for (let j = 0; j <= right.length; j += 1) {
      previous[j] = current[j]!;
    }
  }

  return previous[right.length]!;
}

function stringSimilarity(left: string, right: string): number {
  const maxLength = Math.max(left.length, right.length);
  if (maxLength === 0) {
    return 0;
  }
  return 1 - levenshteinDistance(left, right) / maxLength;
}

function lastToken(value: string): string {
  const tokens = value.split(" ").filter((token) => token.length > 0);
  return tokens[tokens.length - 1] ?? "";
}

function firstToken(value: string): string {
  const tokens = value.split(" ").filter((token) => token.length > 0);
  return tokens[0] ?? "";
}

function hasFirstLastAgreement(left: string, right: string): boolean {
  return firstToken(left) === firstToken(right) && lastToken(left) === lastToken(right);
}

function scoreNameSimilarity(left: string, right: string): number {
  if (left.length === 0 || right.length === 0) {
    return 0;
  }
  if (left === right) {
    return 1;
  }
  if (hasFirstLastAgreement(left, right)) {
    const leftTokens = toTokenSet(left);
    const rightTokens = toTokenSet(right);
    if (leftTokens.size >= 2 && rightTokens.size >= 2) {
      return 0.94;
    }
  }

  const f1 = tokenF1(left, right);
  const similarity = stringSimilarity(left, right);
  const surnameBoost = lastToken(left) === lastToken(right) && f1 >= 0.8 ? 0.04 : 0;
  return Math.min(1, Math.max(f1, similarity) + surnameBoost);
}

function toMatchedWinner(
  winner: ElectionResultWinnerPayload,
  candidate: ElectionResultCandidateContext
): ElectionResultWinnerPayload {
  return {
    ...winner,
    candidate_election_id: candidate.candidateElectionId,
    candidate_id: candidate.candidateId,
    candidate_name: candidate.displayName,
    party: candidate.party ?? winner.party ?? undefined,
  };
}

export function matchCandidateElectionResultWinner(
  winner: ElectionResultWinnerPayload,
  roster: readonly ElectionResultCandidateContext[]
): CandidateElectionResultMatch {
  if (winner.candidate_election_id) {
    const exact = roster.find((candidate) => candidate.candidateElectionId === winner.candidate_election_id);
    if (exact) {
      return {
        winner: toMatchedWinner(winner, exact),
        method: "exact_candidate_election_id",
        confidence: 1,
      };
    }
  }

  const winnerName = normalizeNameForMatch(winner.candidate_name ?? "");
  const winnerParty = normalizeParty(winner.party);
  if (winnerName.length === 0) {
    return { winner, method: "unmatched", confidence: 0 };
  }

  if (winnerParty.length > 0) {
    const exactNameParty = roster.filter(
      (candidate) =>
        normalizeNameForMatch(candidate.displayName) === winnerName &&
        normalizeParty(candidate.party) === winnerParty
    );
    if (exactNameParty.length === 1) {
      return {
        winner: toMatchedWinner(winner, exactNameParty[0]!),
        method: "exact_name_party",
        confidence: 1,
      };
    }
  }

  const exactName = roster.filter((candidate) => normalizeNameForMatch(candidate.displayName) === winnerName);
  if (exactName.length === 1) {
    return {
      winner: toMatchedWinner(winner, exactName[0]!),
      method: "exact_name",
      confidence: 0.98,
    };
  }

  const scored = roster
    .map((candidate) => ({
      candidate,
      confidence: scoreNameSimilarity(winnerName, normalizeNameForMatch(candidate.displayName)),
    }))
    .sort((left, right) => right.confidence - left.confidence);
  const top = scored[0];
  const runnerUp = scored[1];
  const margin = top ? top.confidence - (runnerUp?.confidence ?? 0) : 0;
  if (top && top.confidence >= FUZZY_NAME_MIN_CONFIDENCE && margin >= FUZZY_NAME_MIN_MARGIN) {
    return {
      winner: toMatchedWinner(winner, top.candidate),
      method: "fuzzy_name",
      confidence: top.confidence,
    };
  }

  return {
    winner: {
      candidate_name: winner.candidate_name ?? "",
      ...(winner.party ? { party: winner.party } : {}),
    },
    method: "unmatched",
    confidence: top?.confidence ?? 0,
  };
}

export function matchCandidateElectionResultWinners(
  winners: readonly ElectionResultWinnerPayload[],
  roster: readonly ElectionResultCandidateContext[]
): CandidateElectionResultMatch[] {
  return winners.map((winner) => matchCandidateElectionResultWinner(winner, roster));
}
