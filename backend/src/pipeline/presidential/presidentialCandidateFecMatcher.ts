import type { PresidentialRosterCandidate } from "../../contracts/presidentialRosterPayloadContract.js";
import { normalizeCandidateName } from "../../utils/candidateIdentity.js";
import {
  getPresidentialCandidateByFecId,
  searchPresidentialCandidatesByName,
  type OpenFecClientOptions,
  type OpenFecPresidentialCandidate,
} from "./openFecClient.js";

export type PresidentialCandidateFecMatchStatus = "matched" | "ambiguous" | "unmatched";

export type PresidentialCandidateFecMatchMethod =
  | "exact_fec_id"
  | "exact_name_party"
  | "fuzzy_name_party"
  | "ambiguous"
  | "unmatched";

export type PresidentialCandidateFecMatch = {
  matchStatus: PresidentialCandidateFecMatchStatus;
  method: PresidentialCandidateFecMatchMethod;
  confidence: number;
  matchedFecId?: string;
  matchedCandidate?: OpenFecPresidentialCandidate;
  fecSourceUrls: string[];
  reason?: string;
};

export type PresidentialCandidateFecMatcherOptions = OpenFecClientOptions & {
  getByFecId?: (fecCandidateId: string, options: OpenFecClientOptions) => Promise<OpenFecPresidentialCandidate | null>;
  searchByName?: (
    input: {
      electionYear: number;
      name: string;
      partyCode?: string;
      perPage?: number;
    },
    options: OpenFecClientOptions
  ) => Promise<OpenFecPresidentialCandidate[]>;
};

export type PresidentialCandidateForFecMatch = Omit<PresidentialRosterCandidate, "fec_candidate_id"> & {
  fec_candidate_id?: string;
};

export type PresidentialCandidateFecMatchInput = {
  electionYear: number;
  candidate: PresidentialCandidateForFecMatch;
  expectedParty?: string | null;
  options: PresidentialCandidateFecMatcherOptions;
};

const FUZZY_NAME_MIN_CONFIDENCE = 0.9;
const FUZZY_NAME_MIN_MARGIN = 0.1;

function normalizeNameForMatch(value: string): string {
  return normalizeCandidateName(value)
    .split(" ")
    .filter((token) => !["jr", "sr", "ii", "iii", "iv"].includes(token))
    .join(" ");
}

export function presidentialPartyToOpenFecPartyCode(party: string | null | undefined): string | undefined {
  const normalized = (party ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (normalized === "democratic" || normalized === "democrat" || normalized === "democratic party") {
    return "DEM";
  }
  if (normalized === "republican" || normalized === "gop" || normalized === "republican party") {
    return "REP";
  }
  if (normalized === "libertarian" || normalized === "libertarian party") {
    return "LIB";
  }
  if (normalized === "green" || normalized === "green party") {
    return "GRE";
  }
  return undefined;
}

function normalizedPartyKey(value: string | undefined): string {
  const normalized = (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (normalized === "dem" || normalized === "democrat" || normalized === "democratic party" || normalized === "democratic") {
    return "democratic";
  }
  if (normalized === "rep" || normalized === "gop" || normalized === "republican party" || normalized === "republican") {
    return "republican";
  }
  if (normalized === "lib" || normalized === "libertarian party" || normalized === "libertarian") {
    return "libertarian";
  }
  if (normalized === "gre" || normalized === "green party" || normalized === "green") {
    return "green";
  }
  return normalized;
}

function isPartyCompatible(
  candidate: OpenFecPresidentialCandidate,
  expectedParty: string | null | undefined
): boolean {
  const expectedKey = normalizedPartyKey(expectedParty ?? "");
  if (expectedKey.length === 0) {
    return true;
  }
  const rowKeys = [candidate.party, candidate.partyFull]
    .map((value) => normalizedPartyKey(value))
    .filter((value) => value.length > 0);
  return rowKeys.includes(expectedKey);
}

function isElectionYearCompatible(candidate: OpenFecPresidentialCandidate, electionYear: number): boolean {
  return candidate.electionYears.length === 0 || candidate.electionYears.includes(electionYear);
}

function isCandidateUsable(
  candidate: OpenFecPresidentialCandidate,
  input: { electionYear: number; expectedParty?: string | null }
): boolean {
  if (candidate.office && candidate.office.toUpperCase() !== "P") {
    return false;
  }
  return isElectionYearCompatible(candidate, input.electionYear) && isPartyCompatible(candidate, input.expectedParty);
}

function toMatched(
  candidate: OpenFecPresidentialCandidate,
  method: Exclude<PresidentialCandidateFecMatchMethod, "ambiguous" | "unmatched">,
  confidence: number
): PresidentialCandidateFecMatch {
  return {
    matchStatus: "matched",
    method,
    confidence,
    matchedFecId: candidate.fecCandidateId,
    matchedCandidate: candidate,
    fecSourceUrls: [candidate.fecCandidateUrl],
  };
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
      current[j] = Math.min(previous[j]! + 1, current[j - 1]! + 1, previous[j - 1]! + substitutionCost);
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

function firstToken(value: string): string {
  return value.split(" ").filter((token) => token.length > 0)[0] ?? "";
}

function lastToken(value: string): string {
  const tokens = value.split(" ").filter((token) => token.length > 0);
  return tokens[tokens.length - 1] ?? "";
}

function scoreNameSimilarity(left: string, right: string): number {
  if (left.length === 0 || right.length === 0) {
    return 0;
  }
  if (left === right) {
    return 1;
  }

  const f1 = tokenF1(left, right);
  const similarity = stringSimilarity(left, right);
  const firstLastBoost = firstToken(left) === firstToken(right) && lastToken(left) === lastToken(right) ? 0.04 : 0;
  return Math.min(1, Math.max(f1, similarity) + firstLastBoost);
}

function exactNameMatches(
  candidates: readonly OpenFecPresidentialCandidate[],
  normalizedAiName: string
): OpenFecPresidentialCandidate[] {
  return candidates.filter((candidate) => normalizeNameForMatch(candidate.name) === normalizedAiName);
}

function fuzzyNameMatch(
  candidates: readonly OpenFecPresidentialCandidate[],
  normalizedAiName: string
): PresidentialCandidateFecMatch | null {
  const scored = candidates
    .map((candidate) => ({
      candidate,
      confidence: scoreNameSimilarity(normalizedAiName, normalizeNameForMatch(candidate.name)),
    }))
    .sort((left, right) => right.confidence - left.confidence);

  const top = scored[0];
  if (!top) {
    return null;
  }
  const runnerUp = scored[1];
  const margin = top.confidence - (runnerUp?.confidence ?? 0);
  if (top.confidence >= FUZZY_NAME_MIN_CONFIDENCE && margin >= FUZZY_NAME_MIN_MARGIN) {
    return toMatched(top.candidate, "fuzzy_name_party", top.confidence);
  }
  if (top.confidence >= FUZZY_NAME_MIN_CONFIDENCE) {
    return {
      matchStatus: "ambiguous",
      method: "ambiguous",
      confidence: top.confidence,
      fecSourceUrls: scored
        .filter((row) => top.confidence - row.confidence < FUZZY_NAME_MIN_MARGIN)
        .map((row) => row.candidate.fecCandidateUrl),
      reason: "multiple OpenFEC candidates have similar fuzzy name scores",
    };
  }
  return null;
}

function uniqueByFecId(candidates: readonly OpenFecPresidentialCandidate[]): OpenFecPresidentialCandidate[] {
  const unique: OpenFecPresidentialCandidate[] = [];
  const seen = new Set<string>();
  for (const candidate of candidates) {
    if (seen.has(candidate.fecCandidateId)) {
      continue;
    }
    seen.add(candidate.fecCandidateId);
    unique.push(candidate);
  }
  return unique;
}

export async function matchPresidentialRosterCandidateToFec(
  input: PresidentialCandidateFecMatchInput
): Promise<PresidentialCandidateFecMatch> {
  const getByFecId = input.options.getByFecId ?? getPresidentialCandidateByFecId;
  const searchByName = input.options.searchByName ?? searchPresidentialCandidatesByName;
  const expectedParty = input.expectedParty ?? input.candidate.party;
  const partyCode = presidentialPartyToOpenFecPartyCode(expectedParty);

  if (input.candidate.fec_candidate_id) {
    const candidate = await getByFecId(input.candidate.fec_candidate_id, input.options);
    if (candidate && isCandidateUsable(candidate, { electionYear: input.electionYear, expectedParty })) {
      return toMatched(candidate, "exact_fec_id", 1);
    }
    return {
      matchStatus: "unmatched",
      method: "unmatched",
      confidence: 0,
      fecSourceUrls: candidate ? [candidate.fecCandidateUrl] : [],
      reason: "AI-provided FEC ID was not found or did not match the expected presidential cycle",
    };
  }

  const normalizedAiName = normalizeNameForMatch(input.candidate.display_name);
  if (normalizedAiName.length === 0) {
    return {
      matchStatus: "unmatched",
      method: "unmatched",
      confidence: 0,
      fecSourceUrls: [],
      reason: "candidate display_name is empty after normalization",
    };
  }

  const searched = await searchByName(
    {
      electionYear: input.electionYear,
      name: input.candidate.display_name,
      partyCode,
      perPage: 100,
    },
    input.options
  );
  const usableCandidates = uniqueByFecId(
    searched.filter((candidate) => isCandidateUsable(candidate, { electionYear: input.electionYear, expectedParty }))
  );

  const exactMatches = exactNameMatches(usableCandidates, normalizedAiName);
  if (exactMatches.length === 1) {
    return toMatched(exactMatches[0]!, "exact_name_party", 0.99);
  }
  if (exactMatches.length > 1) {
    return {
      matchStatus: "ambiguous",
      method: "ambiguous",
      confidence: 0.99,
      fecSourceUrls: exactMatches.map((candidate) => candidate.fecCandidateUrl),
      reason: "multiple OpenFEC candidates exactly match candidate name and party",
    };
  }

  const fuzzy = fuzzyNameMatch(usableCandidates, normalizedAiName);
  if (fuzzy) {
    return fuzzy;
  }

  return {
    matchStatus: "unmatched",
    method: "unmatched",
    confidence: 0,
    fecSourceUrls: usableCandidates.map((candidate) => candidate.fecCandidateUrl),
    reason: "no OpenFEC presidential candidate matched candidate name and party",
  };
}
