import { toIllinoisSbeOfficeSearchInput } from "./illinoisFinanceEligibleOffices.js";
import { normalizeIllinoisCommitteeKey } from "./illinoisFinanceAggregators.js";
import {
  fetchIllinoisSbeCandidateContributionRecords,
  ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL,
  type IllinoisSbeClientOptions,
  type IllinoisSbeContributionRecord,
} from "./illinoisSbeClient.js";

export type IllinoisCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  contributionRecords: readonly IllinoisSbeContributionRecord[];
  sourceUrl?: string | null;
};

export type IllinoisCandidateCommitteeMatch = {
  committeeKey: string;
  committeeName: string;
  confidence: "exact";
  source: "illinois_sbe";
  sourceUrl: string | null;
  matchedContributionRowCount: number;
};

export type IllinoisCandidateCommitteeResolution =
  | ({ status: "matched" } & IllinoisCandidateCommitteeMatch)
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_legislative_district"
        | "no_candidate_committee_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_committees";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: IllinoisCandidateCommitteeMatch[];
    };

type CandidateCommitteeAccumulator = {
  committeeKey: string;
  committeeName: string;
  rows: IllinoisSbeContributionRecord[];
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Illinois candidate committee election year: ${value}`);
  }
  return value;
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR|TO|ELECT|COMMITTEE|FRIENDS|CITIZENS|PEOPLE)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeIllinoisCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const keys = new Set<string>();

  function addName(raw: string): void {
    const hasComma = raw.includes(",");
    const normalized = normalizePersonName(raw);
    if (normalized) {
      keys.add(normalized);
    }

    const parts = normalized.split(" ").filter(Boolean);
    if (!hasComma && parts.length >= 2) {
      keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
    }

    const commaParts = raw
      .split(",")
      .map((part) => normalizePersonName(part))
      .filter(Boolean);
    if (commaParts.length >= 2) {
      const lastName = commaParts[0] ?? "";
      const firstNames = commaParts.slice(1).join(" ").trim();
      const flipped = normalizePersonName(`${firstNames} ${lastName}`);
      if (flipped) {
        keys.add(flipped);
        const flippedParts = flipped.split(" ").filter(Boolean);
        if (flippedParts.length >= 2) {
          keys.add(`${flippedParts[0]} ${flippedParts[flippedParts.length - 1]}`);
        }
      }
    }
  }

  addName(trimmed.replace(/\([^()]+\)/g, " "));
  for (const match of trimmed.matchAll(/\(([^()]+)\)/g)) {
    if (match[1]) {
      addName(match[1]);
    }
  }

  return keys;
}

function candidateNameNormalized(value: string): string {
  return [...normalizeIllinoisCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

export function normalizeIllinoisCandidateNameForStorage(value: string): string {
  return candidateNameNormalized(value);
}

function parseYearFromDate(value: string | null | undefined): number | null {
  const trimmed = value?.trim() ?? "";
  const slashMatch = /^\d{1,2}\/\d{1,2}\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[1]) {
    return Number.parseInt(slashMatch[1], 10);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  return isoMatch?.[1] ? Number.parseInt(isoMatch[1], 10) : null;
}

function recordMatchesElectionCycle(record: IllinoisSbeContributionRecord, electionYear: number): boolean {
  const year = parseYearFromDate(record.receivedDate);
  return year !== null && year >= electionYear - 1 && year <= electionYear;
}

function committeeNameLikelyMatchesCandidate(input: {
  committeeName: string;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const committeeNameKey = normalizeTextKey(input.committeeName);
  if (!committeeNameKey) {
    return false;
  }
  const committeeTokens = new Set(committeeNameKey.split(" ").filter(Boolean));
  for (const key of input.candidateNameKeys) {
    const tokens = key.split(" ").filter(Boolean);
    if (tokens.length >= 2 && tokens.every((token) => committeeTokens.has(token))) {
      return true;
    }
  }
  return false;
}

function isLikelyCandidateCommitteeName(value: string): boolean {
  const normalized = normalizeTextKey(value);
  if (!normalized) {
    return false;
  }
  return !/\b(?:PAC|SUPERPAC|SUPER PAC|INDEPENDENT|EXPENDITURE|PARTY|CAUCUS|BALLOT|REFERENDUM)\b/.test(normalized);
}

function toCommitteeMatch(input: {
  accumulator: CandidateCommitteeAccumulator;
  sourceUrl: string | null;
}): IllinoisCandidateCommitteeMatch {
  return {
    committeeKey: input.accumulator.committeeKey,
    committeeName: input.accumulator.committeeName,
    confidence: "exact",
    source: "illinois_sbe",
    sourceUrl: input.sourceUrl,
    matchedContributionRowCount: input.accumulator.rows.length,
  };
}

function isLegislativeInput(input: { officeScope: string; officeName: string }): boolean {
  return (
    (input.officeScope === "state_upper" && input.officeName.trim() === "State Senator") ||
    (input.officeScope === "state_lower" && input.officeName.trim() === "State Lower Chamber Legislator")
  );
}

function splitCandidateNameForSearch(value: string): { firstName: string | null; lastName: string } | null {
  const normalized = value
    .replace(/\([^()]+\)/g, " ")
    .trim()
    .replace(/\s+/g, " ");
  if (!normalized) {
    return null;
  }

  const commaParts = normalized
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (commaParts.length >= 2) {
    return {
      lastName: commaParts[0]!,
      firstName: commaParts.slice(1).join(" "),
    };
  }

  const parts = normalized.split(" ").filter(Boolean);
  if (parts.length === 1) {
    return { firstName: null, lastName: parts[0]! };
  }
  return {
    firstName: parts.slice(0, -1).join(" "),
    lastName: parts[parts.length - 1]!,
  };
}

export function resolveIllinoisCandidateCommittee(
  input: IllinoisCandidateCommitteeResolverInput
): IllinoisCandidateCommitteeResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeIllinoisCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeSearchInput = toIllinoisSbeOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    district: input.district,
  });
  const officeNameNormalized = officeSearchInput?.sbeOffice ?? normalizeTextKey(input.officeName);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (!officeSearchInput) {
    return {
      status: "unmatched",
      reason: isLegislativeInput(input) ? "missing_legislative_district" : "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const rowsByCommittee = new Map<string, CandidateCommitteeAccumulator>();
  for (const record of input.contributionRecords) {
    const committeeName = record.recipientCommitteeName?.trim().replace(/\s+/g, " ") ?? "";
    const committeeKey = normalizeIllinoisCommitteeKey(committeeName);
    if (!committeeName || !committeeKey || !isLikelyCandidateCommitteeName(committeeName)) {
      continue;
    }
    if (!recordMatchesElectionCycle(record, electionYear)) {
      continue;
    }
    if (!committeeNameLikelyMatchesCandidate({ committeeName, candidateNameKeys })) {
      continue;
    }

    const accumulator = rowsByCommittee.get(committeeKey) ?? {
      committeeKey,
      committeeName,
      rows: [],
    };
    accumulator.rows.push(record);
    rowsByCommittee.set(committeeKey, accumulator);
  }

  const matches = [...rowsByCommittee.values()]
    .sort((left, right) => right.rows.length - left.rows.length || left.committeeName.localeCompare(right.committeeName))
    .map((accumulator) => toCommitteeMatch({ accumulator, sourceUrl: input.sourceUrl ?? null }));

  if (matches.length === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (matches.length === 1 || matches[0]!.matchedContributionRowCount > matches[1]!.matchedContributionRowCount) {
    return {
      status: "matched",
      ...matches[0]!,
    };
  }
  return {
    status: "ambiguous",
    reason: "multiple_matching_committees",
    candidateNameNormalized: candidateNameKey,
    officeNameNormalized,
    matches,
  };
}

export async function searchAndResolveIllinoisCandidateCommittee(
  input: Omit<IllinoisCandidateCommitteeResolverInput, "contributionRecords" | "sourceUrl">,
  options?: IllinoisSbeClientOptions
): Promise<IllinoisCandidateCommitteeResolution> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const searchName = splitCandidateNameForSearch(input.candidateName);
  if (!searchName) {
    return resolveIllinoisCandidateCommittee({
      ...input,
      electionYear,
      contributionRecords: [],
      sourceUrl: ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL,
    });
  }

  const contributionRecords = await fetchIllinoisSbeCandidateContributionRecords(
    {
      candidateLastName: searchName.lastName,
      candidateFirstName: searchName.firstName,
      electionYear,
      contributionType: "All Types",
    },
    options
  );

  return resolveIllinoisCandidateCommittee({
    ...input,
    electionYear,
    contributionRecords,
    sourceUrl: ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL,
  });
}
