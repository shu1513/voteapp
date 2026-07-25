import {
  illinoisMunicipalityMatches,
  mapIllinoisSbeOffice,
  normalizeIllinoisSbeLegislativeDistrict,
  toIllinoisFinanceOfficeKey,
  toIllinoisSbeOfficeSearchInput,
} from "./illinoisFinanceEligibleOffices.js";
import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { normalizeIllinoisCommitteeKey } from "./illinoisFinanceAggregators.js";
import {
  fetchIllinoisSbeCandidateContributionRecords,
  ILLINOIS_SBE_CONTRIBUTION_CANDIDATE_SEARCH_URL,
  type IllinoisSbeClientOptions,
  type IllinoisSbeContributionRecord,
} from "./illinoisSbeClient.js";
import type { IllinoisSbeCandidateCommitteeRelation } from "./illinoisSbeNormalizedArtifact.js";

export type IllinoisCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  contributionRecords: readonly IllinoisSbeContributionRecord[];
  sourceUrl?: string | null;
};

export type IllinoisCandidateCommitteeRelationResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  relations: readonly IllinoisSbeCandidateCommitteeRelation[];
};

export type IllinoisCandidateCommitteeMatch = {
  committeeKey: string;
  committeeName: string;
  confidence: "official_relation" | "name_fallback";
  source: "illinois_sbe";
  sourceUrl: string | null;
  matchedContributionRowCount: number;
  sbeCandidateId: string | null;
  sbeCommitteeId: string | null;
  sbeDistrictType: string | null;
  sbeOffice: string | null;
  district: string | null;
  isAtLarge: boolean | null;
};

export type IllinoisCandidateCommitteeResolution =
  | { status: "matched"; matches: IllinoisCandidateCommitteeMatch[] }
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_legislative_district"
        | "no_candidate_committee_match"
        | "no_official_candidate_relation"
        | "jurisdiction_mismatch";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_committees" | "multiple_official_candidates";
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

  function addFirstLastKeys(parts: readonly string[]): void {
    const firstName = parts[0]!;
    const lastName = parts[parts.length - 1]!;
    keys.add(`${firstName} ${lastName}`);
    for (const variant of firstNameVariants(firstName)) {
      keys.add(`${variant} ${lastName}`);
    }
  }

  function addName(raw: string): void {
    const normalized = normalizePersonName(raw);
    if (normalized) {
      keys.add(normalized);
    }

    const commaParts = raw
      .split(",")
      .map((part) => normalizePersonName(part))
      .filter(Boolean);
    // A comma only signals a "Last, First" flip when text survives on both
    // sides after normalization; a bare suffix comma ("Tarver, II") reads as
    // a plain first-to-last name.
    const isLastFirstName = commaParts.length >= 2;

    const parts = normalized.split(" ").filter(Boolean);
    if (!isLastFirstName && parts.length >= 2) {
      addFirstLastKeys(parts);
    }

    if (isLastFirstName) {
      const lastName = commaParts[0] ?? "";
      const firstNames = commaParts.slice(1).join(" ").trim();
      const flipped = normalizePersonName(`${firstNames} ${lastName}`);
      if (flipped) {
        keys.add(flipped);
        const flippedParts = flipped.split(" ").filter(Boolean);
        if (flippedParts.length >= 2) {
          addFirstLastKeys(flippedParts);
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
    confidence: "name_fallback",
    source: "illinois_sbe",
    sourceUrl: input.sourceUrl,
    matchedContributionRowCount: input.accumulator.rows.length,
    sbeCandidateId: null,
    sbeCommitteeId: null,
    sbeDistrictType: null,
    sbeOffice: null,
    district: null,
    isAtLarge: null,
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
  while (parts.length > 1 && /^(?:JR|SR|II|III|IV|V)\.?$/i.test(parts[parts.length - 1]!)) {
    parts.pop();
  }
  if (parts.length === 1) {
    return { firstName: null, lastName: parts[0]! };
  }
  return {
    firstName: parts.slice(0, -1).join(" "),
    lastName: parts[parts.length - 1]!,
  };
}

function relationCandidateNameMatches(
  relation: IllinoisSbeCandidateCommitteeRelation,
  candidateNameKeys: ReadonlySet<string>
): boolean {
  for (const key of normalizeIllinoisCandidateNameKeys(relation.candidateName)) {
    if (candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function relationJurisdictionMatches(input: {
  relation: IllinoisSbeCandidateCommitteeRelation;
  mappedOffice: NonNullable<ReturnType<typeof mapIllinoisSbeOffice>>;
  officeScope: string;
  district?: string | null;
}): boolean {
  if (input.officeScope === "place") {
    return illinoisMunicipalityMatches({
      voteAppDistrictName: input.district,
      sbeDistrictName: input.relation.district,
      sbeDistrictType: input.relation.districtType,
    });
  }
  if (input.officeScope === "state_upper" || input.officeScope === "state_lower") {
    const maxDistrict = input.officeScope === "state_upper" ? 59 : 118;
    return (
      input.mappedOffice.district ===
      normalizeIllinoisSbeLegislativeDistrict(input.district, maxDistrict, input.officeScope)
    );
  }
  return input.officeScope === "statewide";
}

export function resolveIllinoisCandidateCommitteesFromRelations(
  input: IllinoisCandidateCommitteeRelationResolverInput
): IllinoisCandidateCommitteeResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeIllinoisCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const expectedOfficeKey = toIllinoisFinanceOfficeKey({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
  });
  const officeNameNormalized = normalizeTextKey(input.officeName);
  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (!expectedOfficeKey) {
    return {
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const namedRelations = input.relations.filter(
    (relation) =>
      relation.electionYear === electionYear && relationCandidateNameMatches(relation, candidateNameKeys)
  );
  if (namedRelations.length === 0) {
    return {
      status: "unmatched",
      reason: "no_official_candidate_relation",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const matchingRelations = namedRelations.flatMap((relation) => {
    const mappedOffice = mapIllinoisSbeOffice({
      office: relation.office,
      district: relation.district,
      districtType: relation.districtType,
      isAtLarge: relation.isAtLarge,
    });
    if (
      !mappedOffice ||
      mappedOffice.officeKey !== expectedOfficeKey ||
      !relationJurisdictionMatches({
        relation,
        mappedOffice,
        officeScope: input.officeScope,
        district: input.district,
      })
    ) {
      return [];
    }
    return [{ relation, mappedOffice }];
  });
  if (matchingRelations.length === 0) {
    return {
      status: "unmatched",
      reason: "jurisdiction_mismatch",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const candidateIds = new Set(matchingRelations.map(({ relation }) => relation.candidateId));
  const toMatch = ({ relation }: (typeof matchingRelations)[number]): IllinoisCandidateCommitteeMatch => ({
    committeeKey: `SBE:${relation.committeeId}`,
    committeeName: relation.committeeName,
    confidence: "official_relation",
    source: "illinois_sbe",
    sourceUrl: relation.sourceUrl,
    matchedContributionRowCount: 0,
    sbeCandidateId: relation.candidateId,
    sbeCommitteeId: relation.committeeId,
    sbeDistrictType: relation.districtType,
    sbeOffice: relation.office,
    district: relation.district,
    isAtLarge: relation.isAtLarge,
  });
  if (candidateIds.size > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_official_candidates",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
      matches: matchingRelations.map(toMatch),
    };
  }

  const matchesByCommittee = new Map<string, IllinoisCandidateCommitteeMatch>();
  for (const entry of matchingRelations) {
    if (entry.relation.committeeStatus === "inactive") {
      continue;
    }
    const match = toMatch(entry);
    matchesByCommittee.set(match.committeeKey, match);
  }
  const matches = [...matchesByCommittee.values()].sort((left, right) =>
    left.committeeKey.localeCompare(right.committeeKey)
  );
  if (matches.length === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  return { status: "matched", matches };
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
    return { status: "matched", matches: [matches[0]!] };
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
