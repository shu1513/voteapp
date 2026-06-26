import {
  searchTennesseeCampCandidates,
  type TennesseeCampCandidateRecord,
  type TennesseeCampClientOptions,
} from "./tennesseeCampClient.js";
import {
  normalizeTennesseeCampDistrict,
  tennesseeCampOfficeLabelForAppOffice,
  toTennesseeCampOfficeSearchInput,
} from "./tennesseeFinanceEligibleOffices.js";

export type TennesseeCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  candidateRecords: readonly TennesseeCampCandidateRecord[];
};

export type TennesseeCandidateCommitteeSearchInput = Omit<
  TennesseeCandidateCommitteeResolverInput,
  "candidateRecords"
>;

export type TennesseeCandidateCommitteeMatch = {
  campCandidateId: string;
  ownerName: string;
  candidateName: string;
  officeSought: string | null;
  district: string | null;
  confidence: "exact";
  source: "tncamp_search";
  sourceUrl: string | null;
  reportListUrl: string | null;
  matchedRowCount: number;
};

export type TennesseeCandidateCommitteeResolution =
  | ({ status: "matched" } & TennesseeCandidateCommitteeMatch)
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
      matches: TennesseeCandidateCommitteeMatch[];
    };

type CandidateAccumulator = {
  campCandidateId: string;
  ownerName: string;
  candidateName: string;
  officeSought: string | null;
  district: string | null;
  sourceUrl: string | null;
  reportListUrl: string | null;
  rows: TennesseeCampCandidateRecord[];
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Tennessee candidate committee election year: ${value}`);
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
    .replace(/\b(THE|OF|FOR|COMMITTEE|FRIENDS)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeTennesseeCandidateNameKeys(value: string): Set<string> {
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
  return [...normalizeTennesseeCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

function lastNameSearchToken(candidateName: string): string {
  const trimmed = candidateName.replace(/\([^()]+\)/g, " ").trim();
  if (trimmed.includes(",")) {
    const commaFirst = normalizePersonName(trimmed.split(",", 1)[0]);
    if (commaFirst) {
      return commaFirst;
    }
  }
  const normalized = normalizePersonName(trimmed);
  return normalized.split(/\s+/).filter(Boolean).at(-1) ?? trimmed;
}

function recordMatchesCandidateName(input: {
  record: TennesseeCampCandidateRecord;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const names = [input.record.name, input.record.ownerName].filter(Boolean);
  for (const name of names) {
    for (const key of normalizeTennesseeCandidateNameKeys(name)) {
      if (input.candidateNameKeys.has(key)) {
        return true;
      }
    }
  }
  return false;
}

function toCommitteeMatch(accumulator: CandidateAccumulator): TennesseeCandidateCommitteeMatch {
  return {
    campCandidateId: accumulator.campCandidateId,
    ownerName: accumulator.ownerName,
    candidateName: accumulator.candidateName,
    officeSought: accumulator.officeSought,
    district: accumulator.district,
    confidence: "exact",
    source: "tncamp_search",
    sourceUrl: accumulator.sourceUrl,
    reportListUrl: accumulator.reportListUrl,
    matchedRowCount: accumulator.rows.length,
  };
}

function recordMatchesDistrict(input: {
  record: TennesseeCampCandidateRecord;
  expectedDistrict: string | null;
}): boolean {
  if (input.expectedDistrict === null) {
    return true;
  }
  return normalizeTennesseeCampDistrict(input.record.district) === input.expectedDistrict;
}

export function resolveTennesseeCandidateCommittee(
  input: TennesseeCandidateCommitteeResolverInput
): TennesseeCandidateCommitteeResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeTennesseeCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeSearch = toTennesseeCampOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
  });
  const expectedOfficeLabel = tennesseeCampOfficeLabelForAppOffice({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
  });
  const officeNameNormalized = expectedOfficeLabel ?? normalizeTextKey(input.officeName);
  const isLegislativeOffice = input.officeScope === "state_upper" || input.officeScope === "state_lower";
  const expectedDistrict = isLegislativeOffice ? normalizeTennesseeCampDistrict(input.district) : null;

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (!officeSearch || !expectedOfficeLabel) {
    return {
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (isLegislativeOffice && !expectedDistrict) {
    return {
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const matches = new Map<string, CandidateAccumulator>();
  for (const record of input.candidateRecords) {
    const campCandidateId = record.campCandidateId.trim();
    const ownerName = record.ownerName.trim();
    if (!campCandidateId || !ownerName) {
      continue;
    }
    if (record.electionYear !== electionYear) {
      continue;
    }
    if (normalizeTextKey(record.officeSought) !== normalizeTextKey(expectedOfficeLabel)) {
      continue;
    }
    if (!recordMatchesDistrict({ record, expectedDistrict })) {
      continue;
    }
    if (!recordMatchesCandidateName({ record, candidateNameKeys })) {
      continue;
    }
    const accumulator = matches.get(campCandidateId) ?? {
      campCandidateId,
      ownerName,
      candidateName: record.name.trim() || ownerName,
      officeSought: record.officeSought,
      district: record.district,
      sourceUrl: record.sourceUrl,
      reportListUrl: record.reportListUrl,
      rows: [],
    };
    accumulator.rows.push(record);
    matches.set(campCandidateId, accumulator);
  }

  if (matches.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  const resolvedMatches = [...matches.values()].map(toCommitteeMatch);
  if (resolvedMatches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
      matches: resolvedMatches,
    };
  }
  return { status: "matched", ...resolvedMatches[0]! };
}

export async function searchAndResolveTennesseeCandidateCommittee(
  input: TennesseeCandidateCommitteeSearchInput,
  options?: TennesseeCampClientOptions
): Promise<TennesseeCandidateCommitteeResolution> {
  const officeSearch = toTennesseeCampOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
  });
  if (!officeSearch) {
    return resolveTennesseeCandidateCommittee({ ...input, candidateRecords: [] });
  }
  const candidateRecords = await searchTennesseeCampCandidates(
    {
      candidateName: lastNameSearchToken(input.candidateName),
      electionYear: input.electionYear,
      officeSelection: officeSearch.officeSelection,
    },
    options
  );
  return resolveTennesseeCandidateCommittee({ ...input, candidateRecords });
}
