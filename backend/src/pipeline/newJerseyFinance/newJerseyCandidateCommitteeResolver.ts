import {
  searchNewJerseyElecEntities,
  type NewJerseyElecClientOptions,
  type NewJerseyElecEntity,
} from "./newJerseyElecClient.js";
import { isNewJerseyFinanceEligibleOffice } from "./newJerseyFinanceEligibleOffices.js";

export type NewJerseyCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  entityRows: readonly NewJerseyElecEntity[];
  electionTypeCode?: string | null;
  locationCode?: number | string | null;
};

export type NewJerseyCandidateCommitteeSearchInput = Omit<
  NewJerseyCandidateCommitteeResolverInput,
  "entityRows"
>;

export type NewJerseyCandidateCommitteeMatch = {
  entityS: number;
  entityName: string;
  firstName: string | null;
  lastName: string | null;
  office: string | null;
  officeCode: string | null;
  party: string | null;
  partyCode: string | null;
  location: string | null;
  locationCode: number | null;
  electionType: string | null;
  electionTypeCode: string | null;
  confidence: "exact";
  source: "elec_api";
  sourceUrl: string;
  matchedEntityRowCount: number;
};

export type NewJerseyCandidateCommitteeResolution =
  | ({ status: "matched" } & NewJerseyCandidateCommitteeMatch)
  | {
      status: "unmatched";
      reason: "missing_candidate_name" | "unsupported_office" | "no_candidate_entity_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_entities";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: NewJerseyCandidateCommitteeMatch[];
    };

type EntityAccumulator = {
  entity: NewJerseyElecEntity;
  matchedRowCount: number;
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1980 || value > 2100) {
    throw new Error(`Invalid New Jersey candidate committee election year: ${value}`);
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
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeNewJerseyCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const keys = new Set<string>();

  function addName(raw: string): void {
    const normalized = normalizePersonName(raw);
    if (normalized) {
      keys.add(normalized);
    }
    const parts = normalized.split(" ").filter(Boolean);
    if (parts.length >= 2) {
      keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
    }

    const commaParts = raw
      .split(",")
      .map((part) => normalizePersonName(part))
      .filter(Boolean);
    if (commaParts.length >= 2) {
      const lastName = commaParts[0] ?? "";
      const firstNames = commaParts.slice(1).join(" ");
      addName(`${firstNames} ${lastName}`);
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
  return [...normalizeNewJerseyCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

function newJerseyOfficeNameFromEntityOffice(value: string | null | undefined): string | null {
  const normalized = normalizeTextKey(value);
  if (!normalized) {
    return null;
  }
  if (normalized === "GOVERNOR") {
    return "Governor";
  }
  if (normalized === "LIEUTENANT GOVERNOR" || normalized === "LT GOVERNOR") {
    return "Lieutenant Governor";
  }
  if (/\bSENATE\b/.test(normalized) || /\bSENATOR\b/.test(normalized)) {
    return "State Senator";
  }
  if (/\bASSEMBLY\b/.test(normalized)) {
    return "State Lower Chamber Legislator";
  }
  return null;
}

function entityMatchesCandidateName(input: {
  entity: NewJerseyElecEntity;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const values = [
    input.entity.entityName,
    [input.entity.firstName, input.entity.lastName].filter(Boolean).join(" "),
    [input.entity.lastName, input.entity.firstName].filter(Boolean).join(", "),
  ];
  for (const value of values) {
    for (const key of normalizeNewJerseyCandidateNameKeys(value)) {
      if (input.candidateNameKeys.has(key)) {
        return true;
      }
    }
  }
  return false;
}

function entityMatchesOffice(input: { entity: NewJerseyElecEntity; officeName: string }): boolean {
  const mappedOffice = newJerseyOfficeNameFromEntityOffice(input.entity.office);
  return mappedOffice === input.officeName.trim();
}

function normalizeOptionalCode(value: string | number | null | undefined): string | null {
  const normalized = String(value ?? "")
    .trim()
    .toUpperCase();
  return normalized.length > 0 ? normalized : null;
}

function entityMatchesOptionalFilters(input: {
  entity: NewJerseyElecEntity;
  electionTypeCode?: string | null;
  locationCode?: string | number | null;
}): boolean {
  const electionTypeCode = normalizeOptionalCode(input.electionTypeCode);
  if (electionTypeCode && normalizeOptionalCode(input.entity.electionTypeCode) !== electionTypeCode) {
    return false;
  }

  const locationCode = normalizeOptionalCode(input.locationCode);
  if (locationCode && normalizeOptionalCode(input.entity.locationCode) !== locationCode) {
    return false;
  }

  return true;
}

function toCommitteeMatch(accumulator: EntityAccumulator): NewJerseyCandidateCommitteeMatch {
  const { entity } = accumulator;
  return {
    entityS: entity.entityS,
    entityName: entity.entityName,
    firstName: entity.firstName,
    lastName: entity.lastName,
    office: entity.office,
    officeCode: entity.officeCode,
    party: entity.party,
    partyCode: entity.partyCode,
    location: entity.location,
    locationCode: entity.locationCode,
    electionType: entity.electionType,
    electionTypeCode: entity.electionTypeCode,
    confidence: "exact",
    source: "elec_api",
    sourceUrl: entity.sourceUrl,
    matchedEntityRowCount: accumulator.matchedRowCount,
  };
}

function lastNameSearchTerm(candidateName: string): string | null {
  const keys = [...normalizeNewJerseyCandidateNameKeys(candidateName)];
  const firstKey = keys[0];
  if (!firstKey) {
    return null;
  }
  const parts = firstKey.split(" ").filter(Boolean);
  return parts.at(-1) ?? null;
}

export function resolveNewJerseyCandidateCommittee(
  input: NewJerseyCandidateCommitteeResolverInput
): NewJerseyCandidateCommitteeResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeNewJerseyCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeNameNormalized = input.officeName.trim() || normalizeTextKey(input.officeName);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }
  if (!isNewJerseyFinanceEligibleOffice({ officeScope: input.officeScope, officeCanonicalName: input.officeName })) {
    return {
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const matches = new Map<number, EntityAccumulator>();
  for (const entity of input.entityRows) {
    if (entity.electionYear !== electionYear) {
      continue;
    }
    if (!entityMatchesCandidateName({ entity, candidateNameKeys })) {
      continue;
    }
    if (!entityMatchesOffice({ entity, officeName: input.officeName })) {
      continue;
    }
    if (
      !entityMatchesOptionalFilters({
        entity,
        electionTypeCode: input.electionTypeCode,
        locationCode: input.locationCode,
      })
    ) {
      continue;
    }

    const existing = matches.get(entity.entityS);
    if (!existing) {
      matches.set(entity.entityS, { entity, matchedRowCount: 1 });
      continue;
    }
    existing.matchedRowCount += 1;
  }

  if (matches.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_entity_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const entityMatches = [...matches.values()]
    .map(toCommitteeMatch)
    .sort((left, right) => left.entityS - right.entityS);

  if (entityMatches.length === 1 && entityMatches[0]) {
    return {
      status: "matched",
      ...entityMatches[0],
    };
  }

  return {
    status: "ambiguous",
    reason: "multiple_matching_entities",
    candidateNameNormalized: candidateNameKey,
    officeNameNormalized,
    matches: entityMatches,
  };
}

export async function searchAndResolveNewJerseyCandidateCommittee(
  input: NewJerseyCandidateCommitteeSearchInput,
  options: NewJerseyElecClientOptions = {}
): Promise<NewJerseyCandidateCommitteeResolution> {
  if (!isNewJerseyFinanceEligibleOffice({ officeScope: input.officeScope, officeCanonicalName: input.officeName })) {
    return resolveNewJerseyCandidateCommittee({ ...input, entityRows: [] });
  }

  const lastName = lastNameSearchTerm(input.candidateName);
  if (!lastName) {
    return resolveNewJerseyCandidateCommittee({ ...input, entityRows: [] });
  }

  const entityRows = await searchNewJerseyElecEntities(
    {
      lastName,
      nonPacOnly: true,
    },
    options
  );
  return resolveNewJerseyCandidateCommittee({
    ...input,
    entityRows,
  });
}
