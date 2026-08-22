import {
  hasMiddleNameConflict,
  personNamesMatchWithMiddleEvidence,
} from "../finance/personNameMiddleEvidence.js";
import type { NewHampshireFilingEntityRow } from "./newHampshireCfsClient.js";
import { normalizeNewHampshireCandidateAlias } from "./newHampshireOutsideSpendingAggregator.js";

export type NewHampshireCandidateFilerResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  district?: string | null;
  electionCycleId: number;
  filingEntityRows: readonly NewHampshireFilingEntityRow[];
  sourceUrl?: string | null;
};

export type NewHampshireCandidateFilerMatch = {
  filingEntityId: number;
  filerName: string;
  candidateAliases: string[];
  officeName: string;
  district: string | null;
  confidence: "exact";
  source: "cfs_registration";
  sourceUrl: string | null;
  matchedRegistrationRowCount: number;
};

export type NewHampshireCandidateFilerResolution =
  | ({ status: "matched" } & NewHampshireCandidateFilerMatch)
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_required_district"
        | "no_candidate_filer_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_filers";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: NewHampshireCandidateFilerMatch[];
    };

type NewHampshireCanonicalOfficeName =
  | "Governor"
  | "Executive Council"
  | "State Senate"
  | "State Representative"
  | "County Commissioner"
  | "County Attorney"
  | "County Treasurer"
  | "Sheriff"
  | "Register of Deeds"
  | "Register of Probate";

type CandidateFilerAccumulator = {
  filingEntityId: number;
  filerNames: Set<string>;
  candidateAliases: Map<string, string>;
  rows: NewHampshireFilingEntityRow[];
};

type NewHampshireDistrictEvidence = {
  key: string;
  label: string;
};

const NEW_HAMPSHIRE_COUNTIES = [
  "Belknap",
  "Carroll",
  "Cheshire",
  "Coos",
  "Grafton",
  "Hillsborough",
  "Merrimack",
  "Rockingham",
  "Strafford",
  "Sullivan",
] as const;

function normalizeElectionCycleId(value: number): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid New Hampshire candidate filer election-cycle ID: ${value}`);
  }
  return value;
}

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeNewHampshireCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const keys = new Set<string>();
  const normalized = normalizePersonName(trimmed);
  if (normalized) keys.add(normalized);

  const commaParts = trimmed
    .split(",")
    .map(normalizePersonName)
    .filter(Boolean);
  if (commaParts.length >= 2) {
    const lastName = commaParts[0] ?? "";
    const givenNames = commaParts.slice(1).join(" ");
    const flipped = normalizePersonName(`${givenNames} ${lastName}`);
    if (flipped) keys.add(flipped);
  }
  return keys;
}

export function normalizeNewHampshireCandidateNameForStorage(value: string): string {
  const trimmed = value.trim();
  const commaParts = trimmed
    .split(",")
    .map(normalizePersonName)
    .filter(Boolean);
  if (commaParts.length >= 2) {
    return normalizePersonName(`${commaParts.slice(1).join(" ")} ${commaParts[0] ?? ""}`);
  }
  return normalizePersonName(trimmed);
}

function candidateNamesMatch(candidateName: string, officialNames: readonly string[]): boolean {
  const candidateKeys = normalizeNewHampshireCandidateNameKeys(candidateName);
  for (const officialName of officialNames) {
    const rowKeys = normalizeNewHampshireCandidateNameKeys(officialName);
    for (const key of rowKeys) {
      if (
        candidateKeys.has(key) &&
        !hasMiddleNameConflict({
          candidateName,
          rowNames: [officialName],
          normalizePersonName,
        })
      ) {
        return true;
      }
    }
    if (
      personNamesMatchWithMiddleEvidence({
        candidateName,
        rowNames: [officialName],
        normalizePersonName,
      })
    ) {
      return true;
    }
  }
  return false;
}

function canonicalOfficeName(value: string): NewHampshireCanonicalOfficeName | null {
  const key = normalizeTextKey(value)
    .replace(/\bNEW HAMPSHIRE\b/g, " ")
    .replace(/\bNH\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  switch (key) {
    case "GOVERNOR":
      return "Governor";
    case "EXECUTIVE COUNCIL":
    case "EXECUTIVE COUNCILOR":
      return "Executive Council";
    case "STATE SENATE":
    case "STATE SENATOR":
    case "STATE UPPER CHAMBER LEGISLATOR":
      return "State Senate";
    case "STATE REPRESENTATIVE":
    case "STATE HOUSE":
    case "HOUSE REPRESENTATIVE":
    case "HOUSE OF REPRESENTATIVES":
    case "STATE LOWER CHAMBER LEGISLATOR":
      return "State Representative";
    case "COUNTY COMMISSIONER":
      return "County Commissioner";
    case "COUNTY ATTORNEY":
      return "County Attorney";
    case "COUNTY TREASURER":
      return "County Treasurer";
    case "SHERIFF":
      return "Sheriff";
    case "REGISTER OF DEEDS":
      return "Register of Deeds";
    case "REGISTER OF PROBATE":
      return "Register of Probate";
    default:
      return null;
  }
}

function scopeMatchesOffice(officeScope: string, officeName: NewHampshireCanonicalOfficeName): boolean {
  const scope = officeScope.trim().toLowerCase();
  if (scope === "state_upper") return officeName === "State Senate";
  if (scope === "state_lower") return officeName === "State Representative";
  if (scope === "county") {
    return (
      officeName === "County Commissioner" ||
      officeName === "County Attorney" ||
      officeName === "County Treasurer" ||
      officeName === "Sheriff" ||
      officeName === "Register of Deeds" ||
      officeName === "Register of Probate"
    );
  }
  if (scope === "statewide") {
    return officeName === "Governor" || officeName === "Executive Council";
  }
  return false;
}

function officeRequiresDistrict(officeName: NewHampshireCanonicalOfficeName): boolean {
  return officeName !== "Governor";
}

function districtNumber(value: string | null | undefined): string {
  const key = normalizeTextKey(value ?? "")
    .replace(/\bNEW HAMPSHIRE\b/g, " ")
    .replace(/\bNH\b/g, " ")
    .replace(/\b(?:19|20)\d{2}\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const match = /(?:^| )0*(\d+)$/.exec(key);
  return match?.[1] ?? "";
}

function districtCounty(
  district: string | null | undefined,
  officialCounty?: string | null
): string {
  const key = normalizeTextKey([officialCounty, district].filter(Boolean).join(" "));
  return NEW_HAMPSHIRE_COUNTIES.find((county) => key.includes(county.toUpperCase())) ?? "";
}

function normalizeDistrictEvidence(
  officeName: NewHampshireCanonicalOfficeName,
  district: string | null | undefined,
  officialCounty?: string | null
): NewHampshireDistrictEvidence | null {
  const isCountyOffice =
    officeName === "County Commissioner" ||
    officeName === "County Attorney" ||
    officeName === "County Treasurer" ||
    officeName === "Sheriff" ||
    officeName === "Register of Deeds" ||
    officeName === "Register of Probate";
  const county = districtCounty(district, officialCounty);
  if (isCountyOffice && officeName !== "County Commissioner") {
    return county ? { key: county.toUpperCase(), label: county } : null;
  }

  const number = districtNumber(district);
  if (!number) return null;
  if (officeName === "State Representative" || officeName === "County Commissioner") {
    // These district numbers repeat by county. The CFS response calls its
    // county field `town`; VoteApp includes the county in the district name.
    return county
      ? { key: `${county.toUpperCase()}:${number}`, label: `${county} ${number}` }
      : null;
  }
  return { key: number, label: number };
}

function isCandidateRegistration(row: NewHampshireFilingEntityRow): boolean {
  return (
    row.filerTypeCode === "CAN" ||
    row.filerTypeCode === "CC" ||
    row.filerSubTypeCode === "PACCC"
  );
}

function officialCandidateNames(row: NewHampshireFilingEntityRow): string[] {
  const candidateName = row.candidateName?.trim();
  if (candidateName) return [candidateName];

  const structured = [row.firstName, row.lastName].filter(Boolean).join(" ").trim();
  return structured ? [structured] : [];
}

function registrationRaceTargetKey(row: NewHampshireFilingEntityRow): string | null {
  const officeName = canonicalOfficeName(row.officeName ?? "");
  if (!officeName) return null;
  const district = normalizeDistrictEvidence(officeName, row.district, row.county);
  if (officeRequiresDistrict(officeName) && !district) return null;
  return `${officeName}\u0000${district?.key ?? ""}`;
}

function retainUnambiguousCandidateAliases(input: {
  aliases: readonly string[];
  filingEntityRows: readonly NewHampshireFilingEntityRow[];
  electionCycleId: number;
}): string[] {
  return input.aliases.filter((alias) => {
    const raceTargets = new Set<string>();
    for (const row of input.filingEntityRows) {
      if (!isCandidateRegistration(row) || row.electionCycleId !== input.electionCycleId) {
        continue;
      }
      const raceTarget = registrationRaceTargetKey(row);
      if (!raceTarget) continue;
      const names = officialCandidateNames(row);
      if (names.length > 0 && candidateNamesMatch(alias, names)) raceTargets.add(raceTarget);
    }
    return raceTargets.size <= 1;
  });
}

function rememberCandidateAlias(accumulator: CandidateFilerAccumulator, value: string): void {
  const alias = value.trim();
  const key = normalizeNewHampshireCandidateAlias(alias);
  if (key && !accumulator.candidateAliases.has(key)) {
    accumulator.candidateAliases.set(key, alias);
  }
}

function toMatch(input: {
  accumulator: CandidateFilerAccumulator;
  officeName: NewHampshireCanonicalOfficeName;
  district: NewHampshireDistrictEvidence | null;
  sourceUrl: string | null;
}): NewHampshireCandidateFilerMatch {
  const filerName = [...input.accumulator.filerNames].sort((left, right) =>
    left.localeCompare(right)
  )[0];
  if (!filerName) {
    throw new Error(`Missing New Hampshire filer name for entity ${input.accumulator.filingEntityId}`);
  }
  return {
    filingEntityId: input.accumulator.filingEntityId,
    filerName,
    candidateAliases: [...input.accumulator.candidateAliases.values()].sort((left, right) =>
      left.localeCompare(right)
    ),
    officeName: input.officeName,
    district: input.district?.label ?? null,
    confidence: "exact",
    source: "cfs_registration",
    sourceUrl: input.sourceUrl,
    matchedRegistrationRowCount: input.accumulator.rows.length,
  };
}

export function resolveNewHampshireCandidateFiler(
  input: NewHampshireCandidateFilerResolverInput
): NewHampshireCandidateFilerResolution {
  const electionCycleId = normalizeElectionCycleId(input.electionCycleId);
  const candidateNameNormalized = normalizeNewHampshireCandidateNameForStorage(input.candidateName);
  const canonicalOffice = canonicalOfficeName(input.officeName);
  const officeNameNormalized = canonicalOffice ?? normalizeTextKey(input.officeName);
  if (!candidateNameNormalized) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (!canonicalOffice || !scopeMatchesOffice(input.officeScope, canonicalOffice)) {
    return {
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const district = normalizeDistrictEvidence(canonicalOffice, input.district);
  if (officeRequiresDistrict(canonicalOffice) && !district) {
    return {
      status: "unmatched",
      reason: "missing_required_district",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const rowsByFiler = new Map<number, CandidateFilerAccumulator>();
  for (const row of input.filingEntityRows) {
    if (!isCandidateRegistration(row) || row.electionCycleId !== electionCycleId) continue;
    if (canonicalOfficeName(row.officeName ?? "") !== canonicalOffice) continue;
    const rowDistrict = normalizeDistrictEvidence(canonicalOffice, row.district, row.county);
    if (officeRequiresDistrict(canonicalOffice) && rowDistrict?.key !== district?.key) continue;

    const names = officialCandidateNames(row);
    if (names.length === 0 || !candidateNamesMatch(input.candidateName, names)) continue;

    const accumulator = rowsByFiler.get(row.filingEntityId) ?? {
      filingEntityId: row.filingEntityId,
      filerNames: new Set<string>(),
      candidateAliases: new Map<string, string>(),
      rows: [],
    };
    accumulator.rows.push(row);
    accumulator.filerNames.add(row.filerName);
    rememberCandidateAlias(accumulator, input.candidateName);
    for (const name of names) rememberCandidateAlias(accumulator, name);
    rowsByFiler.set(row.filingEntityId, accumulator);
  }

  const matches = [...rowsByFiler.values()]
    .map((accumulator) =>
      toMatch({
        accumulator,
        officeName: canonicalOffice,
        district,
        sourceUrl: input.sourceUrl ?? null,
      })
    )
    .map((match) => ({
      ...match,
      candidateAliases: retainUnambiguousCandidateAliases({
        aliases: match.candidateAliases,
        filingEntityRows: input.filingEntityRows,
        electionCycleId,
      }),
    }))
    .sort((left, right) => left.filingEntityId - right.filingEntityId);

  if (matches.length === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (matches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_filers",
      candidateNameNormalized,
      officeNameNormalized,
      matches,
    };
  }
  return { status: "matched", ...matches[0]! };
}
