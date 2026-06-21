import type { ConnecticutEcrisArtifactRow } from "./connecticutEcrisArtifactReader.js";
import {
  mapConnecticutEcrisOffice,
  normalizeConnecticutEcrisOfficeLabel,
} from "./connecticutFinanceEligibleOffices.js";

export type ConnecticutCandidateCommitteeResolverInput = {
  candidateName: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  receiptRows: readonly ConnecticutEcrisArtifactRow[];
  sourceUrl?: string | null;
};

export type ConnecticutCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  confidence: "exact";
  source: "ecris_bulk";
  sourceUrl: string | null;
  matchedReceiptRowCount: number;
};

export type ConnecticutCandidateCommitteeResolution =
  | ({ status: "matched" } & ConnecticutCandidateCommitteeMatch)
  | {
      status: "unmatched";
      reason:
        | "missing_candidate_name"
        | "unsupported_office"
        | "missing_legislative_district"
        | "no_candidate_office_year_match";
      candidateNameNormalized: string;
      officeNameNormalized: string;
    }
  | {
      status: "ambiguous";
      reason: "multiple_matching_committees";
      candidateNameNormalized: string;
      officeNameNormalized: string;
      matches: ConnecticutCandidateCommitteeMatch[];
    };

type CandidateCommitteeMatchAccumulator = {
  committeeId: string;
  committeeName: string;
  rows: ConnecticutEcrisArtifactRow[];
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2008 || value > 2100) {
    throw new Error(`Invalid Connecticut candidate committee election year: ${value}`);
  }
  return value;
}

function value(row: ConnecticutEcrisArtifactRow, key: string): string {
  return row[key]?.trim() ?? "";
}

function firstNonEmpty(row: ConnecticutEcrisArtifactRow, keys: readonly string[]): string {
  for (const key of keys) {
    const current = value(row, key);
    if (current) {
      return current;
    }
  }
  return "";
}

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeConnecticutCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const normalized = normalizePersonName(trimmed);
  const keys = new Set<string>();
  if (normalized) {
    keys.add(normalized);
  }

  const commaParts = trimmed
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
    return keys;
  }

  const parts = normalized.split(" ").filter(Boolean);
  if (parts.length >= 2) {
    keys.add(`${parts[0]} ${parts[parts.length - 1]}`);
  }

  return keys;
}

function candidateNameFromReceiptRow(row: ConnecticutEcrisArtifactRow): string {
  return [
    value(row, "Candidate First Name"),
    firstNonEmpty(row, ["Candidate Middle Initial", "Candidate Middle Intial"]),
    value(row, "Candidate Last Name"),
  ]
    .filter(Boolean)
    .join(" ");
}

function rowMatchesCandidateName(input: {
  row: ConnecticutEcrisArtifactRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  for (const key of normalizeConnecticutCandidateNameKeys(candidateNameFromReceiptRow(input.row))) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function normalizeDistrict(value: string | null | undefined): string {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) {
    return "";
  }
  const numeric = Number(trimmed);
  if (Number.isInteger(numeric) && numeric >= 0) {
    return String(numeric);
  }
  return normalizeTextKey(trimmed);
}

function parseElectionYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isInteger(parsed) && String(parsed) === trimmed ? parsed : null;
}

function officeCanonicalNameForInput(input: {
  officeName: string;
  district?: string | null;
}): string | null {
  const mapped = mapConnecticutEcrisOffice({ officeSought: input.officeName, district: input.district });
  if (mapped) {
    return mapped.officeCanonicalName;
  }
  const normalized = normalizeConnecticutEcrisOfficeLabel(input.officeName);
  switch (normalized) {
    case "GOVERNOR":
      return "Governor";
    case "LIEUTENANT GOVERNOR":
      return "Lieutenant Governor";
    case "SECRETARY OF STATE":
      return "Secretary of State";
    case "ATTORNEY GENERAL":
      return "Attorney General";
    case "COMPTROLLER":
      return "Comptroller";
    case "STATE TREASURER":
      return "State Treasurer";
    case "STATE SENATOR":
      return "State Senator";
    case "STATE REPRESENTATIVE":
      return "State Lower Chamber Legislator";
    case "STATE LOWER CHAMBER LEGISLATOR":
      return "State Lower Chamber Legislator";
    default:
      return null;
  }
}

function isExpectedLegislativeOffice(officeCanonicalName: string): boolean {
  return officeCanonicalName === "State Senator" || officeCanonicalName === "State Lower Chamber Legislator";
}

function rowMatchesOffice(input: {
  row: ConnecticutEcrisArtifactRow;
  expectedOfficeCanonicalName: string;
  expectedDistrict: string;
}): boolean {
  const mappedOffice = mapConnecticutEcrisOffice({
    officeSought: value(input.row, "Office Sought"),
    district: value(input.row, "District"),
  });
  if (!mappedOffice || mappedOffice.officeCanonicalName !== input.expectedOfficeCanonicalName) {
    return false;
  }
  if (mappedOffice.requiresDistrict) {
    return normalizeDistrict(value(input.row, "District")) === input.expectedDistrict;
  }
  return true;
}

function isCandidateCommittee(row: ConnecticutEcrisArtifactRow): boolean {
  return normalizeTextKey(value(row, "Committee Type")) === "CANDIDATE COMMITTEE";
}

function rowElectionYearMatches(row: ConnecticutEcrisArtifactRow, electionYear: number): boolean {
  return parseElectionYear(value(row, "ElectionYear")) === electionYear;
}

function toCommitteeMatch(input: {
  accumulator: CandidateCommitteeMatchAccumulator;
  sourceUrl: string | null;
}): ConnecticutCandidateCommitteeMatch {
  return {
    committeeId: input.accumulator.committeeId,
    committeeName: input.accumulator.committeeName,
    confidence: "exact",
    source: "ecris_bulk",
    sourceUrl: input.sourceUrl,
    matchedReceiptRowCount: input.accumulator.rows.length,
  };
}

export function resolveConnecticutCandidateCommittee(
  input: ConnecticutCandidateCommitteeResolverInput
): ConnecticutCandidateCommitteeResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeConnecticutCandidateNameKeys(input.candidateName);
  const candidateNameNormalized = [...candidateNameKeys][0] ?? normalizePersonName(input.candidateName);
  const expectedOfficeCanonicalName = officeCanonicalNameForInput({
    officeName: input.officeName,
    district: input.district,
  });
  const officeNameNormalized = expectedOfficeCanonicalName ?? normalizeTextKey(input.officeName);
  const expectedDistrict = normalizeDistrict(input.district);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (!expectedOfficeCanonicalName) {
    return {
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (isExpectedLegislativeOffice(expectedOfficeCanonicalName) && !expectedDistrict) {
    return {
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const rowsByCommittee = new Map<string, CandidateCommitteeMatchAccumulator>();
  for (const row of input.receiptRows) {
    const committeeId = value(row, "Committee ID").toUpperCase();
    const committeeName = value(row, "Committee");
    if (!committeeId || !committeeName) {
      continue;
    }
    if (!isCandidateCommittee(row)) {
      continue;
    }
    if (!rowElectionYearMatches(row, electionYear)) {
      continue;
    }
    if (!rowMatchesOffice({ row, expectedOfficeCanonicalName, expectedDistrict })) {
      continue;
    }
    if (!rowMatchesCandidateName({ row, candidateNameKeys })) {
      continue;
    }

    const accumulator = rowsByCommittee.get(committeeId) ?? {
      committeeId,
      committeeName,
      rows: [],
    };
    accumulator.rows.push(row);
    rowsByCommittee.set(committeeId, accumulator);
  }

  if (rowsByCommittee.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_office_year_match",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const matches = [...rowsByCommittee.values()]
    .map((accumulator) => toCommitteeMatch({ accumulator, sourceUrl: input.sourceUrl ?? null }))
    .sort((left, right) => left.committeeId.localeCompare(right.committeeId));

  if (matches.length === 1) {
    return {
      status: "matched",
      ...matches[0],
    };
  }

  return {
    status: "ambiguous",
    reason: "multiple_matching_committees",
    candidateNameNormalized,
    officeNameNormalized,
    matches,
  };
}
