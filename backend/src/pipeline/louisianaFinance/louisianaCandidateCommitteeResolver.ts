import { personNamesMatchWithMiddleEvidence } from "../finance/personNameMiddleEvidence.js";
import type { LouisianaCampaignFinanceCsvRow } from "./louisianaCampaignFinanceArtifactReader.js";
import {
  mapLouisianaFinanceOffice,
  normalizeLouisianaFinanceDistrict,
  normalizeLouisianaFinanceOfficeName,
} from "./louisianaFinanceEligibleOffices.js";

export type LouisianaCandidateCommitteeRow = LouisianaCampaignFinanceCsvRow & Record<string, string | undefined>;

export type LouisianaCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  candidateRows: readonly LouisianaCandidateCommitteeRow[];
  sourceUrl?: string | null;
};

export type LouisianaCandidateCommitteeMatch = {
  filerNumber: string;
  filerName: string;
  candidateName: string;
  officeName: string;
  district: string | null;
  confidence: "exact";
  source: "la_ethics_search";
  sourceUrl: string | null;
  matchedCandidateRowCount: number;
};

export type LouisianaCandidateCommitteeResolution =
  | ({ status: "matched" } & LouisianaCandidateCommitteeMatch)
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
      matches: LouisianaCandidateCommitteeMatch[];
    };

type CandidateCommitteeAccumulator = {
  filerNumber: string;
  filerName: string;
  candidateName: string;
  officeName: string;
  district: string | null;
  rows: LouisianaCandidateCommitteeRow[];
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Louisiana candidate committee election year: ${value}`);
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
    .replace(/\b(THE|OF|FOR|TO|ELECT|COMMITTEE|FRIENDS|CITIZENS|CAMPAIGN)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeLouisianaCandidateNameKeys(value: string): Set<string> {
  const trimmed = value.trim();
  const keys = new Set<string>();

  function addName(raw: string): void {
    const normalized = normalizePersonName(raw);
    if (normalized) {
      keys.add(normalized);
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

export function normalizeLouisianaCandidateNameForStorage(value: string): string {
  return [...normalizeLouisianaCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

function firstNonEmpty(row: LouisianaCandidateCommitteeRow, keys: readonly string[]): string {
  for (const key of keys) {
    const value = row[key]?.trim() ?? "";
    if (value) {
      return value;
    }
  }
  return "";
}

function candidateNameFromRow(row: LouisianaCandidateCommitteeRow): string {
  const explicit = firstNonEmpty(row, [
    "CandidateName",
    "Candidate Name",
    "Candidate",
    "FilerName",
    "Filer Name",
  ]);
  if (explicit) {
    return explicit;
  }

  const firstName = firstNonEmpty(row, ["FilerFirstName", "Filer First Name", "FirstName", "First Name"]);
  const lastName = firstNonEmpty(row, ["FilerLastName", "Filer Last Name", "LastName", "Last Name"]);
  return [firstName, lastName].filter(Boolean).join(" ");
}

function displayFilerName(row: LouisianaCandidateCommitteeRow): string {
  const explicit = firstNonEmpty(row, ["FilerName", "Filer Name", "CommitteeName", "Committee Name"]);
  if (explicit) {
    return explicit;
  }
  const firstName = firstNonEmpty(row, ["FilerFirstName", "Filer First Name", "FirstName", "First Name"]);
  const lastName = firstNonEmpty(row, ["FilerLastName", "Filer Last Name", "LastName", "Last Name"]);
  if (lastName && firstName) {
    return `${lastName}, ${firstName}`;
  }
  return lastName || firstName;
}

function rowCandidateNameKeys(row: LouisianaCandidateCommitteeRow): Set<string> {
  return normalizeLouisianaCandidateNameKeys(candidateNameFromRow(row));
}

function rowMatchesCandidateName(input: {
  row: LouisianaCandidateCommitteeRow;
  candidateName: string;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  for (const key of rowCandidateNameKeys(input.row)) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  // The key set carries only full-string and comma-flip keys, so a name that
  // differs in middle content on one side ("John A. Smith" vs "Smith, John")
  // never overlaps and the link silently strands. Recover it through the
  // middle-evidence gate: first+last alignment matches unless the middles
  // contradict.
  return personNamesMatchWithMiddleEvidence({
    candidateName: input.candidateName,
    rowNames: [candidateNameFromRow(input.row)],
    normalizePersonName,
  });
}

function rowElectionYears(row: LouisianaCandidateCommitteeRow): Set<number> {
  const years = new Set<number>();
  for (const key of ["ElectionYear", "Election Year", "Year", "ReportYear", "Report Year"]) {
    const raw = row[key]?.trim() ?? "";
    if (/^\d{4}$/.test(raw)) {
      years.add(Number.parseInt(raw, 10));
    }
  }
  for (const key of [
    "Election",
    "ElectionDate",
    "Election Date",
    "ContributionDate",
    "Contribution Date",
    "ExpenditureDate",
    "Expenditure Date",
  ]) {
    const raw = row[key]?.trim() ?? "";
    for (const match of raw.matchAll(/\b(20\d{2})\b/g)) {
      if (match[1]) {
        years.add(Number.parseInt(match[1], 10));
      }
    }
  }
  return years;
}

function rowMatchesElectionYear(row: LouisianaCandidateCommitteeRow, electionYear: number): boolean {
  const years = rowElectionYears(row);
  if (years.size === 0) {
    return false;
  }
  for (const year of years) {
    if (year >= electionYear - 1 && year <= electionYear) {
      return true;
    }
  }
  return false;
}

function rowMatchesOfficeContext(input: {
  row: LouisianaCandidateCommitteeRow;
  officeName: string;
  district: string | null;
}): boolean {
  const rawOffice = firstNonEmpty(input.row, [
    "OfficeSought",
    "Office Sought",
    "OfficeName",
    "Office Name",
    "Office",
    "CandidateOffice",
    "Candidate Office",
  ]);
  const rowOffice = normalizeLouisianaFinanceOfficeName(rawOffice);
  if (rawOffice && rowOffice !== null && rowOffice !== input.officeName) {
    return false;
  }

  const rawDistrict = firstNonEmpty(input.row, [
    "District",
    "DistrictNumber",
    "District Number",
    "OfficeDistrict",
    "Office District",
  ]);
  const rowDistrict = normalizeLouisianaFinanceDistrict(rawDistrict);
  if (rowDistrict && input.district && rowDistrict !== input.district) {
    return false;
  }

  return true;
}

function isLikelyCandidateFiler(row: LouisianaCandidateCommitteeRow): boolean {
  const candidateName = candidateNameFromRow(row);
  const filerName = displayFilerName(row);
  if (!candidateName || !filerName) {
    return false;
  }

  const filerType = normalizeTextKey(firstNonEmpty(row, ["FilerType", "Filer Type", "CommitteeType", "Committee Type"]));
  if (filerType && /\b(PAC|POLITICAL ACTION|PARTY|RECALL|BALLOT|REFERENDUM|OTHER PERSON)\b/.test(filerType)) {
    return false;
  }

  const nameKey = normalizeTextKey(filerName);
  if (/\b(PAC|POLITICAL ACTION|PARTY|RECALL|BALLOT|REFERENDUM|SUPER PAC|SUPERPAC)\b/.test(nameKey)) {
    return false;
  }

  return true;
}

function toCommitteeMatch(input: {
  accumulator: CandidateCommitteeAccumulator;
  sourceUrl: string | null;
}): LouisianaCandidateCommitteeMatch {
  return {
    filerNumber: input.accumulator.filerNumber,
    filerName: input.accumulator.filerName,
    candidateName: input.accumulator.candidateName,
    officeName: input.accumulator.officeName,
    district: input.accumulator.district,
    confidence: "exact",
    source: "la_ethics_search",
    sourceUrl: input.sourceUrl,
    matchedCandidateRowCount: input.accumulator.rows.length,
  };
}

function isLegislativeInput(input: { officeScope: string; officeName: string }): boolean {
  const officeName = normalizeLouisianaFinanceOfficeName(input.officeName);
  return (
    (input.officeScope === "state_upper" && officeName === "State Senator") ||
    (input.officeScope === "state_lower" && officeName === "State Lower Chamber Legislator")
  );
}

export function resolveLouisianaCandidateCommittee(
  input: LouisianaCandidateCommitteeResolverInput
): LouisianaCandidateCommitteeResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeLouisianaCandidateNameKeys(input.candidateName);
  const candidateNameNormalized = normalizeLouisianaCandidateNameForStorage(input.candidateName);
  const officeSearchInput = mapLouisianaFinanceOffice({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    district: input.district,
  });
  const officeNameNormalized = officeSearchInput?.officeName ?? normalizeLouisianaFinanceOfficeName(input.officeName) ?? normalizeTextKey(input.officeName);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (!officeSearchInput) {
    return {
      status: "unmatched",
      reason: isLegislativeInput(input) && !normalizeLouisianaFinanceDistrict(input.district)
        ? "missing_legislative_district"
        : "unsupported_office",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const rowsByFilerNumber = new Map<string, CandidateCommitteeAccumulator>();
  for (const row of input.candidateRows) {
    const filerNumber = firstNonEmpty(row, ["FilerNumber", "Filer Number", "FilerID", "Filer ID"]).replace(/\s+/g, "");
    if (!filerNumber) {
      continue;
    }
    if (!isLikelyCandidateFiler(row)) {
      continue;
    }
    if (!rowMatchesElectionYear(row, electionYear)) {
      continue;
    }
    if (!rowMatchesCandidateName({ row, candidateName: input.candidateName, candidateNameKeys })) {
      continue;
    }
    if (!rowMatchesOfficeContext({ row, officeName: officeSearchInput.officeName, district: officeSearchInput.district })) {
      continue;
    }

    const accumulator = rowsByFilerNumber.get(filerNumber) ?? {
      filerNumber,
      filerName: displayFilerName(row),
      candidateName: candidateNameFromRow(row),
      officeName: officeSearchInput.officeName,
      district: officeSearchInput.district,
      rows: [],
    };
    accumulator.rows.push(row);
    rowsByFilerNumber.set(filerNumber, accumulator);
  }

  if (rowsByFilerNumber.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const sourceUrl = input.sourceUrl?.trim() || null;
  const matches = [...rowsByFilerNumber.values()]
    .map((accumulator) => toCommitteeMatch({ accumulator, sourceUrl }))
    .sort((left, right) => left.filerNumber.localeCompare(right.filerNumber));

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
