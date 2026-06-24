import {
  type MichiganMitnOfficeSearchInput,
  normalizeMichiganMitnLegislativeDistrict,
  toMichiganMitnOfficeSearchInput,
} from "./michiganFinanceEligibleOffices.js";
import { normalizeMichiganMitnLegacyArchiveYear } from "./michiganMitnLegacyArtifactCache.js";
import type { MichiganMitnLegacyContributionRow } from "./michiganMitnLegacyArchiveReader.js";

export type MichiganCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  contributionRows: readonly MichiganMitnLegacyContributionRow[];
  sourceUrl?: string | null;
};

export type MichiganCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  commonName: string | null;
  confidence: "exact";
  source: "mitn_legacy";
  sourceUrl: string | null;
  matchedContributionRowCount: number;
};

export type MichiganCandidateCommitteeResolution =
  | ({ status: "matched" } & MichiganCandidateCommitteeMatch)
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
      matches: MichiganCandidateCommitteeMatch[];
    };

type CandidateCommitteeAccumulator = {
  committeeId: string;
  committeeName: string;
  commonName: string | null;
  rows: MichiganMitnLegacyContributionRow[];
};

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR|COMMITTEE|FRIENDS|TO|ELECT)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePersonName(value: string | null | undefined): string {
  return normalizeTextKey(value)
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeMichiganCandidateNameKeys(value: string): Set<string> {
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
  return [...normalizeMichiganCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

export function normalizeMichiganCandidateNameForStorage(value: string): string {
  return candidateNameNormalized(value);
}

function candidateNameKeysFromContributionRow(row: MichiganMitnLegacyContributionRow): Set<string> {
  const structuredName = [row.can_first_name, row.can_last_name].filter(Boolean).join(" ");
  return normalizeMichiganCandidateNameKeys(structuredName);
}

function rowMatchesCandidateName(input: {
  row: MichiganMitnLegacyContributionRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  for (const key of candidateNameKeysFromContributionRow(input.row)) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function committeeNameFromRow(row: MichiganMitnLegacyContributionRow): string {
  return row.com_legal_name.trim() || row.common_name.trim();
}

function isLikelyCandidateCommitteeRow(row: MichiganMitnLegacyContributionRow): boolean {
  if (!row.cfr_com_id.trim() || !committeeNameFromRow(row)) {
    return false;
  }
  if (!row.can_first_name.trim() || !row.can_last_name.trim()) {
    return false;
  }
  const committeeType = normalizeTextKey(row.com_type);
  if (/\b(?:BALLOT|PAC|INDEPENDENT|POLITICAL|PARTY|CAUCUS|SUPERPAC|SUPER PAC)\b/.test(committeeType)) {
    return false;
  }
  return true;
}

function officeAliasesForSearchInput(officeSearchInput: MichiganMitnOfficeSearchInput): string[] {
  switch (officeSearchInput.mitnOffice) {
    case "State Senate":
      return ["STATE SENATE", "SENATE", "SENATOR"];
    case "State House":
      return ["STATE HOUSE", "HOUSE", "REPRESENTATIVE"];
    default:
      return [officeSearchInput.mitnOffice.toUpperCase()];
  }
}

function districtAliasesForSearchInput(officeSearchInput: MichiganMitnOfficeSearchInput): string[] {
  if (!officeSearchInput.district) {
    return [];
  }
  const district = officeSearchInput.district.replace(/^0+/, "");
  if (officeSearchInput.mitnOffice === "State Senate") {
    return [
      `STATE SENATE ${district}`,
      `SENATE DISTRICT ${district}`,
      `SENATE DIST ${district}`,
      `SENATE ${district}`,
      `SD ${district}`,
      `DISTRICT ${district}`,
      `DIST ${district}`,
    ];
  }
  return [
    `STATE HOUSE ${district}`,
    `HOUSE DISTRICT ${district}`,
    `HOUSE DIST ${district}`,
    `HOUSE ${district}`,
    `HD ${district}`,
    `DISTRICT ${district}`,
    `DIST ${district}`,
  ];
}

function rowOfficeCompatibilityText(row: MichiganMitnLegacyContributionRow): string {
  return normalizeTextKey([committeeNameFromRow(row), row.common_name, row.com_type, row.extra_desc].join(" "));
}

function rowMatchesOfficeContext(input: {
  row: MichiganMitnLegacyContributionRow;
  officeSearchInput: MichiganMitnOfficeSearchInput;
}): boolean {
  const text = rowOfficeCompatibilityText(input.row);
  if (!officeAliasesForSearchInput(input.officeSearchInput).some((alias) => text.includes(normalizeTextKey(alias)))) {
    return false;
  }
  const districtAliases = districtAliasesForSearchInput(input.officeSearchInput);
  return districtAliases.length === 0 || districtAliases.some((alias) => text.includes(normalizeTextKey(alias)));
}

function isLegislativeInput(input: { officeScope: string; officeName: string }): boolean {
  return (
    (input.officeScope === "state_upper" && input.officeName.trim() === "State Senator") ||
    (input.officeScope === "state_lower" && input.officeName.trim() === "State Lower Chamber Legislator")
  );
}

function hasValidLegislativeDistrict(input: { officeScope: string; officeName: string; district?: string | null }): boolean {
  if (input.officeScope === "state_upper" && input.officeName.trim() === "State Senator") {
    return normalizeMichiganMitnLegislativeDistrict(input.district, 38) !== null;
  }
  if (input.officeScope === "state_lower" && input.officeName.trim() === "State Lower Chamber Legislator") {
    return normalizeMichiganMitnLegislativeDistrict(input.district, 110) !== null;
  }
  return true;
}

function toCommitteeMatch(input: {
  accumulator: CandidateCommitteeAccumulator;
  sourceUrl: string | null;
}): MichiganCandidateCommitteeMatch {
  return {
    committeeId: input.accumulator.committeeId,
    committeeName: input.accumulator.committeeName,
    commonName: input.accumulator.commonName,
    confidence: "exact",
    source: "mitn_legacy",
    sourceUrl: input.sourceUrl,
    matchedContributionRowCount: input.accumulator.rows.length,
  };
}

export function resolveMichiganCandidateCommittee(
  input: MichiganCandidateCommitteeResolverInput
): MichiganCandidateCommitteeResolution {
  normalizeMichiganMitnLegacyArchiveYear(input.electionYear);
  const candidateNameKeys = normalizeMichiganCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeSearchInput = toMichiganMitnOfficeSearchInput({
    officeScope: input.officeScope,
    officeCanonicalName: input.officeName,
    district: input.district,
  });
  const officeNameNormalized = officeSearchInput?.mitnOffice ?? normalizeTextKey(input.officeName);

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
      reason:
        isLegislativeInput(input) && !hasValidLegislativeDistrict(input)
          ? "missing_legislative_district"
          : "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const rowsByCommittee = new Map<string, CandidateCommitteeAccumulator>();
  for (const row of input.contributionRows) {
    const committeeId = row.cfr_com_id.trim().toUpperCase();
    const committeeName = committeeNameFromRow(row);
    if (!committeeId || !committeeName) {
      continue;
    }
    if (!isLikelyCandidateCommitteeRow(row)) {
      continue;
    }
    if (!rowMatchesCandidateName({ row, candidateNameKeys })) {
      continue;
    }
    if (!rowMatchesOfficeContext({ row, officeSearchInput })) {
      continue;
    }

    const accumulator = rowsByCommittee.get(committeeId) ?? {
      committeeId,
      committeeName,
      commonName: row.common_name.trim() || null,
      rows: [],
    };
    accumulator.rows.push(row);
    rowsByCommittee.set(committeeId, accumulator);
  }

  if (rowsByCommittee.size === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const sourceUrl = input.sourceUrl?.trim() || null;
  const matches = [...rowsByCommittee.values()]
    .map((accumulator) => toCommitteeMatch({ accumulator, sourceUrl }))
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
    candidateNameNormalized: candidateNameKey,
    officeNameNormalized,
    matches,
  };
}
