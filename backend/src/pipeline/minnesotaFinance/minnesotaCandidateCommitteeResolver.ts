import type { MinnesotaCampaignFinanceCsvRow } from "./minnesotaCampaignFinanceArtifactReader.js";
import {
  mapMinnesotaFinanceOffice,
  normalizeMinnesotaFinanceDistrict,
  normalizeMinnesotaFinanceOfficeName,
} from "./minnesotaFinanceEligibleOffices.js";

export type MinnesotaCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  candidateRows: readonly MinnesotaCampaignFinanceCsvRow[];
  sourceUrl?: string | null;
};

export type MinnesotaCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  confidence: "exact";
  source: "mn_board_viewer";
  sourceUrl: string | null;
  matchedCandidateRowCount: number;
};

export type MinnesotaCandidateCommitteeResolution =
  | ({ status: "matched" } & MinnesotaCandidateCommitteeMatch)
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
      matches: MinnesotaCandidateCommitteeMatch[];
    };

type CandidateCommitteeAccumulator = {
  committeeId: string;
  committeeName: string;
  rows: MinnesotaCampaignFinanceCsvRow[];
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Minnesota candidate committee election year: ${value}`);
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

function firstNonEmpty(row: MinnesotaCampaignFinanceCsvRow, keys: readonly string[]): string {
  for (const key of keys) {
    const value = row[key]?.trim() ?? "";
    if (value) {
      return value;
    }
  }
  return "";
}

export function normalizeMinnesotaCandidateNameKeys(value: string): Set<string> {
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
  return [...normalizeMinnesotaCandidateNameKeys(value)][0] ?? normalizePersonName(value);
}

export function normalizeMinnesotaCandidateNameForStorage(value: string): string {
  return candidateNameNormalized(value);
}

function recordCandidateNameKeys(record: MinnesotaCampaignFinanceCsvRow): Set<string> {
  const name = firstNonEmpty(record, [
    "Candidate",
    "Candidate Name",
    "candidate",
    "candidate_name",
    "Recipient",
  ]);
  return normalizeMinnesotaCandidateNameKeys(name);
}

function recordElectionYear(record: MinnesotaCampaignFinanceCsvRow): number | null {
  const raw = firstNonEmpty(record, ["Year", "Election Year", "electionYear", "election_year"]);
  if (!raw) {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed)) {
    return null;
  }
  return parsed;
}

function recordOfficeMatch(input: {
  record: MinnesotaCampaignFinanceCsvRow;
  expectedOfficeName: string;
  expectedDistrict: string;
}): boolean {
  const office = normalizeMinnesotaFinanceOfficeName(input.record["Office"] || input.record["Office Sought"] || "");
  if (!office || office !== input.expectedOfficeName) {
    return false;
  }

  if (input.expectedDistrict) {
    return normalizeMinnesotaFinanceDistrict(input.record["District"] || input.record["Office District"] || "") === input.expectedDistrict;
  }
  return true;
}

function isUsableCandidateRecord(record: MinnesotaCampaignFinanceCsvRow): boolean {
  const status = normalizeTextKey(
    record["Status"] || record["Candidate Status"] || record["Committee Status"] || record["Committee Type"]
  );
  if (!status) {
    return true;
  }
  if (/\b(WITHDREW|WITHDRAWN|TERMINATED|INACTIVE|CLOSED|DECLINED)\b/.test(status)) {
    return false;
  }
  return true;
}

function toCommitteeMatch(input: {
  accumulator: CandidateCommitteeAccumulator;
  sourceUrl: string | null;
}): MinnesotaCandidateCommitteeMatch {
  return {
    committeeId: input.accumulator.committeeId,
    committeeName: input.accumulator.committeeName,
    confidence: "exact",
    source: "mn_board_viewer",
    sourceUrl: input.sourceUrl,
    matchedCandidateRowCount: input.accumulator.rows.length,
  };
}

export function resolveMinnesotaCandidateCommittee(
  input: MinnesotaCandidateCommitteeResolverInput
): MinnesotaCandidateCommitteeResolution {
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateNameKeys = normalizeMinnesotaCandidateNameKeys(input.candidateName);
  const candidateNameKey = candidateNameNormalized(input.candidateName);
  const officeMatch = mapMinnesotaFinanceOffice({
    officeScope: input.officeScope,
    officeName: input.officeName,
    district: input.district,
  });
  const officeNameNormalized = officeMatch?.officeName ?? normalizeTextKey(input.officeName);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const expectedDistrict = officeMatch?.district ?? "";
  const expectedOfficeName = officeMatch?.officeName ?? normalizeTextKey(input.officeName);
  if (!officeMatch) {
    return {
      status: "unmatched",
      reason:
        (input.officeScope === "state_upper" || input.officeScope === "state_lower") &&
        !normalizeMinnesotaFinanceDistrict(input.district)
          ? "missing_legislative_district"
          : "unsupported_office",
      candidateNameNormalized: candidateNameKey,
      officeNameNormalized,
    };
  }

  const rowsByCommittee = new Map<string, CandidateCommitteeAccumulator>();

  for (const record of input.candidateRows) {
    const committeeId = firstNonEmpty(record, [
      "Committee ID",
      "committee_id",
      "Recipient reg num",
      "Reg Num",
      "committeeId",
    ]);
    const committeeName = firstNonEmpty(record, ["Committee Name", "committee_name", "Recipient", "committeeName"]);
    if (!committeeId || !committeeName) {
      continue;
    }
    if (!isUsableCandidateRecord(record)) {
      continue;
    }
    const rowElectionYear = recordElectionYear(record);
    if (rowElectionYear !== null && rowElectionYear !== electionYear) {
      continue;
    }
    const rowCandidateKeys = recordCandidateNameKeys(record);
    if (!rowCandidateKeys.size) {
      continue;
    }
    if (![...rowCandidateKeys].some((key) => candidateNameKeys.has(key))) {
      continue;
    }
    if (!recordOfficeMatch({ record, expectedOfficeName, expectedDistrict })) {
      continue;
    }

    const accumulator = rowsByCommittee.get(committeeId) ?? {
      committeeId,
      committeeName,
      rows: [],
    };
    accumulator.rows.push(record);
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
