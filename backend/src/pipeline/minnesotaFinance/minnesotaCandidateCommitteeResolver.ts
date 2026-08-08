import { hasMiddleNameConflict } from "../finance/personNameMiddleEvidence.js";
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

export type MinnesotaPccRecipientIdentity = {
  candidateName: string;
  officeName:
    | "Governor"
    | "Secretary of State"
    | "Attorney General"
    | "State Auditor"
    | "State Senator"
    | "State Lower Chamber Legislator";
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
    .replace(/\b(J\s*R|S\s*R|II|III|IV|V)\s*$/, " ")
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

const MINNESOTA_PCC_RECIPIENT_PATTERN =
  /^(.*?)\s+(Sec\s+of\s+State|Atty\s+Gen|State\s+Aud|House|Senate|Gov)\s+Committee\s*$/i;

export function parseMinnesotaPccRecipient(
  record: MinnesotaCampaignFinanceCsvRow
): MinnesotaPccRecipientIdentity | null {
  const recipientType = normalizeTextKey(
    firstNonEmpty(record, ["Recipient type", "Recipient Type", "recipient_type"])
  );
  if (recipientType !== "PCC") {
    return null;
  }

  const recipient = firstNonEmpty(record, ["Recipient"]);
  const match = MINNESOTA_PCC_RECIPIENT_PATTERN.exec(recipient);
  const name = match?.[1]?.trim() ?? "";
  const officeSuffix = normalizeTextKey(match?.[2] ?? "");
  const commaIndex = name.indexOf(",");
  if (commaIndex <= 0 || commaIndex === name.length - 1) {
    return null;
  }

  const lastName = name.slice(0, commaIndex).trim();
  const firstNames = name.slice(commaIndex + 1).trim();
  if (!lastName || !firstNames) {
    return null;
  }

  const officeName = (() => {
    switch (officeSuffix) {
      case "GOV":
        return "Governor";
      case "SEC STATE":
        return "Secretary of State";
      case "ATTY GEN":
        return "Attorney General";
      case "STATE AUD":
        return "State Auditor";
      case "SENATE":
        return "State Senator";
      case "HOUSE":
        return "State Lower Chamber Legislator";
      default:
        return null;
    }
  })();

  return officeName
    ? {
        candidateName: `${firstNames} ${lastName}`,
        officeName,
      }
    : null;
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

function recordCandidateName(record: MinnesotaCampaignFinanceCsvRow): string {
  return firstNonEmpty(record, ["Candidate", "Candidate Name", "candidate", "candidate_name", "Recipient"]);
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
  parsedRecipient: MinnesotaPccRecipientIdentity | null;
  expectedOfficeName: string;
  expectedDistrict: string;
}): boolean {
  const office =
    input.parsedRecipient?.officeName ??
    normalizeMinnesotaFinanceOfficeName(input.record["Office"] || input.record["Office Sought"] || "");
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
    officeCanonicalName: input.officeName,
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
    const rawRecipient = firstNonEmpty(record, ["Recipient"]);
    const parsedRecipient = rawRecipient ? parseMinnesotaPccRecipient(record) : null;
    if (rawRecipient && !parsedRecipient) {
      continue;
    }
    const rowElectionYear = recordElectionYear(record);
    if (rowElectionYear !== null && (rowElectionYear < electionYear - 1 || rowElectionYear > electionYear)) {
      continue;
    }
    const rowCandidateName = parsedRecipient ? parsedRecipient.candidateName : recordCandidateName(record);
    const rowCandidateKeys = normalizeMinnesotaCandidateNameKeys(rowCandidateName);
    if (!rowCandidateKeys.size) {
      continue;
    }
    if (![...rowCandidateKeys].some((key) => candidateNameKeys.has(key))) {
      continue;
    }
    // Key overlap collapses names to first+last, so "Jane A. Doe" would take
    // "Doe, Jane B."'s committee as an "exact" match once the office, district,
    // and year agree. A contradicting middle name rejects the row.
    if (
      hasMiddleNameConflict({
        candidateName: input.candidateName,
        rowNames: [rowCandidateName],
        normalizePersonName,
      })
    ) {
      continue;
    }
    if (!recordOfficeMatch({ record, parsedRecipient, expectedOfficeName, expectedDistrict })) {
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
