import { firstNameVariants } from "../finance/personFirstNameNicknames.js";
import { hasMiddleNameConflict } from "../finance/personNameMiddleEvidence.js";
import type { TexasTecFilerRow } from "./texasTecCsvDatabaseReader.js";
import {
  isTexasFinanceEligibleOffice,
  mapTexasTecOfficeCode,
  type TexasFinanceOfficeScope,
} from "./texasFinanceEligibleOffices.js";

export type TexasCandidateCommitteeResolverInput = {
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  filerRows: readonly TexasTecFilerRow[];
  sourceUrl?: string | null;
};

export type TexasCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  receiptCommitteeIds: string[];
  receiptCommittees: TexasCandidateReceiptCommittee[];
  confidence: "exact";
  source: "tec_bulk";
  sourceUrl: string | null;
  matchedFilerRowCount: number;
};

export type TexasCandidateReceiptCommittee = {
  committeeId: string;
  committeeName: string;
  relationship: "candidate_filer" | "campaign_named_committee";
};

export type TexasCandidateCommitteeResolution =
  | ({ status: "matched" } & TexasCandidateCommitteeMatch)
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
      matches: TexasCandidateCommitteeMatch[];
    };

type CandidateCommitteeAccumulator = {
  committeeId: string;
  committeeName: string;
  rows: TexasTecFilerRow[];
};

function normalizeElectionYear(value: number): number {
  // This module relies on the TEC bulk electronic filing data shape we support from 2014 onward.
  if (!Number.isInteger(value) || value < 2014 || value > 2100) {
    throw new Error(`Invalid Texas candidate committee election year: ${value}`);
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
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePhraseText(value: string): string {
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
    // Bare "V" is a middle initial, not a suffix (GENERATIONAL_SUFFIX_RANK in
    // finance/personNameMiddleEvidence.ts): stripping it erased middle evidence.
    .replace(/\b(JR|SR|II|III|IV)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Nickname expansion is one-sided by design: only the VoteApp candidate name
 * may opt in via `expandNicknames`, while TEC-sourced names always key
 * literally. Expanding both sides would let two distinct formal names meet at
 * a shared nickname key ("Patrick Smith" and "Patricia Smith" both produce
 * "PAT SMITH").
 */
export function normalizeTexasCandidateNameKeys(
  value: string,
  options?: { expandNicknames?: boolean }
): Set<string> {
  const expandNicknames = options?.expandNicknames === true;
  const trimmed = value.trim();
  const normalized = normalizePersonName(trimmed);
  const keys = new Set<string>();
  if (normalized) {
    keys.add(normalized);
  }

  function addFirstLastKeys(parts: readonly string[]): void {
    if (parts.length < 2) {
      return;
    }
    const firstName = parts[0]!;
    const lastName = parts[parts.length - 1]!;
    keys.add(`${firstName} ${lastName}`);
    if (!expandNicknames) {
      return;
    }
    for (const variant of firstNameVariants(firstName)) {
      keys.add(`${variant} ${lastName}`);
    }
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
      addFirstLastKeys(flipped.split(" ").filter(Boolean));
    }
    return keys;
  }

  addFirstLastKeys(normalized.split(" ").filter(Boolean));

  return keys;
}

/**
 * Middle-name evidence gate for TEC person names. Nickname expansion is
 * one-sided (see normalizeTexasCandidateNameKeys), so the first-name
 * comparison expands the VoteApp side only - otherwise "Mike A. Smith" would
 * never line up with "SMITH, MICHAEL B" and the contradicting middle would go
 * unseen.
 */
export function texasCandidateNameMiddleConflicts(input: {
  candidateName: string;
  rowNames: readonly string[];
}): boolean {
  return hasMiddleNameConflict({
    candidateName: input.candidateName,
    rowNames: input.rowNames,
    normalizePersonName,
    firstNamesEquivalent: (candidateFirst, rowFirst) =>
      candidateFirst === rowFirst || firstNameVariants(candidateFirst).includes(rowFirst),
  });
}

function normalizeOfficeScope(value: string): TexasFinanceOfficeScope | null {
  const normalized = value.trim();
  return normalized === "statewide" || normalized === "state_upper" || normalized === "state_lower"
    ? normalized
    : null;
}

function canonicalOfficeNameForInput(officeName: string): string | null {
  switch (normalizeTextKey(officeName)) {
    case "GOVERNOR":
      return "Governor";
    case "LIEUTENANT GOVERNOR":
    case "LT GOVERNOR":
      return "Lieutenant Governor";
    case "ATTORNEY GENERAL":
      return "Attorney General";
    case "COMPTROLLER":
    case "COMPTROLLER PUBLIC ACCOUNTS":
      return "Comptroller";
    case "AGRICULTURE COMMISSIONER":
    case "COMMISSIONER AGRICULTURE":
      return "Agriculture Commissioner";
    case "LAND COMMISSIONER":
    case "COMMISSIONER GENERAL LAND OFFICE":
      return "Land Commissioner";
    case "RAILROAD COMMISSIONER":
    case "RAILROAD COMMISSION":
      return "Railroad Commissioner";
    case "STATE SENATOR":
    case "STATE SENATE":
      return "State Senator";
    case "STATE REPRESENTATIVE":
    case "STATE HOUSE":
    case "STATE LOWER CHAMBER LEGISLATOR":
      return "State Lower Chamber Legislator";
    default:
      return null;
  }
}

function isExpectedLegislativeOffice(officeScope: string | null, officeCanonicalName: string | null): boolean {
  return (
    (officeScope === "state_upper" && officeCanonicalName === "State Senator") ||
    (officeScope === "state_lower" && officeCanonicalName === "State Lower Chamber Legislator")
  );
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

function filerRowPersonNames(row: TexasTecFilerRow): string[] {
  return [[row.filerNameFirst, row.filerNameLast].filter(Boolean).join(" "), row.filerName].filter(Boolean);
}

function candidateNameKeysFromFilerRow(row: TexasTecFilerRow): Set<string> {
  const keys = new Set<string>();
  for (const name of filerRowPersonNames(row)) {
    for (const key of normalizeTexasCandidateNameKeys(name)) {
      keys.add(key);
    }
  }
  return keys;
}

function rowMatchesCandidateName(input: {
  row: TexasTecFilerRow;
  candidateName: string;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  let keyMatched = false;
  for (const key of candidateNameKeysFromFilerRow(input.row)) {
    if (input.candidateNameKeys.has(key)) {
      keyMatched = true;
      break;
    }
  }
  if (!keyMatched) {
    return false;
  }
  // Key overlap collapses names to first+last, which would link
  // "Greg W. Abbott" to "ABBOTT, GREG R" as an "exact" match whenever office,
  // district, and year agree. A contradicting middle name rejects the row.
  return !texasCandidateNameMiddleConflicts({
    candidateName: input.candidateName,
    rowNames: filerRowPersonNames(input.row),
  });
}

function rowOfficeCanonicalName(row: TexasTecFilerRow): string | null {
  const mapped = mapTexasTecOfficeCode({ officeCode: row.contestSeekOfficeCd });
  if (mapped) {
    return mapped.officeCanonicalName;
  }
  return canonicalOfficeNameForInput(row.contestSeekOfficeDescr);
}

function rowMatchesExpectedOfficeDistrict(input: {
  row: TexasTecFilerRow;
  officeScope: TexasFinanceOfficeScope;
  officeCanonicalName: string;
  expectedDistrict: string;
}): boolean {
  const mapping = mapTexasTecOfficeCode({ officeCode: input.row.contestSeekOfficeCd });
  if (mapping) {
    if (mapping.officeScope !== input.officeScope || mapping.officeCanonicalName !== input.officeCanonicalName) {
      return false;
    }
  } else if (rowOfficeCanonicalName(input.row) !== input.officeCanonicalName) {
    return false;
  }

  if (isExpectedLegislativeOffice(input.officeScope, input.officeCanonicalName)) {
    return normalizeDistrict(input.row.contestSeekOfficeDistrict) === input.expectedDistrict;
  }
  return true;
}

function isCandidateOfficeholderFiler(row: TexasTecFilerRow): boolean {
  return normalizeTextKey(row.filerTypeCd) === "COH";
}

function committeeNameFromRow(row: TexasTecFilerRow): string {
  return row.filerName.trim();
}

function containsPhrase(input: { text: string; phrase: string }): boolean {
  return ` ${input.text} `.includes(` ${input.phrase} `);
}

function isOppositionCommitteeName(committeeName: string): boolean {
  const normalized = normalizePhraseText(committeeName);
  return /\b(AGAINST|ANTI|BEAT|DEFEAT|NO TO|OPPOSE|OPPOSES|STOP|VICTORY OVER)\b/.test(normalized);
}

function isSafeCampaignNamedCommittee(input: {
  row: TexasTecFilerRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  if (isCandidateOfficeholderFiler(input.row)) {
    return false;
  }

  const committeeName = committeeNameFromRow(input.row);
  if (!committeeName || isOppositionCommitteeName(committeeName)) {
    return false;
  }

  const normalized = normalizePhraseText(committeeName);
  for (const candidateNameKey of input.candidateNameKeys) {
    const phrase = normalizePhraseText(candidateNameKey);
    if (!phrase) {
      continue;
    }
    if (containsPhrase({ text: normalized, phrase }) && /\b(FOR|CAMPAIGN)\b/.test(normalized)) {
      return true;
    }
  }
  return false;
}

function uniqueReceiptCommittees(receiptCommittees: TexasCandidateReceiptCommittee[]): TexasCandidateReceiptCommittee[] {
  const seen = new Set<string>();
  const result: TexasCandidateReceiptCommittee[] = [];
  for (const receiptCommittee of receiptCommittees) {
    const committeeId = receiptCommittee.committeeId.trim().toUpperCase();
    const committeeName = receiptCommittee.committeeName.trim();
    if (!committeeId || !committeeName || seen.has(committeeId)) {
      continue;
    }
    seen.add(committeeId);
    result.push({
      committeeId,
      committeeName,
      relationship: receiptCommittee.relationship,
    });
  }
  return result.sort(
    (left, right) =>
      (left.relationship === "candidate_filer" ? 0 : 1) -
        (right.relationship === "candidate_filer" ? 0 : 1) || left.committeeId.localeCompare(right.committeeId)
  );
}

function collectReceiptCommittees(input: {
  accumulator: CandidateCommitteeAccumulator;
  candidateNameKeys: ReadonlySet<string>;
  filerRows: readonly TexasTecFilerRow[];
}): TexasCandidateReceiptCommittee[] {
  const receiptCommittees: TexasCandidateReceiptCommittee[] = [
    {
      committeeId: input.accumulator.committeeId,
      committeeName: input.accumulator.committeeName,
      relationship: "candidate_filer",
    },
  ];

  for (const row of input.filerRows) {
    const committeeId = row.filerIdent.trim().toUpperCase();
    const committeeName = committeeNameFromRow(row);
    if (!committeeId || !committeeName || committeeId === input.accumulator.committeeId) {
      continue;
    }
    if (!isSafeCampaignNamedCommittee({ row, candidateNameKeys: input.candidateNameKeys })) {
      continue;
    }
    receiptCommittees.push({
      committeeId,
      committeeName,
      relationship: "campaign_named_committee",
    });
  }

  return uniqueReceiptCommittees(receiptCommittees);
}

function toCommitteeMatch(input: {
  accumulator: CandidateCommitteeAccumulator;
  candidateNameKeys: ReadonlySet<string>;
  filerRows: readonly TexasTecFilerRow[];
  sourceUrl: string | null;
}): TexasCandidateCommitteeMatch {
  const receiptCommittees = collectReceiptCommittees({
    accumulator: input.accumulator,
    candidateNameKeys: input.candidateNameKeys,
    filerRows: input.filerRows,
  });
  return {
    committeeId: input.accumulator.committeeId,
    committeeName: input.accumulator.committeeName,
    receiptCommitteeIds: receiptCommittees.map((committee) => committee.committeeId),
    receiptCommittees,
    confidence: "exact",
    source: "tec_bulk",
    sourceUrl: input.sourceUrl,
    matchedFilerRowCount: input.accumulator.rows.length,
  };
}

export function resolveTexasCandidateCommittee(
  input: TexasCandidateCommitteeResolverInput
): TexasCandidateCommitteeResolution {
  normalizeElectionYear(input.electionYear);
  const officeScope = normalizeOfficeScope(input.officeScope);
  const officeCanonicalName = canonicalOfficeNameForInput(input.officeName);
  const officeNameNormalized = officeCanonicalName ?? normalizeTextKey(input.officeName);
  const candidateNameKeys = normalizeTexasCandidateNameKeys(input.candidateName, { expandNicknames: true });
  const candidateNameNormalized = [...candidateNameKeys][0] ?? normalizePersonName(input.candidateName);
  const expectedDistrict = normalizeDistrict(input.district);

  if (candidateNameKeys.size === 0) {
    return {
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (!officeScope || !officeCanonicalName || !isTexasFinanceEligibleOffice({ officeScope, officeCanonicalName })) {
    return {
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (isExpectedLegislativeOffice(officeScope, officeCanonicalName) && !expectedDistrict) {
    return {
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }

  const rowsByCommittee = new Map<string, CandidateCommitteeAccumulator>();
  for (const row of input.filerRows) {
    const committeeId = row.filerIdent.trim().toUpperCase();
    const committeeName = committeeNameFromRow(row);
    if (!committeeId || !committeeName) {
      continue;
    }
    if (!isCandidateOfficeholderFiler(row)) {
      continue;
    }
    if (!rowMatchesCandidateName({ row, candidateName: input.candidateName, candidateNameKeys })) {
      continue;
    }
    if (
      !rowMatchesExpectedOfficeDistrict({
        row,
        officeScope,
        officeCanonicalName,
        expectedDistrict,
      })
    ) {
      continue;
    }

    const existing = rowsByCommittee.get(committeeId);
    if (existing) {
      existing.rows.push(row);
      continue;
    }
    rowsByCommittee.set(committeeId, {
      committeeId,
      committeeName,
      rows: [row],
    });
  }

  const matches = [...rowsByCommittee.values()]
    .map((accumulator) =>
      toCommitteeMatch({
        accumulator,
        candidateNameKeys,
        filerRows: input.filerRows,
        sourceUrl: input.sourceUrl ?? null,
      })
    )
    .sort((left, right) => left.committeeId.localeCompare(right.committeeId));

  if (matches.length === 0) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  if (matches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized,
      officeNameNormalized,
      matches,
    };
  }

  const match = matches[0];
  if (!match) {
    return {
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized,
      officeNameNormalized,
    };
  }
  return {
    status: "matched",
    ...match,
  };
}
