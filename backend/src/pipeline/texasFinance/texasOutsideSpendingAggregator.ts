import { firstNamesConflict } from "../finance/personFirstNameNicknames.js";
import { normalizeTexasCandidateNameKeys } from "./texasCandidateCommitteeResolver.js";
import {
  isTexasFinanceEligibleOffice,
  mapTexasTecOfficeCode,
  type TexasFinanceOfficeScope,
} from "./texasFinanceEligibleOffices.js";
import type {
  TexasTecCandidateRow,
  TexasTecExpenditureRow,
  TexasTecSpacRow,
} from "./texasTecCsvDatabaseReader.js";

export type TexasSupportOppose = "support" | "oppose";

export type TexasOutsideSpendingGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: TexasSupportOppose;
  amount: number;
  sourceUrl: string | null;
};

export type TexasOutsideSpendingSummary = {
  supportTotal: number;
  opposeTotal: number;
  groups: TexasOutsideSpendingGroup[];
  sourceUrl: string | null;
};

export type TexasOutsideSpendingAggregationInput = {
  candidateName: string;
  candidateCommitteeId: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
  candidateRows: readonly TexasTecCandidateRow[];
  expenditureRows: readonly TexasTecExpenditureRow[];
  spacRows: readonly TexasTecSpacRow[];
  sourceUrl?: string | null;
  maxGroups?: number;
};

export type TexasOutsideSpendingAggregationResult = {
  summary: TexasOutsideSpendingSummary | null;
  matchedCandidateExpenditureRowCount: number;
  includedCandidateExpenditureRowCount: number;
  skippedCandidateExpenditureRowCount: number;
};

type SpacRelationship = {
  committeeId: string;
  committeeName: string;
  supportOppose: TexasSupportOppose;
};

type GroupAccumulator = {
  committeeId: string;
  committeeName: string;
  supportOppose: TexasSupportOppose;
  amountCents: number;
};

const DEFAULT_MAX_GROUPS = 50;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2014 || value > 2100) {
    throw new Error(`Invalid Texas outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Texas outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
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

function normalizeOfficeScope(value: string): TexasFinanceOfficeScope | null {
  const normalized = value.trim();
  return normalized === "statewide" || normalized === "state_upper" || normalized === "state_lower"
    ? normalized
    : null;
}

function isLegislativeOffice(officeScope: TexasFinanceOfficeScope, officeCanonicalName: string): boolean {
  return (
    (officeScope === "state_upper" && officeCanonicalName === "State Senator") ||
    (officeScope === "state_lower" && officeCanonicalName === "State Lower Chamber Legislator")
  );
}

function rowOfficeMatches(input: {
  officeCode: string;
  officeDescription: string;
  district: string;
  officeScope: TexasFinanceOfficeScope;
  officeCanonicalName: string;
  expectedDistrict: string;
}): boolean {
  const mapping = mapTexasTecOfficeCode({ officeCode: input.officeCode });
  if (mapping) {
    if (mapping.officeScope !== input.officeScope || mapping.officeCanonicalName !== input.officeCanonicalName) {
      return false;
    }
  } else if (canonicalOfficeNameForInput(input.officeDescription) !== input.officeCanonicalName) {
    return false;
  }

  if (isLegislativeOffice(input.officeScope, input.officeCanonicalName)) {
    return normalizeDistrict(input.district) === input.expectedDistrict;
  }
  return true;
}

function parseAmountCents(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (normalized.length === 0 || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
    return null;
  }

  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseTexasTecDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const compactMatch = /^(\d{4})(\d{2})(\d{2})$/.exec(trimmed);
  if (compactMatch) {
    return Number(compactMatch[1]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  return null;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseTexasTecDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

export function supportOpposeFromTexasSpacPosition(value: string): TexasSupportOppose | null {
  const normalized = normalizeTextKey(value);
  if (normalized === "SUPPORT" || normalized === "SUPPORTED") {
    return "support";
  }
  if (normalized === "OPPOSE" || normalized === "OPPOSED") {
    return "oppose";
  }
  return null;
}

function isInfoOnly(row: TexasTecExpenditureRow): boolean {
  const normalized = normalizeTextKey(row.infoOnlyFlag);
  return normalized === "Y" || normalized === "YES" || normalized === "TRUE" || normalized === "1";
}

function candidateRowFirstNameToken(row: TexasTecCandidateRow): string | null {
  const token = normalizeTextKey(row.candidateNameFirst).split(" ")[0] ?? "";
  return token.length > 0 ? token : null;
}

// Purpose rows naming two conflicting first names (PATRICK and PATRICIA)
// are positive evidence that the nickname-expanded key set caught two
// distinct people; refuse the whole aggregation rather than pick a side,
// mirroring the committee resolver's both-families-filed rule. Only rows
// from spenders holding a usable SPAC position on the linked committee are
// consulted: no other row can contribute money, so an unrelated spender's
// stray row must not zero out valid totals. Formal spellings of one name
// (STEPHEN/STEVEN) do not conflict.
function matchedRowsSpanConflictingFirstNames(rows: readonly TexasTecCandidateRow[]): boolean {
  const seen: string[] = [];
  for (const row of rows) {
    const token = candidateRowFirstNameToken(row);
    if (!token || seen.includes(token)) {
      continue;
    }
    if (seen.some((existing) => firstNamesConflict(existing, token))) {
      return true;
    }
    seen.push(token);
  }
  return false;
}

function candidateRowNameKeys(row: TexasTecCandidateRow): Set<string> {
  const keys = new Set<string>();
  const structuredName = [row.candidateNameFirst, row.candidateNameLast].filter(Boolean).join(" ");
  for (const key of normalizeTexasCandidateNameKeys(structuredName)) {
    keys.add(key);
  }
  for (const key of normalizeTexasCandidateNameKeys(row.candidateNameOrganization)) {
    keys.add(key);
  }
  return keys;
}

function candidateRowMatchesTarget(input: {
  row: TexasTecCandidateRow;
  candidateNameKeys: ReadonlySet<string>;
  officeScope: TexasFinanceOfficeScope;
  officeCanonicalName: string;
  expectedDistrict: string;
}): boolean {
  let nameMatches = false;
  for (const key of candidateRowNameKeys(input.row)) {
    if (input.candidateNameKeys.has(key)) {
      nameMatches = true;
      break;
    }
  }
  if (!nameMatches) {
    return false;
  }
  return rowOfficeMatches({
    officeCode: input.row.candidateSeekOfficeCd,
    officeDescription: input.row.candidateSeekOfficeDescr,
    district: input.row.candidateSeekOfficeDistrict,
    officeScope: input.officeScope,
    officeCanonicalName: input.officeCanonicalName,
    expectedDistrict: input.expectedDistrict,
  });
}

function spacRowMatchesTarget(input: {
  row: TexasTecSpacRow;
  candidateCommitteeId: string;
  officeScope: TexasFinanceOfficeScope;
  officeCanonicalName: string;
  expectedDistrict: string;
}): boolean {
  if (normalizeId(input.row.candidateFilerIdent) !== input.candidateCommitteeId) {
    return false;
  }
  const candidateFilerType = normalizeTextKey(input.row.candidateFilerTypeCd);
  if (candidateFilerType && candidateFilerType !== "COH") {
    return false;
  }
  return rowOfficeMatches({
    officeCode: input.row.candidateSeekOfficeCd,
    officeDescription: input.row.candidateSeekOfficeDescr,
    district: input.row.candidateSeekOfficeDistrict,
    officeScope: input.officeScope,
    officeCanonicalName: input.officeCanonicalName,
    expectedDistrict: input.expectedDistrict,
  });
}

function expenditureKey(input: { filerIdent: string; expendInfoId: string }): string {
  return `${normalizeId(input.filerIdent)}\u0000${normalizeId(input.expendInfoId)}`;
}

function groupKey(input: { committeeId: string; supportOppose: TexasSupportOppose }): string {
  return `${input.committeeId}\u0000${input.supportOppose}`;
}

function buildSpacRelationships(input: {
  candidateCommitteeId: string;
  officeScope: TexasFinanceOfficeScope;
  officeCanonicalName: string;
  expectedDistrict: string;
  spacRows: readonly TexasTecSpacRow[];
}): Map<string, SpacRelationship | null> {
  const relationships = new Map<string, SpacRelationship | null>();
  for (const row of input.spacRows) {
    if (!spacRowMatchesTarget({ row, ...input })) {
      continue;
    }
    const committeeId = normalizeId(row.spacFilerIdent);
    const committeeName = row.spacFilerName.trim();
    const supportOppose = supportOpposeFromTexasSpacPosition(row.spacPositionCd);
    if (!committeeId || !committeeName || !supportOppose) {
      continue;
    }

    const existing = relationships.get(committeeId);
    if (existing === null) {
      continue;
    }
    if (existing && existing.supportOppose !== supportOppose) {
      relationships.set(committeeId, null);
      continue;
    }
    relationships.set(committeeId, {
      committeeId,
      committeeName,
      supportOppose,
    });
  }
  return relationships;
}

function toGroups(input: {
  groups: Iterable<GroupAccumulator>;
  maxGroups: number;
  sourceUrl: string | null;
}): TexasOutsideSpendingGroup[] {
  return [...input.groups]
    .sort(
      (left, right) =>
        right.amountCents - left.amountCents ||
        left.supportOppose.localeCompare(right.supportOppose) ||
        left.committeeName.localeCompare(right.committeeName)
    )
    .slice(0, input.maxGroups)
    .map((group) => ({
      committeeId: group.committeeId,
      committeeName: group.committeeName,
      supportOppose: group.supportOppose,
      amount: centsToDollars(group.amountCents),
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateTexasOutsideSpending(
  input: TexasOutsideSpendingAggregationInput
): TexasOutsideSpendingAggregationResult {
  const candidateCommitteeId = normalizeId(requireNonEmpty(input.candidateCommitteeId, "Texas candidate committee id"));
  // VoteApp side expands nicknames; TEC purpose-row names stay literal.
  // Two layers keep shared-nickname expansion from combining two people's
  // money: a name match alone never contributes an amount (inclusion also
  // requires the spender to hold a declared SPAC position on THIS candidate's
  // own committee id, see buildSpacRelationships), and related spenders'
  // matched rows spanning conflicting formal first names abort the whole
  // aggregation (see matchedRowsSpanConflictingFirstNames).
  const candidateNameKeys = normalizeTexasCandidateNameKeys(input.candidateName, { expandNicknames: true });
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const officeScope = normalizeOfficeScope(input.officeScope);
  const officeCanonicalName = canonicalOfficeNameForInput(input.officeName);
  const expectedDistrict = normalizeDistrict(input.district);
  if (candidateNameKeys.size === 0) {
    return {
      summary: null,
      matchedCandidateExpenditureRowCount: 0,
      includedCandidateExpenditureRowCount: 0,
      skippedCandidateExpenditureRowCount: 0,
    };
  }
  if (!officeScope || !officeCanonicalName || !isTexasFinanceEligibleOffice({ officeScope, officeCanonicalName })) {
    throw new Error(`Unsupported Texas outside spending office: ${input.officeScope} ${input.officeName}`);
  }
  if (isLegislativeOffice(officeScope, officeCanonicalName) && !expectedDistrict) {
    throw new Error(`Texas outside spending district is required for ${officeCanonicalName}`);
  }

  const relationships = buildSpacRelationships({
    candidateCommitteeId,
    officeScope,
    officeCanonicalName,
    expectedDistrict,
    spacRows: input.spacRows,
  });
  const expendituresByKey = new Map<string, TexasTecExpenditureRow>();
  for (const row of input.expenditureRows) {
    const key = expenditureKey({ filerIdent: row.filerIdent, expendInfoId: row.expendInfoId });
    if (!expendituresByKey.has(key)) {
      expendituresByKey.set(key, row);
    }
  }

  const matchedRows = input.candidateRows.filter((row) =>
    candidateRowMatchesTarget({
      row,
      candidateNameKeys,
      officeScope,
      officeCanonicalName,
      expectedDistrict,
    })
  );
  const relatedMatchedRows = matchedRows.filter((row) => {
    const relationship = relationships.get(normalizeId(row.filerIdent));
    return relationship !== undefined && relationship !== null;
  });
  if (matchedRowsSpanConflictingFirstNames(relatedMatchedRows)) {
    return {
      summary: null,
      matchedCandidateExpenditureRowCount: matchedRows.length,
      includedCandidateExpenditureRowCount: 0,
      skippedCandidateExpenditureRowCount: matchedRows.length,
    };
  }

  const groups = new Map<string, GroupAccumulator>();
  let supportTotalCents = 0;
  let opposeTotalCents = 0;
  let matchedCandidateExpenditureRowCount = 0;
  let includedCandidateExpenditureRowCount = 0;
  let skippedCandidateExpenditureRowCount = 0;

  for (const row of matchedRows) {
    matchedCandidateExpenditureRowCount += 1;

    const committeeId = normalizeId(row.filerIdent);
    const relationship = relationships.get(committeeId);
    const expenditure = expendituresByKey.get(expenditureKey({ filerIdent: row.filerIdent, expendInfoId: row.expendInfoId }));
    if (
      !committeeId ||
      relationship === undefined ||
      relationship === null ||
      !expenditure ||
      isInfoOnly(expenditure)
    ) {
      skippedCandidateExpenditureRowCount += 1;
      continue;
    }

    const amountCents = parseAmountCents(expenditure.expendAmount);
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !isCycleYear({ rawDate: expenditure.expendDt, electionYear })
    ) {
      skippedCandidateExpenditureRowCount += 1;
      continue;
    }

    includedCandidateExpenditureRowCount += 1;
    if (relationship.supportOppose === "support") {
      supportTotalCents += amountCents;
    } else {
      opposeTotalCents += amountCents;
    }

    const key = groupKey({ committeeId: relationship.committeeId, supportOppose: relationship.supportOppose });
    const existing = groups.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      continue;
    }
    groups.set(key, {
      committeeId: relationship.committeeId,
      committeeName: relationship.committeeName,
      supportOppose: relationship.supportOppose,
      amountCents,
    });
  }

  const grouped = toGroups({
    groups: groups.values(),
    maxGroups,
    sourceUrl: input.sourceUrl ?? null,
  });
  if (grouped.length === 0) {
    return {
      summary: null,
      matchedCandidateExpenditureRowCount,
      includedCandidateExpenditureRowCount,
      skippedCandidateExpenditureRowCount,
    };
  }

  return {
    summary: {
      supportTotal: centsToDollars(supportTotalCents),
      opposeTotal: centsToDollars(opposeTotalCents),
      groups: grouped,
      sourceUrl: input.sourceUrl ?? null,
    },
    matchedCandidateExpenditureRowCount,
    includedCandidateExpenditureRowCount,
    skippedCandidateExpenditureRowCount,
  };
}
