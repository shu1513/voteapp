import {
  normalizeTexasCandidateNameKeys,
  texasCandidateNameMiddleConflicts,
} from "../texasFinance/texasCandidateCommitteeResolver.js";
import type {
  TexasTecCandidateRow,
  TexasTecExpenditureRow,
  TexasTecPurposeRow,
} from "../texasFinance/texasTecCsvDatabaseReader.js";
import type {
  TexasOutsideSpendingGroup,
  TexasOutsideSpendingSummary,
  TexasSupportOppose,
} from "../texasFinance/texasOutsideSpendingAggregator.js";
import {
  houstonFinanceOfficeTargetsEqual,
  parseHoustonDisclosureOfficeTarget,
  type HoustonFinanceOfficeTarget,
} from "./houstonFinanceOfficeTargets.js";

function key(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|HONORABLE|MR|MRS|MS|SENATOR|SEN)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const HOUSTON_MAYOR_ALIASES = new Set([
  "HOUSTON MAYOR",
  "MAYOR HOUSTON",
  "MAYOR HOUSTON TX",
  "MAYOR HOUSTON TEXAS",
  "CITY HOUSTON MAYOR",
  "HOUSTON CITY MAYOR",
  "MAYOR CITY HOUSTON",
  "MAYOR OF HOUSTON",
  "MAYOR OF CITY HOUSTON",
]);

export function isTexasTecHoustonMayorDescription(value: string): boolean {
  const normalized = key(value).replace(/\bOF\b/g, " ").replace(/\s+/g, " ").trim();
  return HOUSTON_MAYOR_ALIASES.has(normalized);
}

export function isTexasTecHoustonOfficeDescription(
  value: string,
  officeTarget: HoustonFinanceOfficeTarget
): boolean {
  if (officeTarget.officeName === "Mayor") return isTexasTecHoustonMayorDescription(value);
  const normalized = key(value);
  if (!/\bHOUSTON\b/.test(normalized)) return false;
  const parsed = parseHoustonDisclosureOfficeTarget(normalized);
  return parsed !== null && houstonFinanceOfficeTargetsEqual(parsed, officeTarget);
}

function officeDescription(input: { place: string; description: string; district: string }): string {
  const district = input.district.trim();
  const districtDescription = /^[A-K]$/i.test(district) ? `District ${district}` : district;
  return [input.place, input.description, districtDescription].filter(Boolean).join(" ");
}

function isInfoOnly(value: string): boolean {
  return new Set(["Y", "YES", "TRUE", "1"]).has(key(value));
}

function namesIntersect(left: ReadonlySet<string>, right: ReadonlySet<string>): boolean {
  for (const value of left) if (right.has(value)) return true;
  return false;
}

function purposeCandidateKeys(row: TexasTecPurposeRow): Set<string> {
  return normalizeTexasCandidateNameKeys(row.commActivityName);
}

// Key overlap collapses names to first+last, which would attribute a GPAC's
// spending on "ABBOTT, GREG R" to "Greg W. Abbott" whenever office and year
// agree. A contradicting middle name rejects the row (georgia pattern).
function purposeRowNameConflicts(input: { candidateName: string; row: TexasTecPurposeRow }): boolean {
  return texasCandidateNameMiddleConflicts({
    candidateName: input.candidateName,
    rowNames: [input.row.commActivityName],
  });
}

function candidateRowPersonName(row: TexasTecCandidateRow): string {
  return [row.candidateNameFirst, row.candidateNameLast].filter(Boolean).join(" ");
}

function candidateRowNameConflicts(input: { candidateName: string; row: TexasTecCandidateRow }): boolean {
  // candidateNameOrganization is excluded: it is not a person name.
  return texasCandidateNameMiddleConflicts({
    candidateName: input.candidateName,
    rowNames: [candidateRowPersonName(input.row)],
  });
}

function candidateKeys(row: TexasTecCandidateRow): Set<string> {
  const values = [candidateRowPersonName(row), row.candidateNameOrganization];
  return new Set(values.flatMap((value) => [...normalizeTexasCandidateNameKeys(value)]));
}

function direction(value: string): TexasSupportOppose | null {
  const normalized = key(value);
  if (normalized === "SUPPORT") return "support";
  if (normalized === "OPPOSE") return "oppose";
  return null;
}

function parseAmountCents(value: string): number | null {
  const normalized = value.replace(/[$,]/g, "").trim();
  if (!/^\d+(?:\.\d{1,4})?$/.test(normalized)) return null;
  const cents = Math.round(Number(normalized) * 100);
  return Number.isSafeInteger(cents) && cents > 0 ? cents : null;
}

function dateYear(value: string): number | null {
  const compact = /^(\d{4})\d{4}$/.exec(value.trim());
  const iso = /^(\d{4})-\d{2}-\d{2}/.exec(value.trim());
  const slash = /^\d{1,2}\/\d{1,2}\/(\d{4})/.exec(value.trim());
  return Number(compact?.[1] ?? iso?.[1] ?? slash?.[1] ?? NaN) || null;
}

export function aggregateHoustonTexasGpacOutsideSpending(input: {
  candidateName: string;
  electionYear: number;
  officeTarget?: HoustonFinanceOfficeTarget;
  purposeRows: readonly TexasTecPurposeRow[];
  candidateRows: readonly TexasTecCandidateRow[];
  expenditureRows: readonly TexasTecExpenditureRow[];
  sourceUrl?: string | null;
  maxGroups?: number;
}): {
  summary: TexasOutsideSpendingSummary | null;
  matchedCandidateRowCount: number;
  includedExpenditureCount: number;
  skippedCandidateRowCount: number;
} {
  if (!Number.isInteger(input.electionYear) || input.electionYear < 2014 || input.electionYear > 2100) {
    throw new Error(`Invalid Houston TEC outside-spending election year: ${input.electionYear}`);
  }
  const targetNames = normalizeTexasCandidateNameKeys(input.candidateName);
  const officeTarget = input.officeTarget ?? { officeName: "Mayor", seat: "Houston" };
  const relationshipByReport = new Map<string, TexasSupportOppose | null>();
  for (const row of input.purposeRows) {
    if (
      isInfoOnly(row.infoOnlyFlag) ||
      !["GPAC", "MPAC"].includes(key(row.filerTypeCd)) ||
      key(row.subjectCategoryCd) !== "CANDIDATE" ||
      !namesIntersect(targetNames, purposeCandidateKeys(row)) ||
      purposeRowNameConflicts({ candidateName: input.candidateName, row }) ||
      !isTexasTecHoustonOfficeDescription(officeDescription({
        place: row.activitySeekOfficePlace,
        description: row.activitySeekOfficeDescr,
        district: row.activitySeekOfficeDistrict,
      }), officeTarget)
    ) continue;
    const rowDirection = direction(row.subjectPositionCd);
    if (!rowDirection) continue;
    const reportKey = `${key(row.filerIdent)}\u0000${key(row.reportInfoIdent)}`;
    const existing = relationshipByReport.get(reportKey);
    relationshipByReport.set(reportKey, existing && existing !== rowDirection ? null : rowDirection);
  }

  const expenditureByKey = new Map<string, TexasTecExpenditureRow>();
  for (const row of input.expenditureRows) {
    if (isInfoOnly(row.infoOnlyFlag)) continue;
    const rowKey = `${key(row.filerIdent)}\u0000${key(row.expendInfoId)}`;
    if (!expenditureByKey.has(rowKey)) expenditureByKey.set(rowKey, row);
  }

  const groups = new Map<string, { committeeId: string; committeeName: string; supportOppose: TexasSupportOppose; amountCents: number; count: number }>();
  const seen = new Set<string>();
  let matchedCandidateRowCount = 0;
  let includedExpenditureCount = 0;
  let skippedCandidateRowCount = 0;
  for (const row of input.candidateRows) {
    if (
      isInfoOnly(row.infoOnlyFlag) ||
      !namesIntersect(targetNames, candidateKeys(row)) ||
      candidateRowNameConflicts({ candidateName: input.candidateName, row }) ||
      !isTexasTecHoustonOfficeDescription(officeDescription({
        place: row.candidateSeekOfficePlace,
        description: row.candidateSeekOfficeDescr,
        district: row.candidateSeekOfficeDistrict,
      }), officeTarget)
    ) continue;
    matchedCandidateRowCount += 1;
    const reportKey = `${key(row.filerIdent)}\u0000${key(row.reportInfoIdent)}`;
    const rowDirection = relationshipByReport.get(reportKey);
    const expenditureKey = `${key(row.filerIdent)}\u0000${key(row.expendInfoId)}`;
    const expenditure = expenditureByKey.get(expenditureKey);
    const year = expenditure ? dateYear(expenditure.expendDt) : null;
    const amountCents = expenditure ? parseAmountCents(expenditure.expendAmount) : null;
    if (!rowDirection || !expenditure || !amountCents || year === null || year < input.electionYear - 1 || year > input.electionYear || seen.has(expenditureKey)) {
      skippedCandidateRowCount += 1;
      continue;
    }
    seen.add(expenditureKey);
    includedExpenditureCount += 1;
    const committeeId = key(row.filerIdent);
    const groupKey = `${committeeId}\u0000${rowDirection}`;
    const existing = groups.get(groupKey);
    if (existing) {
      existing.amountCents += amountCents;
      existing.count += 1;
    } else {
      groups.set(groupKey, {
        committeeId,
        committeeName: row.filerName.trim(),
        supportOppose: rowDirection,
        amountCents,
        count: 1,
      });
    }
  }

  const allGroups = [...groups.values()];
  const topGroups: TexasOutsideSpendingGroup[] = allGroups
    .sort((left, right) => right.amountCents - left.amountCents || left.committeeName.localeCompare(right.committeeName))
    .slice(0, input.maxGroups ?? 50)
    .map((group) => ({
      committeeId: group.committeeId,
      committeeName: group.committeeName,
      supportOppose: group.supportOppose,
      amount: group.amountCents / 100,
      sourceUrl: input.sourceUrl ?? null,
    }));
  if (topGroups.length === 0) {
    return { summary: null, matchedCandidateRowCount, includedExpenditureCount, skippedCandidateRowCount };
  }
  return {
    summary: {
      supportTotal: allGroups.filter((group) => group.supportOppose === "support").reduce((sum, group) => sum + group.amountCents, 0) / 100,
      opposeTotal: allGroups.filter((group) => group.supportOppose === "oppose").reduce((sum, group) => sum + group.amountCents, 0) / 100,
      groups: topGroups,
      sourceUrl: input.sourceUrl ?? null,
    },
    matchedCandidateRowCount,
    includedExpenditureCount,
    skippedCandidateRowCount,
  };
}
