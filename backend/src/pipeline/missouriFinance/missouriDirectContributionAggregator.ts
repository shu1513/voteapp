import { selectMissouriCanonicalReportRows, type MissouriReportSelectionDiagnostic } from "./missouriReportInventory.js";
import type {
  MissouriMecContributionRow,
  MissouriMecExpenditureRow,
  MissouriMecReportInventoryRow,
} from "./missouriMecParsers.js";

export const MISSOURI_UNKNOWN_OCCUPATION_LABEL = "Unknown";

export type MissouriFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type MissouriDirectFinanceAggregationResult = {
  directContributionTotal: number;
  totalDisbursements: number;
  directBreakdowns: MissouriFinanceDirectBreakdown[];
  includedContributionRowCount: number;
  includedExpenditureRowCount: number;
  outsideCycleContributionRowCount: number;
  outsideCycleExpenditureRowCount: number;
  inKindAmount: number;
  incurredExpenditureAmount: number;
  unrecognizedContributionKindRowCount: number;
  unrecognizedContributionKindAmount: number;
  unrecognizedExpenditureTypeRowCount: number;
  unrecognizedExpenditureTypeAmount: number;
  contributionReportDiagnostics: MissouriReportSelectionDiagnostic[];
  expenditureReportDiagnostics: MissouriReportSelectionDiagnostic[];
};

type Aggregate = {
  categoryType: MissouriFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributors: Set<string>;
};

function normalizeText(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ");
}

function keyText(value: string | null | undefined): string {
  return normalizeText(value).toLocaleUpperCase("en-US");
}

function contributionFingerprint(row: MissouriMecContributionRow): string {
  return [
    row.mecid,
    row.committeeName,
    row.contributorCommittee,
    row.contributorCompany,
    row.contributorLastName,
    row.contributorFirstName,
    row.employer,
    row.occupation,
    row.contributionDate,
    row.amountCents,
    row.contributionKind,
  ].map(String).join("\u0000");
}

function expenditureFingerprint(row: MissouriMecExpenditureRow): string {
  return [
    row.mecid,
    row.committeeName,
    row.payeeLastName,
    row.payeeFirstName,
    row.payeeCompany,
    row.purpose,
    row.expenditureDate,
    row.amountCents,
    row.expenditureType,
  ].map(String).join("\u0000");
}

function contributorKey(row: MissouriMecContributionRow, index: number): string {
  const parts = [
    row.contributorCommittee,
    row.contributorCompany,
    row.contributorLastName,
    row.contributorFirstName,
  ].map(keyText).filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : `UNKNOWN-${index}`;
}

function occupationName(value: string | null): string {
  const filed = normalizeText(value);
  return !filed || keyText(filed) === "UNKNOWN" ? MISSOURI_UNKNOWN_OCCUPATION_LABEL : filed;
}

function sizeBucket(amountCents: number): string {
  if (amountCents < 10_000) return "$1-$99";
  if (amountCents < 25_000) return "$100-$249";
  if (amountCents < 50_000) return "$250-$499";
  if (amountCents < 100_000) return "$500-$999";
  if (amountCents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  categoryType: Aggregate["categoryType"],
  categoryName: string,
  amountCents: number,
  contributor: string
): void {
  const key = `${categoryType}\u0000${keyText(categoryName)}`;
  const aggregate = aggregates.get(key);
  if (aggregate) {
    aggregate.amountCents += amountCents;
    aggregate.contributors.add(contributor);
  } else {
    aggregates.set(key, { categoryType, categoryName, amountCents, contributors: new Set([contributor]) });
  }
}

function inCycle(date: string, start: string, end: string): boolean {
  return date >= start && date <= end;
}

export function aggregateMissouriDirectFinance(input: {
  inventory: readonly MissouriMecReportInventoryRow[];
  contributionRows: readonly MissouriMecContributionRow[];
  expenditureRows: readonly MissouriMecExpenditureRow[];
  cycleStart: string;
  cycleEnd: string;
  sourceUrl?: string | null;
  maxOccupationBreakdowns?: number;
}): MissouriDirectFinanceAggregationResult {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(input.cycleStart) || !/^\d{4}-\d{2}-\d{2}$/.test(input.cycleEnd) || input.cycleStart > input.cycleEnd) {
    throw new Error(`Invalid Missouri finance cycle window: ${input.cycleStart}..${input.cycleEnd}`);
  }
  const maxOccupationBreakdowns = input.maxOccupationBreakdowns ?? 50;
  if (!Number.isSafeInteger(maxOccupationBreakdowns) || maxOccupationBreakdowns <= 0) {
    throw new Error(`Invalid Missouri occupation breakdown limit: ${input.maxOccupationBreakdowns}`);
  }

  const cycleContributionRows = input.contributionRows.filter((row) =>
    inCycle(row.contributionDate, input.cycleStart, input.cycleEnd)
  );
  const cycleExpenditureRows = input.expenditureRows.filter((row) =>
    inCycle(row.expenditureDate, input.cycleStart, input.cycleEnd)
  );
  const contributionSelection = selectMissouriCanonicalReportRows({
    inventory: input.inventory,
    rows: cycleContributionRows,
    reportName: (row) => row.report,
    amountCents: (row) => row.amountCents,
    safeFingerprint: contributionFingerprint,
  });
  const expenditureSelection = selectMissouriCanonicalReportRows({
    inventory: input.inventory,
    rows: cycleExpenditureRows,
    reportName: (row) => row.report,
    amountCents: (row) => row.amountCents,
    safeFingerprint: expenditureFingerprint,
  });

  const aggregates = new Map<string, Aggregate>();
  let directCents = 0;
  let includedContributionRowCount = 0;
  const outsideCycleContributionRowCount = input.contributionRows.length - cycleContributionRows.length;
  let inKindCents = 0;
  let unrecognizedContributionKindRowCount = 0;
  let unrecognizedContributionKindCents = 0;
  contributionSelection.rows.forEach((row, index) => {
    const kind = keyText(row.contributionKind).replace(/[^A-Z]/g, "");
    if (kind === "INKIND") {
      inKindCents += row.amountCents;
      return;
    }
    if (kind !== "MONETARY") {
      unrecognizedContributionKindRowCount += 1;
      unrecognizedContributionKindCents += row.amountCents;
      return;
    }
    directCents += row.amountCents;
    includedContributionRowCount += 1;
    if (row.amountCents <= 0) return;
    const contributor = contributorKey(row, index);
    addAggregate(aggregates, "contribution_size", sizeBucket(row.amountCents), row.amountCents, contributor);
    addAggregate(aggregates, "occupation", occupationName(row.occupation), row.amountCents, contributor);
  });

  let disbursementCents = 0;
  let includedExpenditureRowCount = 0;
  const outsideCycleExpenditureRowCount = input.expenditureRows.length - cycleExpenditureRows.length;
  let incurredCents = 0;
  let unrecognizedExpenditureTypeRowCount = 0;
  let unrecognizedExpenditureTypeCents = 0;
  for (const row of expenditureSelection.rows) {
    const type = keyText(row.expenditureType);
    if (type === "INCURRED") {
      incurredCents += row.amountCents;
    } else if (type === "PAID") {
      disbursementCents += row.amountCents;
      includedExpenditureRowCount += 1;
    } else {
      unrecognizedExpenditureTypeRowCount += 1;
      unrecognizedExpenditureTypeCents += row.amountCents;
    }
  }

  const byType = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of aggregates.values()) {
    const rows = byType.get(aggregate.categoryType) ?? [];
    rows.push(aggregate);
    byType.set(aggregate.categoryType, rows);
  }
  const directBreakdowns: MissouriFinanceDirectBreakdown[] = [];
  for (const type of ["occupation", "contribution_size"] as const) {
    const limit = type === "occupation" ? maxOccupationBreakdowns : Number.POSITIVE_INFINITY;
    for (const aggregate of (byType.get(type) ?? [])
      .sort((a, b) => b.amountCents - a.amountCents || a.categoryName.localeCompare(b.categoryName))
      .slice(0, limit)) {
      directBreakdowns.push({
        categoryType: type,
        categoryName: aggregate.categoryName,
        amount: aggregate.amountCents / 100,
        contributorCount: aggregate.contributors.size,
        sourceUrl: input.sourceUrl ?? null,
      });
    }
  }

  return {
    directContributionTotal: directCents / 100,
    totalDisbursements: disbursementCents / 100,
    directBreakdowns,
    includedContributionRowCount,
    includedExpenditureRowCount,
    outsideCycleContributionRowCount,
    outsideCycleExpenditureRowCount,
    inKindAmount: inKindCents / 100,
    incurredExpenditureAmount: incurredCents / 100,
    unrecognizedContributionKindRowCount,
    unrecognizedContributionKindAmount: unrecognizedContributionKindCents / 100,
    unrecognizedExpenditureTypeRowCount,
    unrecognizedExpenditureTypeAmount: unrecognizedExpenditureTypeCents / 100,
    contributionReportDiagnostics: contributionSelection.diagnostics,
    expenditureReportDiagnostics: expenditureSelection.diagnostics,
  };
}
