import { classifyFinanceLabel } from "../finance/financeLabelClassifier.js";
import type { NewHampshireReceiptRow } from "./newHampshireCfsClient.js";
import { selectCurrentNewHampshireReceiptReportVersions } from "./newHampshirePhaseZero.js";

// Official CFS subtype codes. Live 2026 receipt search partitions completely
// into these donor codes plus ITR (interest).
export const NEW_HAMPSHIRE_DIRECT_CONTRIBUTION_SUBTYPE_CODES: ReadonlySet<string> = new Set([
  "INKIND",
  "ITMC",
  "ITMY",
  "ITNMC",
  "MTCB",
  "NITMC",
  "NITMY",
  "NITNMC",
]);

const NEW_HAMPSHIRE_NON_DIRECT_RECEIPT_SUBTYPE_CODES: ReadonlySet<string> = new Set(["ITR"]);

export type NewHampshireDirectContributionAggregationInput = {
  filingEntityId: number;
  electionYear: number;
  receiptRows: readonly NewHampshireReceiptRow[];
  sourceUrl?: string | null;
};

export type NewHampshireDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type NewHampshireFinanceDirectBreakdown = {
  categoryType: "industry" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: null;
  sourceUrl: string | null;
};

export type NewHampshireDirectContributionAggregationResult = {
  summary: NewHampshireDirectFinanceSummary;
  directBreakdowns: NewHampshireFinanceDirectBreakdown[];
  sourceRowCount: number;
  currentVersionRowCount: number;
  supersededRowCount: number;
  directContributionRowCount: number;
  nonDirectReceiptRowCount: number;
  nonPositiveRowCount: number;
};

type Aggregate = {
  categoryType: NewHampshireFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
};

function normalizeFilingEntityId(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid New Hampshire direct contribution filingEntityId: ${value}`);
  }
  return value;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2016 || value > 2100) {
    throw new Error(`Invalid New Hampshire direct contribution election year: ${value}`);
  }
  return value;
}

function amountToCents(amount: number, transactionId: number): number {
  const cents = Math.round(amount * 100);
  if (!Number.isFinite(amount) || !Number.isSafeInteger(cents)) {
    throw new Error(`Invalid New Hampshire receipt amount for transaction ${transactionId}: ${amount}`);
  }
  return cents;
}

function contributionSizeBucket(amountCents: number): string {
  if (amountCents < 100) return "$0.01-$0.99";
  if (amountCents < 100 * 100) return "$1-$99";
  if (amountCents < 250 * 100) return "$100-$249";
  if (amountCents < 500 * 100) return "$250-$499";
  if (amountCents < 1_000 * 100) return "$500-$999";
  if (amountCents < 5_000 * 100) return "$1,000-$4,999";
  return "$5,000+";
}

function aggregateKey(categoryType: Aggregate["categoryType"], categoryName: string): string {
  return `${categoryType}\u0000${categoryName}`;
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  categoryType: Aggregate["categoryType"],
  categoryName: string,
  amountCents: number
): void {
  const key = aggregateKey(categoryType, categoryName);
  const existing = aggregates.get(key);
  if (existing) {
    existing.amountCents += amountCents;
    return;
  }
  aggregates.set(key, { categoryType, categoryName, amountCents });
}

function isDirectContribution(row: NewHampshireReceiptRow): boolean {
  if (NEW_HAMPSHIRE_DIRECT_CONTRIBUTION_SUBTYPE_CODES.has(row.transactionSubTypeCode)) {
    return true;
  }
  if (NEW_HAMPSHIRE_NON_DIRECT_RECEIPT_SUBTYPE_CODES.has(row.transactionSubTypeCode)) {
    return false;
  }
  throw new Error(
    `Unknown New Hampshire receipt subtype ${JSON.stringify(row.transactionSubTypeCode)} ` +
      `(${JSON.stringify(row.transactionSubType)}) for transaction ${row.transactionId}`
  );
}

function validateRows(input: {
  rows: readonly NewHampshireReceiptRow[];
  filingEntityId: number;
  expectedCycleName: string;
}): void {
  for (const row of input.rows) {
    if (row.filerEntityId !== input.filingEntityId) {
      throw new Error(
        `New Hampshire receipt search was not exact: expected filer ${input.filingEntityId}, received ${row.filerEntityId}`
      );
    }
    if (row.electionCycle !== input.expectedCycleName) {
      throw new Error(
        `New Hampshire receipt search returned cycle ${JSON.stringify(row.electionCycle)}; ` +
          `expected ${input.expectedCycleName}`
      );
    }
    if (row.transactionTypeDescription !== "Receipt") {
      throw new Error(
        `New Hampshire receipt search returned transaction type ` +
          `${JSON.stringify(row.transactionTypeDescription)} for transaction ${row.transactionId}`
      );
    }
    isDirectContribution(row);
  }
}

function toDirectBreakdowns(
  aggregates: Iterable<Aggregate>,
  sourceUrl: string | null
): NewHampshireFinanceDirectBreakdown[] {
  const categoryOrder: Aggregate["categoryType"][] = ["industry", "contribution_size"];
  const values = [...aggregates];
  const result: NewHampshireFinanceDirectBreakdown[] = [];
  for (const categoryType of categoryOrder) {
    for (const aggregate of values
      .filter((value) => value.categoryType === categoryType)
      .sort(
        (left, right) =>
          right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName)
      )) {
      result.push({
        categoryType,
        categoryName: aggregate.categoryName,
        amount: aggregate.amountCents / 100,
        contributorCount: null,
        sourceUrl,
      });
    }
  }
  return result;
}

export function aggregateNewHampshireDirectContributions(
  input: NewHampshireDirectContributionAggregationInput
): NewHampshireDirectContributionAggregationResult {
  const filingEntityId = normalizeFilingEntityId(input.filingEntityId);
  const electionYear = normalizeElectionYear(input.electionYear);
  const expectedCycleName = `${electionYear} Election Cycle`;
  const sourceUrl = input.sourceUrl ?? null;
  validateRows({ rows: input.receiptRows, filingEntityId, expectedCycleName });

  const currentRows = selectCurrentNewHampshireReceiptReportVersions(input.receiptRows);
  const aggregates = new Map<string, Aggregate>();
  let totalReceiptsCents = 0;
  let directContributionTotalCents = 0;
  let directContributionRowCount = 0;
  let nonDirectReceiptRowCount = 0;
  let nonPositiveRowCount = 0;

  for (const row of currentRows) {
    const amountCents = amountToCents(row.transactionAmount, row.transactionId);
    if (amountCents <= 0) {
      nonPositiveRowCount += 1;
      continue;
    }
    totalReceiptsCents += amountCents;
    if (!isDirectContribution(row)) {
      nonDirectReceiptRowCount += 1;
      continue;
    }

    directContributionRowCount += 1;
    directContributionTotalCents += amountCents;
    addAggregate(aggregates, "contribution_size", contributionSizeBucket(amountCents), amountCents);

    if (row.employerName !== null) {
      const industry = classifyFinanceLabel({
        rawLabel: row.employerName,
        labelType: "employer",
      }).industrySlug;
      if (industry !== null) addAggregate(aggregates, "industry", industry, amountCents);
    }
  }

  return {
    summary: {
      totalReceipts: totalReceiptsCents / 100,
      directContributionTotal: directContributionTotalCents / 100,
      sourceUrl,
    },
    directBreakdowns: toDirectBreakdowns(aggregates.values(), sourceUrl),
    sourceRowCount: input.receiptRows.length,
    currentVersionRowCount: currentRows.length,
    supersededRowCount: input.receiptRows.length - currentRows.length,
    directContributionRowCount,
    nonDirectReceiptRowCount,
    nonPositiveRowCount,
  };
}
