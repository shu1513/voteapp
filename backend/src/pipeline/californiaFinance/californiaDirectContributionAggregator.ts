import { toCaliforniaElectionCycle } from "./californiaPowerSearchClient.js";
import type {
  CaliforniaFinanceDirectBreakdownInput,
  CaliforniaFinanceSummaryInput,
} from "./californiaFinanceWriter.js";

export type CalAccessReceiptRow = Record<string, string | null | undefined>;

export type CaliforniaDirectContributionAggregationInput = {
  controlledCommitteeId: string;
  electionYear: number;
  receiptRows: readonly CalAccessReceiptRow[];
  controlledCommitteeFilingIds?: readonly string[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type CaliforniaDirectContributionAggregationResult = {
  summary: CaliforniaFinanceSummaryInput;
  directBreakdowns: CaliforniaFinanceDirectBreakdownInput[];
  matchedReceiptRowCount: number;
  includedReceiptRowCount: number;
  skippedReceiptRowCount: number;
};

type Aggregate = {
  categoryType: "occupation" | "employer" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2001 || value > 2100) {
    throw new Error(`Invalid California direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid California direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function value(row: CalAccessReceiptRow, key: string): string {
  return row[key]?.trim() ?? "";
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
}

function parseAmount(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (normalized.length === 0) {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseCalAccessDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  return null;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseCalAccessDateYear(input.rawDate);
  if (year === null) {
    return false;
  }
  const cycleStartYear = toCaliforniaElectionCycle(input.electionYear);
  return year >= cycleStartYear && year <= input.electionYear;
}

function contributionSizeBucket(amount: number): string {
  if (amount < 100) {
    return "$1-$99";
  }
  if (amount < 250) {
    return "$100-$249";
  }
  if (amount < 500) {
    return "$250-$499";
  }
  if (amount < 1_000) {
    return "$500-$999";
  }
  if (amount < 5_000) {
    return "$1,000-$4,999";
  }
  return "$5,000+";
}

function aggregateKey(categoryType: Aggregate["categoryType"], categoryName: string): string {
  return `${categoryType}\u0000${categoryName.trim().toUpperCase()}`;
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  input: { categoryType: Aggregate["categoryType"]; categoryName: string; amount: number }
): void {
  const categoryName = input.categoryName.trim().replace(/\s+/g, " ");
  if (categoryName.length === 0) {
    return;
  }

  const key = aggregateKey(input.categoryType, categoryName);
  const existing = aggregates.get(key);
  if (!existing) {
    aggregates.set(key, {
      categoryType: input.categoryType,
      categoryName,
      amount: input.amount,
      contributorCount: 1,
    });
    return;
  }

  existing.amount += input.amount;
  existing.contributorCount += 1;
}

function isReceiptForControlledCommittee(input: {
  row: CalAccessReceiptRow;
  controlledCommitteeId: string;
  controlledCommitteeFilingIds: Set<string>;
}): boolean {
  const rowCommitteeId = normalizeId(value(input.row, "CMTE_ID"));
  if (rowCommitteeId && rowCommitteeId === input.controlledCommitteeId) {
    return true;
  }

  const filingId = normalizeId(value(input.row, "FILING_ID"));
  return filingId.length > 0 && input.controlledCommitteeFilingIds.has(filingId);
}

function toDirectBreakdowns(input: {
  aggregates: Iterable<Aggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): CaliforniaFinanceDirectBreakdownInput[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: CaliforniaFinanceDirectBreakdownInput[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["occupation", "employer", "contribution_size"];
  for (const categoryType of categoryOrder) {
    const limit = categoryType === "contribution_size" ? Number.POSITIVE_INFINITY : input.maxBreakdownsPerCategory;
    for (const aggregate of (byCategory.get(categoryType) ?? [])
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, limit)) {
      result.push({
        categoryType: aggregate.categoryType,
        categoryName: aggregate.categoryName,
        amount: aggregate.amount,
        contributorCount: aggregate.contributorCount,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateCaliforniaDirectContributions(
  input: CaliforniaDirectContributionAggregationInput
): CaliforniaDirectContributionAggregationResult {
  const controlledCommitteeId = normalizeId(
    requireNonEmpty(input.controlledCommitteeId, "California controlled committee id")
  );
  const electionYear = normalizeElectionYear(input.electionYear);
  const controlledCommitteeFilingIds = new Set(
    (input.controlledCommitteeFilingIds ?? []).map(normalizeId).filter(Boolean)
  );
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const aggregates = new Map<string, Aggregate>();
  let matchedReceiptRowCount = 0;
  let includedReceiptRowCount = 0;
  let skippedReceiptRowCount = 0;
  let totalReceipts = 0;

  for (const row of input.receiptRows) {
    if (!isReceiptForControlledCommittee({ row, controlledCommitteeId, controlledCommitteeFilingIds })) {
      continue;
    }
    matchedReceiptRowCount += 1;

    const amount = parseAmount(value(row, "AMOUNT"));
    if (amount === null || amount <= 0 || !isCycleYear({ rawDate: value(row, "RCPT_DATE"), electionYear })) {
      skippedReceiptRowCount += 1;
      continue;
    }

    includedReceiptRowCount += 1;
    totalReceipts += amount;
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: value(row, "CTRIB_OCC"),
      amount,
    });
    addAggregate(aggregates, {
      categoryType: "employer",
      categoryName: value(row, "CTRIB_EMP"),
      amount,
    });
    addAggregate(aggregates, {
      categoryType: "contribution_size",
      categoryName: contributionSizeBucket(amount),
      amount,
    });
  }

  return {
    summary: {
      totalReceipts,
      sourceUrl: input.sourceUrl ?? null,
    },
    directBreakdowns: toDirectBreakdowns({
      aggregates: aggregates.values(),
      sourceUrl: input.sourceUrl ?? null,
      maxBreakdownsPerCategory,
    }),
    matchedReceiptRowCount,
    includedReceiptRowCount,
    skippedReceiptRowCount,
  };
}
