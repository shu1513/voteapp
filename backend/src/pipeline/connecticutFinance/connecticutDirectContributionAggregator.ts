import type { ConnecticutEcrisArtifactRow } from "./connecticutEcrisArtifactReader.js";

export type ConnecticutDirectContributionAggregationInput = {
  committeeId: string;
  electionYear: number;
  receiptRows: readonly ConnecticutEcrisArtifactRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type ConnecticutDirectFinanceSummary = {
  totalReceipts: number;
  sourceUrl: string | null;
};

export type ConnecticutFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type ConnecticutDirectContributionAggregationResult = {
  summary: ConnecticutDirectFinanceSummary;
  directBreakdowns: ConnecticutFinanceDirectBreakdown[];
  matchedReceiptRowCount: number;
  includedReceiptRowCount: number;
  skippedReceiptRowCount: number;
};

type Aggregate = {
  categoryType: ConnecticutFinanceDirectBreakdown["categoryType"];
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
  if (!Number.isInteger(value) || value < 2008 || value > 2100) {
    throw new Error(`Invalid Connecticut direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Connecticut direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function value(row: ConnecticutEcrisArtifactRow, key: string): string {
  return row[key]?.trim() ?? "";
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
}

function normalizeTextKey(value: string): string {
  return value
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAmount(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (normalized.length === 0) {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseElectionYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isInteger(parsed) && String(parsed) === trimmed ? parsed : null;
}

function isCandidateCommittee(row: ConnecticutEcrisArtifactRow): boolean {
  return normalizeTextKey(value(row, "Committee Type")) === "CANDIDATE COMMITTEE";
}

function rowElectionYearMatches(row: ConnecticutEcrisArtifactRow, electionYear: number): boolean {
  return parseElectionYear(value(row, "ElectionYear")) === electionYear;
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

function toDirectBreakdowns(input: {
  aggregates: Iterable<Aggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): ConnecticutFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: ConnecticutFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["occupation", "contribution_size"];
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

export function aggregateConnecticutDirectContributions(
  input: ConnecticutDirectContributionAggregationInput
): ConnecticutDirectContributionAggregationResult {
  const committeeId = normalizeId(requireNonEmpty(input.committeeId, "Connecticut committee id"));
  const electionYear = normalizeElectionYear(input.electionYear);
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
    if (normalizeId(value(row, "Committee ID")) !== committeeId) {
      continue;
    }
    matchedReceiptRowCount += 1;

    const amount = parseAmount(value(row, "Amount"));
    if (amount === null || amount <= 0 || !isCandidateCommittee(row) || !rowElectionYearMatches(row, electionYear)) {
      skippedReceiptRowCount += 1;
      continue;
    }

    includedReceiptRowCount += 1;
    totalReceipts += amount;
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: value(row, "Occupation"),
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
