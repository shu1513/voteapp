import type { ArizonaSpotlightIncomeTransaction } from "./arizonaSpotlightClient.js";

export type ArizonaDirectContributionAggregationInput = {
  committeeId: string;
  committeeIds?: readonly string[];
  electionYear: number;
  incomeTransactions: readonly ArizonaSpotlightIncomeTransaction[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type ArizonaDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type ArizonaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type ArizonaDirectContributionAggregationResult = {
  summary: ArizonaDirectFinanceSummary;
  directBreakdowns: ArizonaFinanceDirectBreakdown[];
  matchedIncomeTransactionCount: number;
  includedIncomeTransactionCount: number;
  skippedIncomeTransactionCount: number;
};

type Aggregate = {
  categoryType: ArizonaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
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
  if (!Number.isInteger(value) || value < 2002 || value > 2100) {
    throw new Error(`Invalid Arizona direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Arizona direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeCommitteeId(value: string): string {
  return value.trim().toUpperCase();
}

function normalizeTextKey(value: string | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function amountToCents(amount: number): number | null {
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseDateYear(value: string | undefined): number | null {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
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

function isCycleYear(input: { transaction: ArizonaSpotlightIncomeTransaction; electionYear: number }): boolean {
  const year = parseDateYear(input.transaction.transactionDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function normalizeCommitteeIds(input: { committeeId: string; committeeIds?: readonly string[] }): Set<string> {
  const committeeIds = new Set<string>([normalizeCommitteeId(requireNonEmpty(input.committeeId, "Arizona committee id"))]);
  for (const committeeId of input.committeeIds ?? []) {
    const normalized = normalizeCommitteeId(committeeId);
    if (normalized) {
      committeeIds.add(normalized);
    }
  }
  return committeeIds;
}

function contributorIdentityKey(transaction: ArizonaSpotlightIncomeTransaction): string {
  const parts = [transaction.transactionName, transaction.city, transaction.state, transaction.zipCode]
    .map(normalizeTextKey)
    .filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : "unknown";
}

function isContributionIncomeTransaction(transaction: ArizonaSpotlightIncomeTransaction): boolean {
  const transactionType = normalizeTextKey(transaction.transactionType);
  if (!transactionType) {
    return false;
  }
  if (/\b(LOAN|TRANSFER|REFUND|REBATE|INTEREST|OFFSET|IN KIND EXPENSE|EXPENDITURE)\b/.test(transactionType)) {
    return false;
  }
  return /\bCONTRIBUTION(S)?\b/.test(transactionType);
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
  return `${categoryType}\u0000${normalizeTextKey(categoryName)}`;
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  input: {
    categoryType: Aggregate["categoryType"];
    categoryName: string | undefined;
    amountCents: number;
    contributorKey: string;
  }
): void {
  const categoryName = input.categoryName?.trim().replace(/\s+/g, " ");
  if (!categoryName) {
    return;
  }

  const key = aggregateKey(input.categoryType, categoryName);
  const existing = aggregates.get(key);
  if (!existing) {
    aggregates.set(key, {
      categoryType: input.categoryType,
      categoryName,
      amountCents: input.amountCents,
      contributorKeys: new Set([input.contributorKey]),
    });
    return;
  }
  existing.amountCents += input.amountCents;
  existing.contributorKeys.add(input.contributorKey);
}

function toDirectBreakdowns(input: {
  aggregates: Iterable<Aggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): ArizonaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: ArizonaFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["occupation", "contribution_size"];
  for (const categoryType of categoryOrder) {
    const limit = categoryType === "contribution_size" ? Number.POSITIVE_INFINITY : input.maxBreakdownsPerCategory;
    for (const aggregate of (byCategory.get(categoryType) ?? [])
      .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))
      .slice(0, limit)) {
      result.push({
        categoryType: aggregate.categoryType,
        categoryName: aggregate.categoryName,
        amount: centsToDollars(aggregate.amountCents),
        contributorCount: aggregate.contributorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }
  return result;
}

export function aggregateArizonaDirectContributions(
  input: ArizonaDirectContributionAggregationInput
): ArizonaDirectContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const committeeIds = normalizeCommitteeIds(input);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const aggregates = new Map<string, Aggregate>();
  let matchedIncomeTransactionCount = 0;
  let includedIncomeTransactionCount = 0;
  let skippedIncomeTransactionCount = 0;
  let totalReceiptsCents = 0;

  for (const transaction of input.incomeTransactions) {
    if (!committeeIds.has(normalizeCommitteeId(transaction.committeeId))) {
      continue;
    }
    matchedIncomeTransactionCount += 1;

    const amountCents = amountToCents(transaction.amount);
    if (amountCents === null || amountCents <= 0 || !isCycleYear({ transaction, electionYear })) {
      skippedIncomeTransactionCount += 1;
      continue;
    }

    totalReceiptsCents += amountCents;
    if (!isContributionIncomeTransaction(transaction)) {
      skippedIncomeTransactionCount += 1;
      continue;
    }

    includedIncomeTransactionCount += 1;
    const contributorKey = contributorIdentityKey(transaction);
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: transaction.occupation,
      amountCents,
      contributorKey,
    });
    addAggregate(aggregates, {
      categoryType: "contribution_size",
      categoryName: contributionSizeBucket(transaction.amount),
      amountCents,
      contributorKey,
    });
  }

  const sourceUrl = input.sourceUrl ?? null;
  return {
    summary: {
      totalReceipts: centsToDollars(totalReceiptsCents),
      directContributionTotal: centsToDollars([...aggregates.values()]
        .filter((aggregate) => aggregate.categoryType === "contribution_size")
        .reduce((total, aggregate) => total + aggregate.amountCents, 0)),
      sourceUrl,
    },
    directBreakdowns: toDirectBreakdowns({
      aggregates: aggregates.values(),
      sourceUrl,
      maxBreakdownsPerCategory,
    }),
    matchedIncomeTransactionCount,
    includedIncomeTransactionCount,
    skippedIncomeTransactionCount,
  };
}
