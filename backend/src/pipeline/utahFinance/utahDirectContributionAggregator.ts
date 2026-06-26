import type { UtahDisclosuresTransactionRow } from "./utahDisclosuresClient.js";

export type UtahDirectContributionAggregationInput = {
  electionYear: number;
  transactions: readonly UtahDisclosuresTransactionRow[];
  committeeName?: string | null;
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type UtahDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  totalDisbursements: number;
  sourceUrl: string | null;
};

export type UtahFinanceDirectBreakdown = {
  categoryType: "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type UtahDirectContributionAggregationResult = {
  summary: UtahDirectFinanceSummary;
  directBreakdowns: UtahFinanceDirectBreakdown[];
  matchedTransactionRowCount: number;
  includedContributionRowCount: number;
  skippedTransactionRowCount: number;
};

type Aggregate = {
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1998 || value > 2100) {
    throw new Error(`Invalid Utah direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Utah direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeEntityNameKey(value: string | null | undefined): string {
  return normalizeTextKey(
    (value ?? "")
      .replace(/\[[^\]]+\]/g, " ")
      .replace(/\([^()]+\)/g, " ")
  );
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

function parseUtahDateYear(value: string | undefined): number | null {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^\d{1,2}\/\d{1,2}\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[1]) {
    return Number(slashMatch[1]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number(isoMatch[1]);
  }
  return null;
}

function transactionMatchesElectionYear(input: {
  transaction: UtahDisclosuresTransactionRow;
  electionYear: number;
}): boolean {
  const transactionYear = parseUtahDateYear(input.transaction.transactionDate);
  if (transactionYear !== null) {
    return transactionYear === input.electionYear;
  }
  const filedYear = parseUtahDateYear(input.transaction.filed);
  return filedYear === null || filedYear === input.electionYear;
}

function transactionMatchesCommittee(input: {
  transaction: UtahDisclosuresTransactionRow;
  committeeName?: string | null;
}): boolean {
  const committeeKey = normalizeEntityNameKey(input.committeeName);
  if (!committeeKey) {
    return true;
  }
  return normalizeEntityNameKey(input.transaction.entityName) === committeeKey;
}

function transactionTypeKey(transaction: UtahDisclosuresTransactionRow): string {
  return normalizeTextKey(transaction.transactionType);
}

function isPositiveAmount(transaction: UtahDisclosuresTransactionRow): boolean {
  const amountCents = amountToCents(transaction.amount);
  return amountCents !== null && amountCents > 0;
}

function isNegativeAmount(transaction: UtahDisclosuresTransactionRow): boolean {
  const amountCents = amountToCents(transaction.amount);
  return amountCents !== null && amountCents < 0;
}

export function isUtahTotalReceipt(input: {
  transaction: UtahDisclosuresTransactionRow;
  electionYear: number;
  committeeName?: string | null;
}): boolean {
  return (
    isPositiveAmount(input.transaction) &&
    transactionMatchesCommittee(input) &&
    transactionMatchesElectionYear({
      transaction: input.transaction,
      electionYear: normalizeElectionYear(input.electionYear),
    }) &&
    transactionTypeKey(input.transaction).includes("CONTRIBUTION")
  );
}

export function isUtahTotalDisbursement(input: {
  transaction: UtahDisclosuresTransactionRow;
  electionYear: number;
  committeeName?: string | null;
}): boolean {
  return (
    (isPositiveAmount(input.transaction) || isNegativeAmount(input.transaction)) &&
    transactionMatchesCommittee(input) &&
    transactionMatchesElectionYear({
      transaction: input.transaction,
      electionYear: normalizeElectionYear(input.electionYear),
    }) &&
    transactionTypeKey(input.transaction).includes("EXPENDITURE")
  );
}

export function isUtahDirectDonorSupportReceipt(input: {
  transaction: UtahDisclosuresTransactionRow;
  electionYear: number;
  committeeName?: string | null;
}): boolean {
  return isUtahTotalReceipt(input) && input.transaction.loan !== true;
}

function contributorIdentityKey(transaction: UtahDisclosuresTransactionRow): string {
  const parts = [
    transaction.name,
    transaction.address1,
    transaction.address2,
    transaction.city,
    transaction.state,
    transaction.zip,
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : normalizeTextKey(transaction.transactionId) || "unknown";
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

function addAggregate(
  aggregates: Map<string, Aggregate>,
  input: {
    categoryName: string;
    amountCents: number;
    contributorKey: string;
  }
): void {
  const categoryName = input.categoryName.trim();
  if (!categoryName) {
    return;
  }
  const existing = aggregates.get(categoryName);
  if (!existing) {
    aggregates.set(categoryName, {
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
}): UtahFinanceDirectBreakdown[] {
  return [...input.aggregates]
    .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))
    .slice(0, input.maxBreakdownsPerCategory)
    .map((aggregate) => ({
      categoryType: "contribution_size" as const,
      categoryName: aggregate.categoryName,
      amount: centsToDollars(aggregate.amountCents),
      contributorCount: aggregate.contributorKeys.size,
      sourceUrl: input.sourceUrl,
    }));
}

export function aggregateUtahDirectContributions(
  input: UtahDirectContributionAggregationInput
): UtahDirectContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;
  const aggregates = new Map<string, Aggregate>();
  let totalReceiptCents = 0;
  let directContributionCents = 0;
  let totalDisbursementCents = 0;
  let matchedTransactionRowCount = 0;
  let includedContributionRowCount = 0;

  for (const transaction of input.transactions) {
    const matchesCommitteeAndYear =
      transactionMatchesCommittee({ transaction, committeeName: input.committeeName }) &&
      transactionMatchesElectionYear({ transaction, electionYear });
    if (!matchesCommitteeAndYear) {
      continue;
    }
    matchedTransactionRowCount += 1;

    const amountCents = amountToCents(transaction.amount);
    if (amountCents === null) {
      continue;
    }
    if (amountCents > 0 && isUtahTotalReceipt({ transaction, electionYear, committeeName: input.committeeName })) {
      totalReceiptCents += amountCents;
    }
    if (isUtahTotalDisbursement({ transaction, electionYear, committeeName: input.committeeName })) {
      totalDisbursementCents += Math.abs(amountCents);
    }
    if (amountCents <= 0) {
      continue;
    }
    if (!isUtahDirectDonorSupportReceipt({ transaction, electionYear, committeeName: input.committeeName })) {
      continue;
    }

    includedContributionRowCount += 1;
    directContributionCents += amountCents;
    addAggregate(aggregates, {
      categoryName: contributionSizeBucket(transaction.amount),
      amountCents,
      contributorKey: contributorIdentityKey(transaction),
    });
  }

  return {
    summary: {
      totalReceipts: centsToDollars(totalReceiptCents),
      directContributionTotal: centsToDollars(directContributionCents),
      totalDisbursements: centsToDollars(totalDisbursementCents),
      sourceUrl,
    },
    directBreakdowns: toDirectBreakdowns({
      aggregates: aggregates.values(),
      sourceUrl,
      maxBreakdownsPerCategory,
    }),
    matchedTransactionRowCount,
    includedContributionRowCount,
    skippedTransactionRowCount: matchedTransactionRowCount - includedContributionRowCount,
  };
}
