import type { NewJerseyElecContributionRow } from "./newJerseyElecClient.js";

export type NewJerseyDirectContributionAggregationInput = {
  entityS: number;
  electionYear: number;
  contributions: readonly NewJerseyElecContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type NewJerseyDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type NewJerseyFinanceDirectBreakdown = {
  categoryType: "occupation" | "employer" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type NewJerseyDirectContributionAggregationResult = {
  summary: NewJerseyDirectFinanceSummary;
  directBreakdowns: NewJerseyFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: NewJerseyFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

function normalizeEntityS(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid New Jersey direct contribution aggregation entityS: ${value}`);
  }
  return value;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1980 || value > 2100) {
    throw new Error(`Invalid New Jersey direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Jersey direct contribution aggregation ${fieldName}: ${value}`);
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

export function isNewJerseyDirectDonorSupportContribution(input: {
  contribution: NewJerseyElecContributionRow;
  entityS: number;
  electionYear: number;
}): boolean {
  const amountCents = amountToCents(input.contribution.amount);
  return (
    input.contribution.entityS === normalizeEntityS(input.entityS) &&
    (input.contribution.electionYear === null || input.contribution.electionYear === normalizeElectionYear(input.electionYear)) &&
    amountCents !== null &&
    amountCents > 0 &&
    input.contribution.isIndividual === true
  );
}

function contributorIdentityKey(contribution: NewJerseyElecContributionRow): string {
  const parts = [
    contribution.contributorName,
    contribution.employerName,
    contribution.occupationName,
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  return "unknown";
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
    categoryName: string | null | undefined;
    amountCents: number;
    contributorKey: string;
  }
): void {
  const categoryName = input.categoryName?.trim().replace(/\s+/g, " ") ?? "";
  if (!categoryName || !normalizeTextKey(categoryName)) {
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
}): NewJerseyFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: NewJerseyFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["occupation", "employer", "contribution_size"];
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

export function aggregateNewJerseyDirectContributions(
  input: NewJerseyDirectContributionAggregationInput
): NewJerseyDirectContributionAggregationResult {
  const entityS = normalizeEntityS(input.entityS);
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;
  const aggregates = new Map<string, Aggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;
  let totalReceiptsCents = 0;

  for (const contribution of input.contributions) {
    if (contribution.entityS !== entityS) {
      continue;
    }

    matchedContributionRowCount += 1;
    const amountCents = amountToCents(contribution.amount);
    if (
      amountCents === null ||
      amountCents <= 0 ||
      contribution.isIndividual !== true ||
      (contribution.electionYear !== null && contribution.electionYear !== electionYear)
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    totalReceiptsCents += amountCents;
    const contributorKey = contributorIdentityKey(contribution);
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: contribution.occupationName,
      amountCents,
      contributorKey,
    });
    addAggregate(aggregates, {
      categoryType: "employer",
      categoryName: contribution.employerName,
      amountCents,
      contributorKey,
    });
    addAggregate(aggregates, {
      categoryType: "contribution_size",
      categoryName: contributionSizeBucket(centsToDollars(amountCents)),
      amountCents,
      contributorKey,
    });
  }

  return {
    summary: {
      totalReceipts: centsToDollars(totalReceiptsCents),
      directContributionTotal: centsToDollars(totalReceiptsCents),
      sourceUrl,
    },
    directBreakdowns: toDirectBreakdowns({
      aggregates: aggregates.values(),
      sourceUrl,
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
