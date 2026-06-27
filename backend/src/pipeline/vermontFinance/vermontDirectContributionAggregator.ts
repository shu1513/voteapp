import type { VermontContributionRow } from "./vermontCampaignFinanceClient.js";

export type VermontDirectContributionAggregationInput = {
  filerRegistrationGuid: string;
  electionYear: number;
  contributionRows: readonly VermontContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type VermontDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type VermontFinanceDirectBreakdown = {
  categoryType: "contributor_source_type" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type VermontDirectContributionAggregationResult = {
  summary: VermontDirectFinanceSummary;
  directBreakdowns: VermontFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: VermontFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Vermont direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Vermont direct contribution aggregation ${fieldName}: ${value}`);
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

function isCandidateFiler(row: VermontContributionRow): boolean {
  const filerType = normalizeTextKey(`${row.filerTypeCode ?? ""} ${row.filerTypeDescription ?? ""}`);
  return /\b(CAN|CANDIDATE)\b/.test(filerType);
}

function isDirectContribution(input: { row: VermontContributionRow; electionYear: number }): boolean {
  const amountCents = amountToCents(input.row.transactionAmount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    input.row.electionYear === input.electionYear &&
    isCandidateFiler(input.row)
  );
}

function contributorIdentityKey(row: VermontContributionRow): string {
  const parts = [
    row.sourceName,
    row.sourceFirstName,
    row.sourceMiddleName,
    row.sourceLastName,
    row.addressLine1,
    row.city,
    row.stateCode,
    row.zipCode,
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  return normalizeTextKey(row.guid) || "unknown";
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

function contributorSourceType(row: VermontContributionRow): string | null {
  return row.transactionSource?.trim() || row.transactionSourceTypeCode?.trim() || null;
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
}): VermontFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const bucket = byCategory.get(aggregate.categoryType) ?? [];
    bucket.push(aggregate);
    byCategory.set(aggregate.categoryType, bucket);
  }

  const result: VermontFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["contributor_source_type", "contribution_size"];
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

export function aggregateVermontDirectContributions(
  input: VermontDirectContributionAggregationInput
): VermontDirectContributionAggregationResult {
  const filerRegistrationGuid = requireNonEmpty(input.filerRegistrationGuid, "Vermont filer registration guid");
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

  for (const row of input.contributionRows) {
    if (row.filerRegistrationGuid.trim() !== filerRegistrationGuid) {
      continue;
    }
    matchedContributionRowCount += 1;
    const amountCents = amountToCents(row.transactionAmount);
    if (amountCents === null || !isDirectContribution({ row, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    totalReceiptsCents += amountCents;
    const contributorKey = contributorIdentityKey(row);
    addAggregate(aggregates, {
      categoryType: "contributor_source_type",
      categoryName: contributorSourceType(row),
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
