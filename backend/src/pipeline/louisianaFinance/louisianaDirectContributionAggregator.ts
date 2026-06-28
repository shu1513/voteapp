import type { LouisianaCampaignFinanceCsvRow } from "./louisianaCampaignFinanceArtifactReader.js";

export type LouisianaDirectContributionAggregationInput = {
  filerNumber: string;
  electionYear: number;
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type LouisianaDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type LouisianaFinanceDirectBreakdown = {
  categoryType: "contribution_size" | "contributor_type";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type LouisianaDirectContributionAggregationResult = {
  summary: LouisianaDirectFinanceSummary;
  directBreakdowns: LouisianaFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: LouisianaFinanceDirectBreakdown["categoryType"];
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
    throw new Error(`Invalid Louisiana direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Louisiana direct contribution aggregation ${fieldName}: ${value}`);
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

function firstNonEmpty(row: LouisianaCampaignFinanceCsvRow, keys: readonly string[]): string {
  for (const key of keys) {
    const value = row[key]?.trim() ?? "";
    if (value) {
      return value;
    }
  }
  return "";
}

function normalizeFilerNumber(value: string): string {
  return value.trim().replace(/\s+/g, "");
}

function parseAmountCents(raw: string): number | null {
  const trimmed = raw.trim();
  const isParentheticalNegative = /^\(.+\)$/.test(trimmed);
  const normalized = trimmed.replace(/[,$()]/g, "");
  if (!normalized || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
    return null;
  }
  const amount = Number(normalized) * (isParentheticalNegative ? -1 : 1);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseYearFromDate(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const isoMatch = /^(\d{4})-\d{1,2}-\d{1,2}\b/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number.parseInt(isoMatch[1], 10);
  }
  const slashMatch = /^\d{1,2}\/\d{1,2}\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[1]) {
    return Number.parseInt(slashMatch[1], 10);
  }
  return null;
}

function isElectionCycleDate(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseYearFromDate(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
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

function contributorIdentityKey(row: LouisianaCampaignFinanceCsvRow): string {
  const parts = [
    firstNonEmpty(row, ["ContributorName", "Contributor Name"]),
    firstNonEmpty(row, ["ContributorAddr1", "Contributor Address 1"]),
    firstNonEmpty(row, ["ContributorAddr2", "Contributor Address 2"]),
    firstNonEmpty(row, ["ContributorCity", "Contributor City"]),
    firstNonEmpty(row, ["ContributorrState", "ContributorState", "Contributor State"]),
    firstNonEmpty(row, ["ContributorZip", "Contributor Zip"]),
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : "unknown";
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
}): LouisianaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const bucket = byCategory.get(aggregate.categoryType) ?? [];
    bucket.push(aggregate);
    byCategory.set(aggregate.categoryType, bucket);
  }

  const result: LouisianaFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["contributor_type", "contribution_size"];
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

export function aggregateLouisianaDirectContributions(
  input: LouisianaDirectContributionAggregationInput
): LouisianaDirectContributionAggregationResult {
  const filerNumber = normalizeFilerNumber(requireNonEmpty(input.filerNumber, "Louisiana filer number"));
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
    if (normalizeFilerNumber(firstNonEmpty(row, ["FilerNumber", "Filer Number"])) !== filerNumber) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(firstNonEmpty(row, ["ContributionAmt", "Contribution Amount", "Amount"]));
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !isElectionCycleDate({
        rawDate: firstNonEmpty(row, ["ContributionDate", "Contribution Date", "ReceiptDate", "Receipt Date"]),
        electionYear,
      })
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    totalReceiptsCents += amountCents;
    const contributorKey = contributorIdentityKey(row);
    addAggregate(aggregates, {
      categoryType: "contributor_type",
      categoryName: firstNonEmpty(row, ["ContributorTypeCode", "Contributor Type Code", "ContributorType"]),
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
