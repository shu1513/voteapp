import {
  centsToFloridaDollars,
  floridaElectionCycleStartYear,
  normalizeFloridaDisplayText,
  normalizeFloridaTextKey,
  parseFloridaAmountCents,
  parseFloridaDateYear,
  type FloridaContributionRow,
} from "./floridaCampaignFinanceRows.js";

export { floridaElectionCycleStartYear } from "./floridaCampaignFinanceRows.js";

export type FloridaDirectContributionAggregationInput = {
  recipientName: string;
  recipientNames?: readonly string[];
  electionYear: number;
  contributionRows: readonly FloridaContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type FloridaDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type FloridaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type FloridaDirectContributionAggregationResult = {
  summary: FloridaDirectFinanceSummary;
  directBreakdowns: FloridaFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: FloridaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const INVALID_OCCUPATION_KEYS = new Set([
  "INFO REQUESTED",
  "INFORMATION REQUESTED",
  "N A",
  "NA",
  "NONE",
  "NOT APPLICABLE",
  "NULL",
  "UNKNOWN",
]);

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1996 || value > 2100) {
    throw new Error(`Invalid Florida direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Florida direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeRecipientNames(input: { recipientName: string; recipientNames?: readonly string[] }): Set<string> {
  const names = new Set<string>();
  names.add(normalizeFloridaTextKey(requireNonEmpty(input.recipientName, "Florida recipient name")));
  for (const name of input.recipientNames ?? []) {
    const normalized = normalizeFloridaTextKey(name);
    if (normalized) {
      names.add(normalized);
    }
  }
  return names;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseFloridaDateYear(input.rawDate);
  if (year === null) {
    return false;
  }
  return year >= floridaElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
}

function isExcludedTransactionType(row: FloridaContributionRow): boolean {
  const normalizedType = normalizeFloridaTextKey(row.transactionType);
  const normalizedInKindDescription = normalizeFloridaTextKey(row.inKindDescription);
  const combined = `${normalizedType} ${normalizedInKindDescription}`.trim();
  if (!combined) {
    return false;
  }
  return /\b(?:INK|IN KIND|INKIND|LOAN|REFUND|REBATE|RETURNED|REVERSAL|TRANSFER|MATCHING FUNDS?)\b/.test(combined);
}

function isExcludedContributor(row: FloridaContributionRow): boolean {
  const contributor = normalizeFloridaTextKey(row.contributorName);
  return contributor === "STATE OF FLORIDA" || contributor === "FLORIDA ELECTION CAMPAIGN FINANCING TRUST FUND";
}

export function isFloridaTotalReceipt(input: { row: FloridaContributionRow; electionYear: number }): boolean {
  const amountCents = parseFloridaAmountCents(input.row.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    normalizeFloridaTextKey(input.row.recipientName).length > 0 &&
    !isExcludedTransactionType(input.row) &&
    !isExcludedContributor(input.row) &&
    isCycleYear({ rawDate: input.row.contributionDate, electionYear: normalizeElectionYear(input.electionYear) })
  );
}

export function isFloridaDirectDonorSupportReceipt(input: {
  row: FloridaContributionRow;
  electionYear: number;
}): boolean {
  return isFloridaTotalReceipt(input);
}

function contributorIdentityKey(row: FloridaContributionRow): string {
  const parts = [
    row.contributorName,
    row.address,
    row.city,
    row.state,
    row.zip,
    row.occupation,
  ]
    .map(normalizeFloridaTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  return "unknown";
}

function normalizeOccupation(raw: string): string | null {
  const display = normalizeFloridaDisplayText(raw);
  if (!display) {
    return null;
  }
  const key = normalizeFloridaTextKey(display);
  return INVALID_OCCUPATION_KEYS.has(key) ? null : display;
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
  input: { categoryType: Aggregate["categoryType"]; categoryName: string; amountCents: number; contributorKey: string }
): void {
  const categoryName = normalizeFloridaDisplayText(input.categoryName);
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
}): FloridaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: FloridaFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["occupation", "contribution_size"];
  for (const categoryType of categoryOrder) {
    const limit = categoryType === "contribution_size" ? Number.POSITIVE_INFINITY : input.maxBreakdownsPerCategory;
    for (const aggregate of (byCategory.get(categoryType) ?? [])
      .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))
      .slice(0, limit)) {
      result.push({
        categoryType: aggregate.categoryType,
        categoryName: aggregate.categoryName,
        amount: centsToFloridaDollars(aggregate.amountCents),
        contributorCount: aggregate.contributorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateFloridaDirectContributions(
  input: FloridaDirectContributionAggregationInput
): FloridaDirectContributionAggregationResult {
  const recipientNames = normalizeRecipientNames(input);
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
  let directContributionTotalCents = 0;

  for (const row of input.contributionRows) {
    if (!recipientNames.has(normalizeFloridaTextKey(row.recipientName))) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseFloridaAmountCents(row.amount);
    if (amountCents === null || !isFloridaDirectDonorSupportReceipt({ row, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    totalReceiptsCents += amountCents;
    directContributionTotalCents += amountCents;

    const contributorKey = contributorIdentityKey(row);
    const amount = centsToFloridaDollars(amountCents);
    const occupation = normalizeOccupation(row.occupation);
    if (occupation) {
      addAggregate(aggregates, {
        categoryType: "occupation",
        categoryName: occupation,
        amountCents,
        contributorKey,
      });
    }
    addAggregate(aggregates, {
      categoryType: "contribution_size",
      categoryName: contributionSizeBucket(amount),
      amountCents,
      contributorKey,
    });
  }

  return {
    summary: {
      totalReceipts: centsToFloridaDollars(totalReceiptsCents),
      directContributionTotal: centsToFloridaDollars(directContributionTotalCents),
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
