import type { AlaskaApocCampaignIncomeRow } from "./alaskaApocClient.js";
import { parseAlaskaApocDateYear } from "./alaskaApocClient.js";

export type AlaskaDirectContributionAggregationInput = {
  candidateName: string;
  electionYear: number;
  incomeRows: readonly AlaskaApocCampaignIncomeRow[];
  candidateFilerId?: string | null;
  candidateFilerName?: string | null;
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type AlaskaDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type AlaskaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type AlaskaDirectContributionAggregationResult = {
  summary: AlaskaDirectFinanceSummary;
  directBreakdowns: AlaskaFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: AlaskaFinanceDirectBreakdown["categoryType"];
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
    throw new Error(`Invalid Alaska direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Alaska direct contribution aggregation ${fieldName}: ${value}`);
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

function rowYear(row: AlaskaApocCampaignIncomeRow): number | null {
  return row.reportYear ?? parseAlaskaApocDateYear(row.date);
}

function isCycleYear(input: { row: AlaskaApocCampaignIncomeRow; electionYear: number }): boolean {
  const year = rowYear(input.row);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isFiledStatus(status: string): boolean {
  const key = normalizeTextKey(status);
  return !/\b(REJECTED|VOID|VOIDED|DELETED|WITHDRAWN)\b/.test(key);
}

function matchesCandidateFiler(input: {
  row: AlaskaApocCampaignIncomeRow;
  candidateName: string;
  candidateFilerId?: string | null;
  candidateFilerName?: string | null;
}): boolean {
  const rowFilerId = normalizeTextKey(input.row.filerId);
  const rowFilerName = normalizeTextKey(input.row.filerName);
  const rowName = normalizeTextKey(input.row.name);
  const candidateName = normalizeTextKey(input.candidateName);
  const candidateFilerId = normalizeTextKey(input.candidateFilerId);
  const candidateFilerName = normalizeTextKey(input.candidateFilerName);

  if (candidateFilerId && rowFilerId === candidateFilerId) {
    return true;
  }
  if (candidateFilerName && rowFilerName === candidateFilerName) {
    return true;
  }
  return Boolean(candidateName && (rowFilerName.includes(candidateName) || rowName.includes(candidateName)));
}

function contributorIdentityKey(row: AlaskaApocCampaignIncomeRow): string {
  const parts = [row.contributor, row.address, row.city, row.state, row.zip, row.employer, row.occupation]
    .map(normalizeTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  return "unknown";
}

function contributionSizeBucket(amount: number): string {
  if (amount < 100) {
    return "$0.01-$99.99";
  }
  if (amount < 250) {
    return "$100-$249.99";
  }
  if (amount < 500) {
    return "$250-$499.99";
  }
  if (amount < 1_000) {
    return "$500-$999.99";
  }
  if (amount < 5_000) {
    return "$1,000-$4,999.99";
  }
  return "$5,000+";
}

function aggregateKey(categoryType: Aggregate["categoryType"], categoryName: string): string {
  return `${categoryType}\u0000${normalizeTextKey(categoryName)}`;
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  input: { categoryType: Aggregate["categoryType"]; categoryName: string; amountCents: number; contributorKey: string }
): void {
  const categoryName = input.categoryName.trim().replace(/\s+/g, " ");
  if (!categoryName) {
    return;
  }
  const key = aggregateKey(input.categoryType, categoryName);
  const existing = aggregates.get(key);
  if (existing) {
    existing.amountCents += input.amountCents;
    existing.contributorKeys.add(input.contributorKey);
    return;
  }
  aggregates.set(key, {
    categoryType: input.categoryType,
    categoryName,
    amountCents: input.amountCents,
    contributorKeys: new Set([input.contributorKey]),
  });
}

function toDirectBreakdowns(input: {
  aggregates: Iterable<Aggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): AlaskaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: AlaskaFinanceDirectBreakdown[] = [];
  for (const categoryType of ["occupation", "contribution_size"] as const) {
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

export function aggregateAlaskaDirectContributions(
  input: AlaskaDirectContributionAggregationInput
): AlaskaDirectContributionAggregationResult {
  const candidateName = requireNonEmpty(input.candidateName, "Alaska candidate name");
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

  for (const row of input.incomeRows) {
    if (
      !matchesCandidateFiler({
        row,
        candidateName,
        candidateFilerId: input.candidateFilerId,
        candidateFilerName: input.candidateFilerName,
      })
    ) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = amountToCents(row.amount);
    if (amountCents === null || amountCents <= 0 || !isCycleYear({ row, electionYear }) || !isFiledStatus(row.status)) {
      skippedContributionRowCount += 1;
      continue;
    }

    totalReceiptsCents += amountCents;
    if (!row.contributor.trim()) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    directContributionTotalCents += amountCents;
    const contributorKey = contributorIdentityKey(row);
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: row.occupation,
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
      directContributionTotal: centsToDollars(directContributionTotalCents),
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
