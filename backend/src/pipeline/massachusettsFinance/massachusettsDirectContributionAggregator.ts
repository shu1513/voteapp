import type { MassachusettsOcpfContributionItem } from "./massachusettsOcpfClient.js";

export type MassachusettsDirectContributionAggregationInput = {
  candidateCpfId: string;
  electionYear: number;
  contributionItems: readonly MassachusettsOcpfContributionItem[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type MassachusettsDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type MassachusettsFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type MassachusettsDirectContributionAggregationResult = {
  summary: MassachusettsDirectFinanceSummary;
  directBreakdowns: MassachusettsFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: MassachusettsFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DIRECT_DONOR_RECORD_TYPE = "INDIVIDUAL";

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Massachusetts direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Massachusetts direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeCpfId(value: string | undefined): string {
  return (value ?? "").trim();
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

function parseMassachusettsOcpfDateYear(raw: string | undefined): number | null {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[3]) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number(isoMatch[1]);
  }
  return null;
}

function isElectionYearItem(input: { item: MassachusettsOcpfContributionItem; electionYear: number }): boolean {
  const year = parseMassachusettsOcpfDateYear(input.item.date);
  return year === input.electionYear;
}

function recordTypeKey(item: MassachusettsOcpfContributionItem): string {
  return normalizeTextKey(item.recordTypeDescription);
}

function isCandidateCpfItem(input: { item: MassachusettsOcpfContributionItem; candidateCpfId: string }): boolean {
  return normalizeCpfId(input.item.cpfId) === input.candidateCpfId;
}

export function isMassachusettsTotalReceipt(input: {
  item: MassachusettsOcpfContributionItem;
  candidateCpfId: string;
  electionYear: number;
}): boolean {
  const candidateCpfId = requireNonEmpty(input.candidateCpfId, "Massachusetts candidate CPF ID");
  const amountCents = amountToCents(input.item.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCandidateCpfItem({ item: input.item, candidateCpfId }) &&
    isElectionYearItem({ item: input.item, electionYear: normalizeElectionYear(input.electionYear) })
  );
}

export function isMassachusettsDirectDonorSupportReceipt(input: {
  item: MassachusettsOcpfContributionItem;
  candidateCpfId: string;
  electionYear: number;
}): boolean {
  return isMassachusettsTotalReceipt(input) && recordTypeKey(input.item) === DIRECT_DONOR_RECORD_TYPE;
}

function contributorIdentityKey(item: MassachusettsOcpfContributionItem): string {
  const parts = [item.contributorName, item.employer, item.occupation]
    .map(normalizeTextKey)
    .filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : normalizeTextKey(item.itemId) || "unknown";
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
}): MassachusettsFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: MassachusettsFinanceDirectBreakdown[] = [];
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

export function aggregateMassachusettsDirectContributions(
  input: MassachusettsDirectContributionAggregationInput
): MassachusettsDirectContributionAggregationResult {
  const candidateCpfId = requireNonEmpty(input.candidateCpfId, "Massachusetts candidate CPF ID");
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

  for (const item of input.contributionItems) {
    if (!isCandidateCpfItem({ item, candidateCpfId })) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = amountToCents(item.amount);
    if (amountCents === null || amountCents <= 0 || !isElectionYearItem({ item, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    totalReceiptsCents += amountCents;
    if (recordTypeKey(item) !== DIRECT_DONOR_RECORD_TYPE) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    directContributionTotalCents += amountCents;
    const contributorKey = contributorIdentityKey(item);
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: item.occupation,
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
