import { normalizePennsylvaniaCampaignFinanceExportYear } from "./pennsylvaniaCampaignFinanceArtifactCache.js";
import type { PennsylvaniaCampaignFinanceContributionRow } from "./pennsylvaniaCampaignFinanceReader.js";

export type PennsylvaniaDirectContributionAggregationInput = {
  filerId: string;
  electionYear: number;
  contributionRows: readonly PennsylvaniaCampaignFinanceContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type PennsylvaniaDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type PennsylvaniaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type PennsylvaniaDirectContributionAggregationResult = {
  summary: PennsylvaniaDirectFinanceSummary;
  directBreakdowns: PennsylvaniaFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionEventCount: number;
  skippedContributionEventCount: number;
};

export type PennsylvaniaContributionEvent = {
  rawDate: string;
  rawAmount: string;
};

type Aggregate = {
  categoryType: PennsylvaniaFinanceDirectBreakdown["categoryType"];
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

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Pennsylvania direct contribution aggregation ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
}

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAmountCents(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (!normalized || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
    return null;
  }
  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parsePennsylvaniaContributionDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const compactMatch = /^(\d{4})(\d{2})(\d{2})$/.exec(trimmed);
  if (compactMatch) {
    return Number(compactMatch[1]);
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

export function pennsylvaniaElectionCycleStartYear(electionYear: number): number {
  return normalizePennsylvaniaCampaignFinanceExportYear(electionYear) - 1;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parsePennsylvaniaContributionDateYear(input.rawDate);
  if (year === null) {
    return false;
  }
  return year >= pennsylvaniaElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
}

function contributionEventsFromRow(row: PennsylvaniaCampaignFinanceContributionRow): PennsylvaniaContributionEvent[] {
  return [
    { rawDate: row.CONTDATE1, rawAmount: row.CONTAMT1 },
    { rawDate: row.CONTDATE2, rawAmount: row.CONTAMT2 },
    { rawDate: row.CONTDATE3, rawAmount: row.CONTAMT3 },
  ].filter((event) => event.rawDate.trim().length > 0 || event.rawAmount.trim().length > 0);
}

export function isPennsylvaniaDirectContributionEvent(input: {
  event: PennsylvaniaContributionEvent;
  electionYear: number;
}): boolean {
  const amountCents = parseAmountCents(input.event.rawAmount);
  return amountCents !== null && amountCents > 0 && isCycleYear({ rawDate: input.event.rawDate, electionYear: input.electionYear });
}

function contributorIdentityKey(row: PennsylvaniaCampaignFinanceContributionRow): string {
  const parts = [
    row.CONTRIBUTOR,
    row.ADDRESS1,
    row.ADDRESS2,
    row.CITY,
    row.STATE,
    row.ZIPCODE,
    row.OCCUPATION,
    row.ENAME,
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  return normalizeTextKey(row.CampaignFinanceID) || "unknown";
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
  const categoryName = input.categoryName.trim().replace(/\s+/g, " ");
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
}): PennsylvaniaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: PennsylvaniaFinanceDirectBreakdown[] = [];
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

export function aggregatePennsylvaniaDirectContributions(
  input: PennsylvaniaDirectContributionAggregationInput
): PennsylvaniaDirectContributionAggregationResult {
  const filerId = normalizeId(requireNonEmpty(input.filerId, "Pennsylvania filer id"));
  const electionYear = normalizePennsylvaniaCampaignFinanceExportYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;
  const aggregates = new Map<string, Aggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionEventCount = 0;
  let skippedContributionEventCount = 0;
  let totalReceiptsCents = 0;
  let directContributionTotalCents = 0;

  for (const row of input.contributionRows) {
    if (normalizeId(row.FilerID) !== filerId) {
      continue;
    }
    matchedContributionRowCount += 1;
    const contributorKey = contributorIdentityKey(row);

    for (const event of contributionEventsFromRow(row)) {
      const amountCents = parseAmountCents(event.rawAmount);
      if (
        amountCents === null ||
        amountCents <= 0 ||
        !isCycleYear({ rawDate: event.rawDate, electionYear })
      ) {
        skippedContributionEventCount += 1;
        continue;
      }

      includedContributionEventCount += 1;
      totalReceiptsCents += amountCents;
      directContributionTotalCents += amountCents;
      addAggregate(aggregates, {
        categoryType: "occupation",
        categoryName: row.OCCUPATION,
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
    includedContributionEventCount,
    skippedContributionEventCount,
  };
}
