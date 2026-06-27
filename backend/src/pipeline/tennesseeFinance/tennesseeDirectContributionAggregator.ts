import {
  normalizeTennesseeCandidateNameKeys,
  type TennesseeCandidateCommitteeMatch,
} from "./tennesseeCandidateCommitteeResolver.js";
import type { TennesseeCampContributionRecord } from "./tennesseeCampClient.js";

export type TennesseeDirectContributionAggregationInput = {
  candidate: Pick<TennesseeCandidateCommitteeMatch, "ownerName" | "candidateName">;
  electionYear: number;
  contributions: readonly TennesseeCampContributionRecord[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type TennesseeDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type TennesseeFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type TennesseeDirectContributionAggregationResult = {
  summary: TennesseeDirectFinanceSummary;
  directBreakdowns: TennesseeFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: TennesseeFinanceDirectBreakdown["categoryType"];
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
    throw new Error(`Invalid Tennessee direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Tennessee direct contribution aggregation ${fieldName}: ${value}`);
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

function parseDateYear(raw: string | null | undefined): number | null {
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

export function tennesseeElectionCycleStartYear(electionYear: number): number {
  return normalizeElectionYear(electionYear) - 1;
}

function isInElectionCycle(input: { contribution: TennesseeCampContributionRecord; electionYear: number }): boolean {
  const year = parseDateYear(input.contribution.date) ?? input.contribution.electionYear;
  return year !== null && year >= tennesseeElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
}

function recipientMatchesCandidate(input: {
  contribution: TennesseeCampContributionRecord;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  const recipientName = input.contribution.recipientName ?? "";
  for (const key of normalizeTennesseeCandidateNameKeys(recipientName)) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function isMonetaryNonAdjustment(contribution: TennesseeCampContributionRecord): boolean {
  return normalizeTextKey(contribution.type) === "MONETARY" && normalizeTextKey(contribution.adjustment) !== "Y";
}

export function isTennesseeDirectDonorSupportContribution(input: {
  contribution: TennesseeCampContributionRecord;
  candidateNameKeys: ReadonlySet<string>;
  electionYear: number;
}): boolean {
  const amountCents = amountToCents(input.contribution.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isMonetaryNonAdjustment(input.contribution) &&
    recipientMatchesCandidate(input) &&
    isInElectionCycle({ contribution: input.contribution, electionYear: normalizeElectionYear(input.electionYear) })
  );
}

function contributorIdentityKey(contribution: TennesseeCampContributionRecord): string {
  const parts = [contribution.contributorName, contribution.contributorEmployer, contribution.contributorOccupation]
    .map(normalizeTextKey)
    .filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : "unknown";
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
}): TennesseeFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }
  const result: TennesseeFinanceDirectBreakdown[] = [];
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

export function aggregateTennesseeDirectContributions(
  input: TennesseeDirectContributionAggregationInput
): TennesseeDirectContributionAggregationResult {
  const ownerName = requireNonEmpty(input.candidate.ownerName, "Tennessee CAMP owner name");
  const candidateName = requireNonEmpty(input.candidate.candidateName, "Tennessee candidate name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;
  const candidateNameKeys = new Set([
    ...normalizeTennesseeCandidateNameKeys(ownerName),
    ...normalizeTennesseeCandidateNameKeys(candidateName),
  ]);
  const aggregates = new Map<string, Aggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;
  let totalReceiptsCents = 0;
  let directContributionTotalCents = 0;

  for (const contribution of input.contributions) {
    if (!recipientMatchesCandidate({ contribution, candidateNameKeys })) {
      continue;
    }
    matchedContributionRowCount += 1;
    if (!isTennesseeDirectDonorSupportContribution({ contribution, candidateNameKeys, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }
    const amountCents = amountToCents(contribution.amount);
    if (amountCents === null || amountCents <= 0) {
      skippedContributionRowCount += 1;
      continue;
    }

    totalReceiptsCents += amountCents;
    directContributionTotalCents += amountCents;
    includedContributionRowCount += 1;
    const contributorKey = contributorIdentityKey(contribution);
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: contribution.contributorOccupation,
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
