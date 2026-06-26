import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import type { ArizonaSpotlightIncomeTransaction } from "./arizonaSpotlightClient.js";
import type { ArizonaOutsideSpendingGroup, ArizonaSupportOppose } from "./arizonaOutsideSpendingAggregator.js";

export type ArizonaFinanceOutsideGroupBreakdown = {
  committeeId: string;
  supportOppose: ArizonaSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type ArizonaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly ArizonaOutsideSpendingGroup[];
  incomeTransactions: readonly ArizonaSpotlightIncomeTransaction[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type ArizonaOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: ArizonaFinanceOutsideGroupBreakdown[];
  matchedIncomeTransactionCount: number;
  includedIncomeTransactionCount: number;
  skippedIncomeTransactionCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: ArizonaSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
  sourceUrl: string | null;
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: ArizonaSupportOppose;
  industrySlug: string;
  amountCents: number;
  contributorKeys: Set<string>;
  sourceUrl: string | null;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2002 || value > 2100) {
    throw new Error(`Invalid Arizona outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Arizona outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return 0;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Arizona outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
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

function groupKey(input: { committeeId: string; supportOppose: ArizonaSupportOppose }): string {
  return `${normalizeCommitteeId(input.committeeId)}\u0000${input.supportOppose}`;
}

function donorKey(input: { committeeId: string; supportOppose: ArizonaSupportOppose; normalizedName: string }): string {
  return `${normalizeCommitteeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: { committeeId: string; supportOppose: ArizonaSupportOppose; industrySlug: string }): string {
  return `${normalizeCommitteeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function contributorIdentityKey(transaction: ArizonaSpotlightIncomeTransaction): string {
  const parts = [transaction.transactionName, transaction.city, transaction.state, transaction.zipCode]
    .map(normalizeTextKey)
    .filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : "unknown";
}

function addDonorAggregate(
  aggregates: Map<string, DonorAggregate>,
  input: {
    committeeId: string;
    supportOppose: ArizonaSupportOppose;
    displayName: string;
    amountCents: number;
    sourceUrl: string | null;
  }
): void {
  const displayName = input.displayName.trim().replace(/\s+/g, " ");
  if (!displayName) {
    return;
  }
  const normalizedName = normalizeFinanceLabel(displayName, "donor");
  if (!normalizedName) {
    return;
  }

  const key = donorKey({ ...input, normalizedName });
  const existing = aggregates.get(key);
  if (!existing) {
    aggregates.set(key, {
      committeeId: input.committeeId,
      supportOppose: input.supportOppose,
      displayName,
      normalizedName,
      amountCents: input.amountCents,
      sourceUrl: input.sourceUrl,
    });
    return;
  }
  existing.amountCents += input.amountCents;
  existing.sourceUrl ??= input.sourceUrl;
}

function addIndustryAggregate(
  aggregates: Map<string, IndustryAggregate>,
  input: {
    committeeId: string;
    supportOppose: ArizonaSupportOppose;
    industrySlug: string;
    amountCents: number;
    contributorKey: string;
    sourceUrl: string | null;
  }
): void {
  const key = industryKey(input);
  const existing = aggregates.get(key);
  if (!existing) {
    aggregates.set(key, {
      committeeId: input.committeeId,
      supportOppose: input.supportOppose,
      industrySlug: input.industrySlug,
      amountCents: input.amountCents,
      contributorKeys: new Set([input.contributorKey]),
      sourceUrl: input.sourceUrl,
    });
    return;
  }
  existing.amountCents += input.amountCents;
  existing.contributorKeys.add(input.contributorKey);
  existing.sourceUrl ??= input.sourceUrl;
}

function industrySlugForTransaction(transaction: ArizonaSpotlightIncomeTransaction): string | null {
  const donorName = transaction.transactionName?.trim();
  if (donorName) {
    const donorClassification = classifyFinanceLabel({ rawLabel: donorName, labelType: "donor" });
    if (donorClassification.industrySlug) {
      return donorClassification.industrySlug;
    }
  }

  const employer = transaction.employer?.trim();
  if (employer) {
    const employerClassification = classifyFinanceLabel({ rawLabel: employer, labelType: "employer" });
    return employerClassification.industrySlug;
  }

  return null;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  maxBreakdownsPerCategory: number;
  minIndustryAmountCents: number;
}): ArizonaFinanceOutsideGroupBreakdown[] {
  const result: ArizonaFinanceOutsideGroupBreakdown[] = [];
  const donorsByBucket = new Map<string, DonorAggregate[]>();
  const industriesByBucket = new Map<string, IndustryAggregate[]>();

  for (const donor of input.donors) {
    const key = groupKey(donor);
    const bucket = donorsByBucket.get(key) ?? [];
    bucket.push(donor);
    donorsByBucket.set(key, bucket);
  }
  for (const industry of input.industries) {
    const key = groupKey(industry);
    const bucket = industriesByBucket.get(key) ?? [];
    bucket.push(industry);
    industriesByBucket.set(key, bucket);
  }

  for (const bucket of [...donorsByBucket.values()].sort((left, right) =>
    groupKey(left[0]!).localeCompare(groupKey(right[0]!))
  )) {
    for (const donor of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        committeeId: donor.committeeId,
        supportOppose: donor.supportOppose,
        categoryType: "donor",
        categoryName: donor.displayName,
        amount: centsToDollars(donor.amountCents),
        contributorCount: 1,
        sourceUrl: donor.sourceUrl,
      });
    }
  }

  for (const bucket of [...industriesByBucket.values()].sort((left, right) =>
    groupKey(left[0]!).localeCompare(groupKey(right[0]!))
  )) {
    for (const industry of bucket
      .filter((industry) => industry.amountCents >= input.minIndustryAmountCents)
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        committeeId: industry.committeeId,
        supportOppose: industry.supportOppose,
        categoryType: "industry",
        categoryName: industry.industrySlug,
        amount: centsToDollars(industry.amountCents),
        contributorCount: industry.contributorKeys.size,
        sourceUrl: industry.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateArizonaOutsideGroupContributions(
  input: ArizonaOutsideGroupContributionAggregationInput
): ArizonaOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const outsideGroupsByCommitteeId = new Map<string, ArizonaOutsideSpendingGroup[]>();

  for (const group of input.outsideGroups) {
    const committeeId = normalizeCommitteeId(group.committeeId);
    if (!committeeId) {
      continue;
    }
    const existing = outsideGroupsByCommitteeId.get(committeeId) ?? [];
    existing.push(group);
    outsideGroupsByCommitteeId.set(committeeId, existing);
  }

  if (outsideGroupsByCommitteeId.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      matchedIncomeTransactionCount: 0,
      includedIncomeTransactionCount: 0,
      skippedIncomeTransactionCount: 0,
    };
  }

  const donors = new Map<string, DonorAggregate>();
  const industries = new Map<string, IndustryAggregate>();
  let matchedIncomeTransactionCount = 0;
  let includedIncomeTransactionCount = 0;
  let skippedIncomeTransactionCount = 0;

  for (const transaction of input.incomeTransactions) {
    const committeeId = normalizeCommitteeId(transaction.committeeId);
    const matchingGroups = outsideGroupsByCommitteeId.get(committeeId) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedIncomeTransactionCount += 1;

    const amountCents = amountToCents(transaction.amount);
    const donorName = transaction.transactionName?.trim().replace(/\s+/g, " ") ?? "";
    if (!donorName || amountCents === null || amountCents <= 0 || !isCycleYear({ transaction, electionYear })) {
      skippedIncomeTransactionCount += 1;
      continue;
    }

    includedIncomeTransactionCount += 1;
    const contributorKey = contributorIdentityKey(transaction);
    const sourceUrl = transaction.sourceUrl ?? input.sourceUrl ?? null;
    const industrySlug = industrySlugForTransaction(transaction);

    for (const group of matchingGroups) {
      addDonorAggregate(donors, {
        committeeId: group.committeeId,
        supportOppose: group.supportOppose,
        displayName: donorName,
        amountCents,
        sourceUrl,
      });
      if (industrySlug) {
        addIndustryAggregate(industries, {
          committeeId: group.committeeId,
          supportOppose: group.supportOppose,
          industrySlug,
          amountCents,
          contributorKey,
          sourceUrl,
        });
      }
    }
  }

  return {
    outsideGroupBreakdowns: toBreakdowns({
      donors: donors.values(),
      industries: industries.values(),
      maxBreakdownsPerCategory,
      minIndustryAmountCents,
    }),
    matchedIncomeTransactionCount,
    includedIncomeTransactionCount,
    skippedIncomeTransactionCount,
  };
}
