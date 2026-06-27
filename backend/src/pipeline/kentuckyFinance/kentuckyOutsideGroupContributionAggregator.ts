import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import type { KentuckyKrefContributionRecord } from "./kentuckyKrefClient.js";
import type { KentuckyOutsideSpendingGroup, KentuckySupportOppose } from "./kentuckyOutsideSpendingAggregator.js";

export type KentuckyFinanceOutsideGroupBreakdown = {
  committeeKey: string;
  supportOppose: KentuckySupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type KentuckyOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly KentuckyOutsideSpendingGroup[];
  contributionRecords: readonly KentuckyKrefContributionRecord[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type KentuckyOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: KentuckyFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeKey: string;
  supportOppose: KentuckySupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeKey: string;
  supportOppose: KentuckySupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Kentucky outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Kentucky outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Kentucky outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
}

function normalizeCommitteeKey(value: string | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ").toUpperCase();
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

function parseKrefDateYear(raw: string | undefined): number | null {
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

function isCycleRecord(input: { record: KentuckyKrefContributionRecord; electionYear: number }): boolean {
  const year = input.record.electionYear ?? parseKrefDateYear(input.record.receiptDate);
  return year !== null && year !== undefined && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isOrganizationContributor(record: KentuckyKrefContributionRecord): boolean {
  const contributorName = normalizeTextKey(record.contributorName);
  if (!contributorName) {
    return false;
  }

  const contributorType = normalizeTextKey(record.contributorType);
  if (/\b(INDIVIDUAL|PERSON|CANDIDATE|SELF)\b/.test(contributorType)) {
    return false;
  }
  if (
    /\b(BUSINESS|CORPORATION|COMPANY|COMMITTEE|PAC|ORGANIZATION|ASSOCIATION|NONPROFIT|NON PROFIT|LABOR|UNION|PARTNERSHIP|LLC|L L C|TRUST|OTHER)\b/.test(
      contributorType
    )
  ) {
    return true;
  }

  return /\b(INC|LLC|CORP|CORPORATION|COMPANY|ASSOCIATION|PAC|COMMITTEE|UNION|FOUNDATION|FUND|TRUST|LOCAL)\b/.test(
    contributorName
  );
}

function isOutsideDonorContribution(input: { record: KentuckyKrefContributionRecord; electionYear: number }): boolean {
  const amountCents = amountToCents(input.record.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCycleRecord(input) &&
    isOrganizationContributor(input.record)
  );
}

function recordCommitteeKey(record: KentuckyKrefContributionRecord): string {
  return normalizeCommitteeKey(record.toOrganizationName ?? record.recipientName);
}

function donorKey(input: {
  committeeKey: string;
  supportOppose: KentuckySupportOppose;
  normalizedName: string;
}): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  committeeKey: string;
  supportOppose: KentuckySupportOppose;
  industrySlug: string;
}): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function breakdownBucketKey(input: { committeeKey: string; supportOppose: KentuckySupportOppose }): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): KentuckyFinanceOutsideGroupBreakdown[] {
  const result: KentuckyFinanceOutsideGroupBreakdown[] = [];
  const donorsByBucket = new Map<string, DonorAggregate[]>();
  const industriesByBucket = new Map<string, IndustryAggregate[]>();

  for (const donor of input.donors) {
    const key = breakdownBucketKey(donor);
    const bucket = donorsByBucket.get(key) ?? [];
    bucket.push(donor);
    donorsByBucket.set(key, bucket);
  }
  for (const industry of input.industries) {
    const key = breakdownBucketKey(industry);
    const bucket = industriesByBucket.get(key) ?? [];
    bucket.push(industry);
    industriesByBucket.set(key, bucket);
  }

  const bucketSortKey = (bucket: Array<{ committeeKey: string; supportOppose: KentuckySupportOppose }>): string =>
    bucket[0] ? breakdownBucketKey(bucket[0]) : "";

  for (const bucket of [...donorsByBucket.values()].sort((left, right) =>
    bucketSortKey(left).localeCompare(bucketSortKey(right))
  )) {
    for (const donor of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        committeeKey: donor.committeeKey,
        supportOppose: donor.supportOppose,
        categoryType: "donor",
        categoryName: donor.displayName,
        amount: centsToDollars(donor.amountCents),
        contributorCount: 1,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  for (const bucket of [...industriesByBucket.values()].sort((left, right) =>
    bucketSortKey(left).localeCompare(bucketSortKey(right))
  )) {
    for (const industry of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        committeeKey: industry.committeeKey,
        supportOppose: industry.supportOppose,
        categoryType: "industry",
        categoryName: industry.industrySlug,
        amount: centsToDollars(industry.amountCents),
        contributorCount: industry.donorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateKentuckyOutsideGroupContributions(
  input: KentuckyOutsideGroupContributionAggregationInput
): KentuckyOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const sourceUrl = input.sourceUrl ?? null;

  const outsideGroupsByCommitteeKey = new Map<string, KentuckyOutsideSpendingGroup[]>();
  for (const group of input.outsideGroups) {
    const committeeKey = normalizeCommitteeKey(group.committeeKey || group.committeeName);
    if (!committeeKey) {
      continue;
    }
    const existing = outsideGroupsByCommitteeKey.get(committeeKey) ?? [];
    existing.push({ ...group, committeeKey });
    outsideGroupsByCommitteeKey.set(committeeKey, existing);
  }

  if (outsideGroupsByCommitteeKey.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    };
  }

  const donors = new Map<string, DonorAggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;

  for (const record of input.contributionRecords) {
    const committeeKey = recordCommitteeKey(record);
    const matchingGroups = outsideGroupsByCommitteeKey.get(committeeKey) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = amountToCents(record.amount);
    const displayName = record.contributorName?.trim().replace(/\s+/g, " ") ?? "";
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    if (!displayName || !normalizedName || amountCents === null || !isOutsideDonorContribution({ record, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    for (const group of matchingGroups) {
      const key = donorKey({ committeeKey: group.committeeKey, supportOppose: group.supportOppose, normalizedName });
      const existing = donors.get(key);
      if (existing) {
        existing.amountCents += amountCents;
        continue;
      }
      donors.set(key, {
        committeeKey: group.committeeKey,
        supportOppose: group.supportOppose,
        displayName,
        normalizedName,
        amountCents,
      });
    }
  }

  const industries = new Map<string, IndustryAggregate>();
  for (const donor of donors.values()) {
    if (donor.amountCents < minIndustryAmountCents) {
      continue;
    }
    const classification = classifyFinanceLabel({ rawLabel: donor.displayName, labelType: "donor" });
    if (!classification.industrySlug) {
      continue;
    }
    const key = industryKey({
      committeeKey: donor.committeeKey,
      supportOppose: donor.supportOppose,
      industrySlug: classification.industrySlug,
    });
    const existing = industries.get(key);
    if (existing) {
      existing.amountCents += donor.amountCents;
      existing.donorKeys.add(donor.normalizedName);
      continue;
    }
    industries.set(key, {
      committeeKey: donor.committeeKey,
      supportOppose: donor.supportOppose,
      industrySlug: classification.industrySlug,
      amountCents: donor.amountCents,
      donorKeys: new Set([donor.normalizedName]),
    });
  }

  return {
    outsideGroupBreakdowns: toBreakdowns({
      donors: donors.values(),
      industries: industries.values(),
      sourceUrl,
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
