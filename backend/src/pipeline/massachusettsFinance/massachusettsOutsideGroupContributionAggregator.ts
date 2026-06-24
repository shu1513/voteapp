import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import type { MassachusettsOcpfReceiptItem, MassachusettsOcpfReportDetail } from "./massachusettsOcpfClient.js";
import type { MassachusettsOutsideSpendingGroup, MassachusettsSupportOppose } from "./massachusettsOutsideSpendingAggregator.js";

export type MassachusettsFinanceOutsideGroupBreakdown = {
  iepacCpfId: string;
  supportOppose: MassachusettsSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type MassachusettsOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly MassachusettsOutsideSpendingGroup[];
  reportDetails: readonly MassachusettsOcpfReportDetail[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type MassachusettsOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: MassachusettsFinanceOutsideGroupBreakdown[];
  matchedReceiptRowCount: number;
  includedReceiptRowCount: number;
  skippedReceiptRowCount: number;
};

type DonorAggregate = {
  iepacCpfId: string;
  supportOppose: MassachusettsSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
  sourceUrl: string | null;
};

type IndustryAggregate = {
  iepacCpfId: string;
  supportOppose: MassachusettsSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
  sourceUrl: string | null;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Massachusetts outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Massachusetts outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Massachusetts outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
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

function isElectionYearReceipt(input: { item: MassachusettsOcpfReceiptItem; electionYear: number }): boolean {
  const year = parseMassachusettsOcpfDateYear(input.item.date);
  return year === input.electionYear;
}

function isContributionReceipt(item: MassachusettsOcpfReceiptItem): boolean {
  const recordType = normalizeTextKey(item.recordTypeDescription);
  return !/\b(REFUND|LOAN|REVERSAL)\b/.test(recordType);
}

function isOrganizationContributor(item: MassachusettsOcpfReceiptItem): boolean {
  const contributorName = normalizeTextKey(item.contributorName);
  if (!contributorName) {
    return false;
  }

  const contributorType = normalizeTextKey(item.contributorType ?? item.recordTypeDescription);
  if (/\b(INDIVIDUAL|PERSON|CANDIDATE|SELF)\b/.test(contributorType)) {
    return false;
  }
  if (
    /\b(BUSINESS|CORPORATION|COMPANY|COMMITTEE|PAC|ORGANIZATION|ASSOCIATION|NONPROFIT|NON PROFIT|LABOR|UNION|PARTNERSHIP|LLC|L L C|TRUST)\b/.test(
      contributorType
    )
  ) {
    return true;
  }

  return /\b(INC|LLC|CORP|CORPORATION|COMPANY|ASSOCIATION|PAC|COMMITTEE|UNION|FOUNDATION|FUND|TRUST|LOCAL)\b/.test(
    contributorName
  );
}

function isOutsideDonorReceipt(input: { item: MassachusettsOcpfReceiptItem; electionYear: number }): boolean {
  const amountCents = amountToCents(input.item.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isElectionYearReceipt(input) &&
    isContributionReceipt(input.item) &&
    isOrganizationContributor(input.item)
  );
}

function groupKey(input: { iepacCpfId: string; supportOppose: MassachusettsSupportOppose }): string {
  return `${normalizeCpfId(input.iepacCpfId)}\u0000${input.supportOppose}`;
}

function donorKey(input: {
  iepacCpfId: string;
  supportOppose: MassachusettsSupportOppose;
  normalizedName: string;
}): string {
  return `${normalizeCpfId(input.iepacCpfId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  iepacCpfId: string;
  supportOppose: MassachusettsSupportOppose;
  industrySlug: string;
}): string {
  return `${normalizeCpfId(input.iepacCpfId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function breakdownBucketKey(input: { iepacCpfId: string; supportOppose: MassachusettsSupportOppose }): string {
  return `${normalizeCpfId(input.iepacCpfId)}\u0000${input.supportOppose}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  maxBreakdownsPerCategory: number;
}): MassachusettsFinanceOutsideGroupBreakdown[] {
  const result: MassachusettsFinanceOutsideGroupBreakdown[] = [];
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

  const bucketSortKey = (bucket: Array<{ iepacCpfId: string; supportOppose: MassachusettsSupportOppose }>): string =>
    bucket[0] ? breakdownBucketKey(bucket[0]) : "";

  for (const bucket of [...donorsByBucket.values()].sort((left, right) =>
    bucketSortKey(left).localeCompare(bucketSortKey(right))
  )) {
    for (const donor of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        iepacCpfId: donor.iepacCpfId,
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
    bucketSortKey(left).localeCompare(bucketSortKey(right))
  )) {
    for (const industry of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        iepacCpfId: industry.iepacCpfId,
        supportOppose: industry.supportOppose,
        categoryType: "industry",
        categoryName: industry.industrySlug,
        amount: centsToDollars(industry.amountCents),
        contributorCount: industry.donorKeys.size,
        sourceUrl: industry.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateMassachusettsOutsideGroupContributions(
  input: MassachusettsOutsideGroupContributionAggregationInput
): MassachusettsOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const sourceUrl = input.sourceUrl ?? null;

  const outsideGroupsByCpfId = new Map<string, MassachusettsOutsideSpendingGroup[]>();
  for (const group of input.outsideGroups) {
    const iepacCpfId = normalizeCpfId(group.iepacCpfId);
    if (!iepacCpfId) {
      continue;
    }
    const existing = outsideGroupsByCpfId.get(iepacCpfId) ?? [];
    existing.push({ ...group, iepacCpfId });
    outsideGroupsByCpfId.set(iepacCpfId, existing);
  }

  if (outsideGroupsByCpfId.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      matchedReceiptRowCount: 0,
      includedReceiptRowCount: 0,
      skippedReceiptRowCount: 0,
    };
  }

  const donors = new Map<string, DonorAggregate>();
  let matchedReceiptRowCount = 0;
  let includedReceiptRowCount = 0;
  let skippedReceiptRowCount = 0;

  for (const report of input.reportDetails) {
    const iepacCpfId = normalizeCpfId(report.cpfId);
    const matchingGroups = outsideGroupsByCpfId.get(iepacCpfId) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    const supportOpposeValues = [...new Set(matchingGroups.map((group) => group.supportOppose))];
    const supportOppose = supportOpposeValues.length === 1 ? supportOpposeValues[0] ?? null : null;

    for (const item of report.receipts) {
      matchedReceiptRowCount += 1;
      const amountCents = amountToCents(item.amount);
      const displayName = item.contributorName?.trim().replace(/\s+/g, " ") ?? "";
      const normalizedName = normalizeFinanceLabel(displayName, "donor");
      if (
        !supportOppose ||
        !displayName ||
        !normalizedName ||
        amountCents === null ||
        !isOutsideDonorReceipt({ item, electionYear })
      ) {
        skippedReceiptRowCount += 1;
        continue;
      }

      includedReceiptRowCount += 1;
      const group = matchingGroups.find((candidateGroup) => candidateGroup.supportOppose === supportOppose);
      if (!group) {
        skippedReceiptRowCount += 1;
        includedReceiptRowCount -= 1;
        continue;
      }
      const key = donorKey({ iepacCpfId, supportOppose, normalizedName });
      const existing = donors.get(key);
      if (existing) {
        existing.amountCents += amountCents;
        continue;
      }
      donors.set(key, {
        iepacCpfId: group.iepacCpfId,
        supportOppose,
        displayName,
        normalizedName,
        amountCents,
        sourceUrl: item.sourceUrl ?? report.sourceUrl ?? group.sourceUrl ?? sourceUrl,
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
      iepacCpfId: donor.iepacCpfId,
      supportOppose: donor.supportOppose,
      industrySlug: classification.industrySlug,
    });
    const existing = industries.get(key);
    if (existing) {
      existing.amountCents += donor.amountCents;
      existing.donorKeys.add(donor.normalizedName);
      existing.sourceUrl = existing.sourceUrl ?? donor.sourceUrl;
      continue;
    }
    industries.set(key, {
      iepacCpfId: donor.iepacCpfId,
      supportOppose: donor.supportOppose,
      industrySlug: classification.industrySlug,
      amountCents: donor.amountCents,
      donorKeys: new Set([donor.normalizedName]),
      sourceUrl: donor.sourceUrl,
    });
  }

  return {
    outsideGroupBreakdowns: toBreakdowns({
      donors: donors.values(),
      industries: industries.values(),
      maxBreakdownsPerCategory,
    }),
    matchedReceiptRowCount,
    includedReceiptRowCount,
    skippedReceiptRowCount,
  };
}
