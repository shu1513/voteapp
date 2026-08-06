import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import type { DistrictOfColumbiaOcfContributionRecord } from "./districtOfColumbiaOcfClient.js";
import type {
  DistrictOfColumbiaOutsideSpendingGroup,
  DistrictOfColumbiaSupportOppose,
} from "./districtOfColumbiaOutsideSpendingAggregator.js";

export type DistrictOfColumbiaFinanceOutsideGroupBreakdown = {
  committeeKey: string;
  supportOppose: DistrictOfColumbiaSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type DistrictOfColumbiaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly DistrictOfColumbiaOutsideSpendingGroup[];
  contributionRecords: readonly DistrictOfColumbiaOcfContributionRecord[];
  sourceUrl?: string | null;
  minIndustryAmount?: number;
};

export type DistrictOfColumbiaOutsideGroupContributionAggregationResult = {
  // ALL donor rows, uncapped (sorted by amount). The sync layer classifies
  // every donor and only caps the PERSISTED donor display rows — capping here
  // would silently drop tail donors from the rebuilt industry totals.
  outsideGroupBreakdowns: DistrictOfColumbiaFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeKey: string;
  supportOppose: DistrictOfColumbiaSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeKey: string;
  supportOppose: DistrictOfColumbiaSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid D.C. outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid D.C. outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
}

function normalizeCommitteeKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
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

function parseDistrictOfColumbiaOcfDateYear(raw: string | undefined): number | null {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  return null;
}

function isCycleRecord(input: { record: DistrictOfColumbiaOcfContributionRecord; electionYear: number }): boolean {
  if (input.record.electionYear !== undefined) {
    return input.record.electionYear === input.electionYear;
  }
  const year = parseDistrictOfColumbiaOcfDateYear(input.record.date);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function recordCommitteeKey(record: DistrictOfColumbiaOcfContributionRecord): string {
  return normalizeCommitteeKey(record.committeeKey ?? record.committeeName ?? "");
}

function isOrganizationContributor(record: DistrictOfColumbiaOcfContributionRecord): boolean {
  const contributorType = normalizeTextKey(record.contributorType);
  const contributorName = normalizeTextKey(record.contributorName);
  if (!contributorName) {
    return false;
  }
  if (/\b(INDIVIDUAL|PERSON|CANDIDATE|SELF)\b/.test(contributorType)) {
    return false;
  }
  if (/\b(BUSINESS|CORPORATION|COMPANY|COMMITTEE|PAC|ORGANIZATION|ASSOCIATION|NONPROFIT|NON PROFIT|LABOR|UNION|PARTNERSHIP|LLC|L L C|TRUST)\b/.test(contributorType)) {
    return true;
  }
  return /\b(INC|LLC|CORP|CORPORATION|COMPANY|ASSOCIATION|PAC|COMMITTEE|UNION|FOUNDATION|FUND|TRUST)\b/.test(
    contributorName
  );
}

function isOutsideDonorReceipt(input: {
  record: DistrictOfColumbiaOcfContributionRecord;
  electionYear: number;
}): boolean {
  const amountCents = amountToCents(input.record.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCycleRecord(input) &&
    isOrganizationContributor(input.record)
  );
}

function groupKey(input: { committeeKey: string; supportOppose: DistrictOfColumbiaSupportOppose }): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}`;
}

function donorKey(input: {
  committeeKey: string;
  supportOppose: DistrictOfColumbiaSupportOppose;
  normalizedName: string;
}): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  committeeKey: string;
  supportOppose: DistrictOfColumbiaSupportOppose;
  industrySlug: string;
}): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
}): DistrictOfColumbiaFinanceOutsideGroupBreakdown[] {
  const result: DistrictOfColumbiaFinanceOutsideGroupBreakdown[] = [];

  for (const donor of [...input.donors]
    .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))) {
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

  for (const industry of [...input.industries]
    .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))) {
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

  return result;
}

export function aggregateDistrictOfColumbiaOutsideGroupContributions(
  input: DistrictOfColumbiaOutsideGroupContributionAggregationInput
): DistrictOfColumbiaOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const outsideGroupKeys = new Map<string, DistrictOfColumbiaOutsideSpendingGroup>();
  for (const group of input.outsideGroups) {
    const committeeKey = normalizeCommitteeKey(group.committeeKey);
    if (committeeKey) {
      outsideGroupKeys.set(groupKey({ committeeKey, supportOppose: group.supportOppose }), {
        ...group,
        committeeKey,
      });
    }
  }
  const outsideGroupsByCommitteeKey = new Map<string, DistrictOfColumbiaOutsideSpendingGroup[]>();
  for (const group of outsideGroupKeys.values()) {
    const committeeKey = normalizeCommitteeKey(group.committeeKey);
    const existing = outsideGroupsByCommitteeKey.get(committeeKey);
    if (existing) {
      existing.push(group);
    } else {
      outsideGroupsByCommitteeKey.set(committeeKey, [group]);
    }
  }

  if (outsideGroupKeys.size === 0) {
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
    if (!displayName || !normalizedName || amountCents === null || !isOutsideDonorReceipt({ record, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    for (const group of matchingGroups) {
      const key = donorKey({ committeeKey, supportOppose: group.supportOppose, normalizedName });
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
    const classification = classifyFinanceLabel({
      rawLabel: donor.displayName,
      labelType: "donor",
    });
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
      sourceUrl: input.sourceUrl ?? null,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
