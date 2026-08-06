import { classifyFinanceLabel, normalizeFinanceLabel } from "../finance/financeLabelClassifier.js";

import type { MinnesotaCampaignFinanceCsvRow } from "./minnesotaCampaignFinanceArtifactReader.js";

export type MinnesotaSupportOppose = "support" | "oppose";
export type MinnesotaFinanceOutsideCategoryType = "donor" | "industry";

export type MinnesotaFinanceOutsideGroupInput = {
  committeeId: string;
  committeeName: string;
  supportOppose: MinnesotaSupportOppose;
  amount: number;
  sourceUrl?: string | null;
};

export type MinnesotaFinanceOutsideGroupBreakdownInput = {
  committeeId: string;
  supportOppose: MinnesotaSupportOppose;
  categoryType: MinnesotaFinanceOutsideCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type MinnesotaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly MinnesotaFinanceOutsideGroupInput[];
  contributionRows: readonly MinnesotaCampaignFinanceCsvRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type MinnesotaOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: MinnesotaFinanceOutsideGroupBreakdownInput[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: MinnesotaSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: MinnesotaSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Minnesota outside group contribution ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Minnesota outside group contribution election year: ${value}`);
  }
  return value;
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
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function firstNonEmpty(row: MinnesotaCampaignFinanceCsvRow, keys: readonly string[]): string {
  for (const key of keys) {
    const value = row[key]?.trim() ?? "";
    if (value) {
      return value;
    }
  }
  return "";
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

function parseYearFromText(raw: string): number | null {
  const trimmed = raw.trim();
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
  const yearMatch = /^(\d{4})$/.exec(trimmed);
  if (yearMatch) {
    return Number(yearMatch[1]);
  }
  return null;
}

function isCycleYear(input: { row: MinnesotaCampaignFinanceCsvRow; electionYear: number }): boolean {
  const rawYear = firstNonEmpty(input.row, ["Year", "Election Year", "electionYear", "election_year"]);
  const parsedYear = rawYear
    ? Number(rawYear)
    : parseYearFromText(firstNonEmpty(input.row, ["Receipt date", "Date", "Received date"]));
  return parsedYear !== null && Number.isInteger(parsedYear) && parsedYear >= input.electionYear - 1 && parsedYear <= input.electionYear;
}

function contributorDisplayName(row: MinnesotaCampaignFinanceCsvRow): string {
  return firstNonEmpty(row, ["Contributor", "Contributor Name", "Contributor name", "Contributor full name"]);
}

function contributorType(row: MinnesotaCampaignFinanceCsvRow): string {
  return normalizeTextKey(firstNonEmpty(row, ["Contrib type", "Contributor type", "Contributor Type", "Source type"]));
}

function isOrganizationContributor(row: MinnesotaCampaignFinanceCsvRow): boolean {
  const type = contributorType(row);
  if (!type) {
    return false;
  }
  if (/\b(?:INDIVIDUAL|PERSON|HUMAN|SELF|SELF EMPLOYED|UNEMPLOYED|RETIRED|HOMEMAKER)\b/.test(type)) {
    return false;
  }
  return /\b(?:BUSINESS|NONPROFIT|PAC|POLITICAL|COMMITTEE|UNION|ASSOCIATION|FOUNDATION|COMPANY|CORPORATION|CORP|INC|LLC|LP|LTD|ORG|ORGANIZATION|CLUB|COUNCIL|TRUST|FUND|INSTITUTE|REGISTRANT)\b/.test(
    type
  );
}

function outsideGroupKey(input: { committeeId: string; supportOppose: MinnesotaSupportOppose }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}`;
}

function donorKey(input: {
  committeeId: string;
  supportOppose: MinnesotaSupportOppose;
  normalizedName: string;
}): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  committeeId: string;
  supportOppose: MinnesotaSupportOppose;
  industrySlug: string;
}): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function bucketByGroup<T extends { committeeId: string; supportOppose: MinnesotaSupportOppose }>(
  values: Iterable<T>
): T[][] {
  const buckets = new Map<string, T[]>();
  for (const value of values) {
    const key = outsideGroupKey(value);
    const bucket = buckets.get(key) ?? [];
    bucket.push(value);
    buckets.set(key, bucket);
  }
  return [...buckets.entries()].sort(([left], [right]) => left.localeCompare(right)).map(([, bucket]) => bucket);
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): MinnesotaFinanceOutsideGroupBreakdownInput[] {
  const result: MinnesotaFinanceOutsideGroupBreakdownInput[] = [];

  // Cap donors per (committee, direction) bucket so every outside group keeps
  // its own top donors instead of competing in one global list.
  for (const bucket of bucketByGroup(input.donors)) {
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
        sourceUrl: input.sourceUrl,
      });
    }
  }

  // Industry rows are bounded per bucket by the finance industry slug set and
  // are never capped.
  for (const bucket of bucketByGroup(input.industries)) {
    for (const industry of bucket.sort(
      (left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug)
    )) {
      result.push({
        committeeId: industry.committeeId,
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

export function aggregateMinnesotaOutsideGroupContributions(
  input: MinnesotaOutsideGroupContributionAggregationInput
): MinnesotaOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );

  const outsideGroupKeys = new Map<string, MinnesotaFinanceOutsideGroupInput>();
  for (const group of input.outsideGroups) {
    const committeeId = normalizeId(group.committeeId);
    if (committeeId) {
      outsideGroupKeys.set(outsideGroupKey({ committeeId, supportOppose: group.supportOppose }), group);
    }
  }

  const outsideGroupsByCommitteeId = new Map<string, MinnesotaFinanceOutsideGroupInput[]>();
  for (const group of outsideGroupKeys.values()) {
    const committeeId = normalizeId(group.committeeId);
    const existing = outsideGroupsByCommitteeId.get(committeeId);
    if (existing) {
      existing.push(group);
    } else {
      outsideGroupsByCommitteeId.set(committeeId, [group]);
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

  for (const row of input.contributionRows) {
    const committeeId = normalizeId(
      firstNonEmpty(row, ["Recipient reg num", "Recipient Reg Num", "Recipient ID", "Recipient committee id"])
    );
    const matchingGroups = outsideGroupsByCommitteeId.get(committeeId) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    if (matchingGroups.length > 1) {
      skippedContributionRowCount += 1;
      continue;
    }
    matchedContributionRowCount += 1;

    const displayName = contributorDisplayName(row);
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    const amountCents = parseAmountCents(firstNonEmpty(row, ["Amount", "amount", "Transaction Amount"]));
    if (!displayName || !normalizedName || amountCents === null || amountCents <= 0 || !isCycleYear({ row, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }
    if (!isOrganizationContributor(row)) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    for (const group of matchingGroups) {
      const key = donorKey({ committeeId: group.committeeId, supportOppose: group.supportOppose, normalizedName });
      const existing = donors.get(key);
      if (existing) {
        existing.amountCents += amountCents;
        continue;
      }
      donors.set(key, {
        committeeId: group.committeeId,
        supportOppose: group.supportOppose,
        displayName,
        normalizedName,
        amountCents,
      });
    }
  }

  const industries = new Map<string, IndustryAggregate>();
  for (const donor of donors.values()) {
    const classification = classifyFinanceLabel({
      rawLabel: donor.displayName,
      labelType: "donor",
    });
    if (!classification.industrySlug) {
      continue;
    }
    const key = industryKey({
      committeeId: donor.committeeId,
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
      committeeId: donor.committeeId,
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
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
