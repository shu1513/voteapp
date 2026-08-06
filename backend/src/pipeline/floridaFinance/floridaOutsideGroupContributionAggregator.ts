import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import {
  centsToFloridaDollars,
  floridaElectionCycleStartYear,
  normalizeFloridaDisplayText,
  normalizeFloridaTextKey,
  parseFloridaAmountCents,
  parseFloridaDateYear,
  type FloridaContributionRow,
} from "./floridaCampaignFinanceRows.js";

export type FloridaSupportOppose = "support" | "oppose";

export type FloridaOutsideFinanceGroup = {
  committeeId: string;
  committeeName: string;
  supportOppose: FloridaSupportOppose;
  amount: number;
  sourceUrl?: string | null;
  committeeNames?: readonly string[];
};

export type FloridaFinanceOutsideGroupBreakdown = {
  committeeId: string;
  supportOppose: FloridaSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type FloridaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly FloridaOutsideFinanceGroup[];
  contributionRows: readonly FloridaContributionRow[];
  sourceUrl?: string | null;
  minIndustryAmount?: number;
};

export type FloridaOutsideGroupContributionAggregationResult = {
  // ALL donor rows, uncapped (sorted by amount within each group). The sync
  // layer classifies every donor and only caps the PERSISTED donor display
  // rows — capping here would silently drop tail donors from the rebuilt
  // industry totals of a >cap-donor group.
  outsideGroupBreakdowns: FloridaFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: FloridaSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: FloridaSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1996 || value > 2100) {
    throw new Error(`Invalid Florida outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Florida outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
}

function normalizeCommitteeId(value: string): string {
  return value.trim().toUpperCase();
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseFloridaDateYear(input.rawDate);
  return year !== null && year >= floridaElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
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

function isOutsideGroupDonorReceipt(input: { row: FloridaContributionRow; electionYear: number }): boolean {
  const amountCents = parseFloridaAmountCents(input.row.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    normalizeFloridaDisplayText(input.row.contributorName).length > 0 &&
    !isExcludedTransactionType(input.row) &&
    isCycleYear({ rawDate: input.row.contributionDate, electionYear: input.electionYear })
  );
}

function groupKey(input: { committeeId: string; supportOppose: FloridaSupportOppose }): string {
  return `${normalizeCommitteeId(input.committeeId)}\u0000${input.supportOppose}`;
}

function donorKey(input: { committeeId: string; supportOppose: FloridaSupportOppose; normalizedName: string }): string {
  return `${normalizeCommitteeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: { committeeId: string; supportOppose: FloridaSupportOppose; industrySlug: string }): string {
  return `${normalizeCommitteeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function groupBreakdownAggregates<T extends { committeeId: string; supportOppose: FloridaSupportOppose }>(
  values: Iterable<T>
): T[][] {
  const grouped = new Map<string, T[]>();
  for (const value of values) {
    const key = groupKey(value);
    const list = grouped.get(key) ?? [];
    list.push(value);
    grouped.set(key, list);
  }
  return [...grouped.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, list]) => list);
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
}): FloridaFinanceOutsideGroupBreakdown[] {
  const result: FloridaFinanceOutsideGroupBreakdown[] = [];

  for (const donors of groupBreakdownAggregates(input.donors)) {
    for (const donor of donors
      .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))) {
      result.push({
        committeeId: donor.committeeId,
        supportOppose: donor.supportOppose,
        categoryType: "donor",
        categoryName: donor.displayName,
        amount: centsToFloridaDollars(donor.amountCents),
        contributorCount: 1,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  for (const industries of groupBreakdownAggregates(input.industries)) {
    for (const industry of industries
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))) {
      result.push({
        committeeId: industry.committeeId,
        supportOppose: industry.supportOppose,
        categoryType: "industry",
        categoryName: industry.industrySlug,
        amount: centsToFloridaDollars(industry.amountCents),
        contributorCount: industry.donorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateFloridaOutsideGroupContributions(
  input: FloridaOutsideGroupContributionAggregationInput
): FloridaOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const outsideGroups = new Map<string, FloridaOutsideFinanceGroup>();
  for (const group of input.outsideGroups) {
    const committeeId = normalizeCommitteeId(requireNonEmpty(group.committeeId, "Florida outside group committee id"));
    requireNonEmpty(group.committeeName, "Florida outside group committee name");
    outsideGroups.set(groupKey({ committeeId, supportOppose: group.supportOppose }), {
      ...group,
      committeeId,
    });
  }

  const groupsByRecipientName = new Map<string, FloridaOutsideFinanceGroup[]>();
  for (const group of outsideGroups.values()) {
    for (const name of [group.committeeName, ...(group.committeeNames ?? [])]) {
      const normalized = normalizeFloridaTextKey(name);
      if (!normalized) {
        continue;
      }
      const existing = groupsByRecipientName.get(normalized);
      if (existing) {
        existing.push(group);
      } else {
        groupsByRecipientName.set(normalized, [group]);
      }
    }
  }

  if (outsideGroups.size === 0) {
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
    const matchingGroups = groupsByRecipientName.get(normalizeFloridaTextKey(row.recipientName)) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseFloridaAmountCents(row.amount);
    const displayName = normalizeFloridaDisplayText(row.contributorName);
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    if (!displayName || !normalizedName || amountCents === null || !isOutsideGroupDonorReceipt({ row, electionYear })) {
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
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
