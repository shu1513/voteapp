import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import { normalizeMichiganMitnLegacyArchiveYear } from "./michiganMitnLegacyRowTypes.js";
import type { MichiganMitnLegacyContributionRow } from "./michiganMitnLegacyRowTypes.js";
import type { MichiganOutsideSpendingGroup, MichiganSupportOppose } from "./michiganOutsideSpendingAggregator.js";

export type MichiganFinanceOutsideGroupBreakdown = {
  committeeId: string;
  supportOppose: MichiganSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type MichiganOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly MichiganOutsideSpendingGroup[];
  contributionRows: readonly MichiganMitnLegacyContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type MichiganOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: MichiganFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: MichiganSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: MichiganSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Michigan outside group contribution ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Michigan outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
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

function parseMichiganMitnDateYear(raw: string): number | null {
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
  return null;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseMichiganMitnDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isExcludedContributionType(raw: string): boolean {
  const normalized = normalizeTextKey(raw);
  if (!normalized) {
    return false;
  }
  return /\b(?:LOAN|REFUND|REBATE|INTEREST|DIVIDEND|TRANSFER|DISBURSEMENT|EXPENDITURE|DEBT|REPAYMENT|RETURNED|REVERSAL)\b/.test(
    normalized
  );
}

function donorDisplayName(row: MichiganMitnLegacyContributionRow): string {
  return row.l_name_or_org.trim().replace(/\s+/g, " ");
}

function isOrganizationContributor(row: MichiganMitnLegacyContributionRow): boolean {
  if (!donorDisplayName(row) || row.f_name.trim()) {
    return false;
  }
  const contributorType = normalizeTextKey(row.contribtype);
  return !/\b(?:INDIVIDUAL|PERSON|CANDIDATE|SELF)\b/.test(contributorType);
}

function isOutsideDonorReceipt(input: { row: MichiganMitnLegacyContributionRow; electionYear: number }): boolean {
  const amountCents = parseAmountCents(input.row.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    normalizeId(input.row.cfr_com_id).length > 0 &&
    isCycleYear({ rawDate: input.row.received_date, electionYear: input.electionYear }) &&
    !isExcludedContributionType(input.row.contribtype) &&
    isOrganizationContributor(input.row)
  );
}

function groupKey(input: { committeeId: string; supportOppose: MichiganSupportOppose }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}`;
}

function donorKey(input: { committeeId: string; supportOppose: MichiganSupportOppose; normalizedName: string }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: { committeeId: string; supportOppose: MichiganSupportOppose; industrySlug: string }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): MichiganFinanceOutsideGroupBreakdown[] {
  const result: MichiganFinanceOutsideGroupBreakdown[] = [];

  for (const donor of [...input.donors]
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

  for (const industry of [...input.industries]
    .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
    .slice(0, input.maxBreakdownsPerCategory)) {
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

  return result;
}

export function aggregateMichiganOutsideGroupContributions(
  input: MichiganOutsideGroupContributionAggregationInput
): MichiganOutsideGroupContributionAggregationResult {
  const electionYear = normalizeMichiganMitnLegacyArchiveYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);

  const outsideGroupKeys = new Map<string, MichiganOutsideSpendingGroup>();
  for (const group of input.outsideGroups) {
    const committeeId = normalizeId(group.committeeId);
    if (committeeId) {
      outsideGroupKeys.set(groupKey({ committeeId, supportOppose: group.supportOppose }), group);
    }
  }

  const outsideGroupsByCommitteeId = new Map<string, MichiganOutsideSpendingGroup[]>();
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
    const committeeId = normalizeId(row.cfr_com_id);
    const matchingGroups = outsideGroupsByCommitteeId.get(committeeId) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(row.amount);
    const displayName = donorDisplayName(row);
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    if (!displayName || !normalizedName || amountCents === null || !isOutsideDonorReceipt({ row, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    for (const group of matchingGroups) {
      const key = donorKey({ committeeId, supportOppose: group.supportOppose, normalizedName });
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
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
