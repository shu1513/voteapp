import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import { parseMaineCfisMoney, type MaineCfisContributionRow } from "./maineCfisArtifactReader.js";
import type { MaineOutsideSpendingGroup, MaineSupportOppose } from "./maineOutsideSpendingAggregator.js";

export type MaineFinanceOutsideGroupBreakdown = {
  committeeId: string;
  supportOppose: MaineSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type MaineOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly MaineOutsideSpendingGroup[];
  contributionRows: readonly MaineCfisContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type MaineOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: MaineFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: MaineSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: MaineSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

const OUTSIDE_DONOR_RECEIPT_TYPES = new Set([
  "CONTRIBUTION",
  "CONTRIBUTIONS",
  "IN KIND",
  "IN KIND CONTRIBUTION",
  "IN KIND CONTRIBUTIONS",
  "MONETARY",
  "MONETARY ITEMIZED",
  "MONETARY UNITEMIZED",
  "MONETARY CONTRIBUTION",
  "MONETARY CONTRIBUTIONS",
]);

const ORGANIZATION_SOURCE_TYPE_WORDS = [
  "ASSOCIATION",
  "BUSINESS",
  "COMMITTEE",
  "COMPANY",
  "CORPORATION",
  "ENTITY",
  "LABOR",
  "LLC",
  "NONPROFIT",
  "ORGANIZATION",
  "PAC",
  "PARTY",
  "POLITICAL",
  "UNION",
] as const;

const ORGANIZATION_NAME_PATTERN =
  /\b(ASSOCIATION|BUILDERS?|BUSINESS|CO|COMPANY|CORP|CORPORATION|FOUNDATION|GROUP|INC|INCORPORATED|INDUSTRIES|LABOR|LLC|L L C|LLP|L L P|LOCAL|PAC|PARTNERS|PARTNERSHIP|REALTY|REALTORS?|UNION)\b/i;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Maine outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Maine outside group contribution ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Maine outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
}

function normalizeId(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAmountCents(raw: string): number | null {
  const amount = parseMaineCfisMoney(raw);
  if (amount === null || !Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseMaineCfisDateYear(raw: string): number | null {
  const trimmed = raw.trim();
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

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseMaineCfisDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isOrganizationReceiptSourceType(value: string): boolean {
  const normalized = normalizeTextKey(value);
  if (!normalized || normalized === "INDIVIDUAL") {
    return false;
  }
  return ORGANIZATION_SOURCE_TYPE_WORDS.some((word) => normalized.includes(word));
}

function organizationContributorDisplayName(row: MaineCfisContributionRow): string | null {
  const lastName = row["Last Name"].trim();
  const firstName = row["First Name"].trim();
  const middleName = row["Middle Name"].trim();
  if (firstName || middleName || !lastName) {
    return null;
  }
  if (!isOrganizationReceiptSourceType(row["Receipt Source Type"]) && !ORGANIZATION_NAME_PATTERN.test(lastName)) {
    return null;
  }
  return lastName;
}

function isOutsideCommitteeContribution(row: MaineCfisContributionRow): boolean {
  const committeeType = normalizeTextKey(row["Committee Type"]);
  return (
    /\b(PAC|POLITICAL ACTION COMMITTEE|POLITICAL COMMITTEE|BALLOT QUESTION COMMITTEE)\b/.test(committeeType) &&
    committeeType !== "CANDIDATE" &&
    committeeType !== "CANDIDATE COMMITTEE"
  );
}

function isOutsideDonorReceipt(input: { row: MaineCfisContributionRow; electionYear: number }): boolean {
  const amountCents = parseAmountCents(input.row["Receipt Amount"]);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isOutsideCommitteeContribution(input.row) &&
    isCycleYear({ rawDate: input.row["Receipt Date"], electionYear: input.electionYear }) &&
    OUTSIDE_DONOR_RECEIPT_TYPES.has(normalizeTextKey(input.row["Receipt Type"])) &&
    organizationContributorDisplayName(input.row) !== null
  );
}

function groupKey(input: { committeeId: string; supportOppose: MaineSupportOppose }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}`;
}

function donorKey(input: { committeeId: string; supportOppose: MaineSupportOppose; normalizedName: string }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: { committeeId: string; supportOppose: MaineSupportOppose; industrySlug: string }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): MaineFinanceOutsideGroupBreakdown[] {
  const result: MaineFinanceOutsideGroupBreakdown[] = [];
  const donorGroups = new Map<string, DonorAggregate[]>();
  const industryGroups = new Map<string, IndustryAggregate[]>();

  for (const donor of input.donors) {
    const key = groupKey(donor);
    const list = donorGroups.get(key) ?? [];
    list.push(donor);
    donorGroups.set(key, list);
  }
  for (const industry of input.industries) {
    const key = groupKey(industry);
    const list = industryGroups.get(key) ?? [];
    list.push(industry);
    industryGroups.set(key, list);
  }

  for (const donors of donorGroups.values()) {
    for (const donor of donors
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

  for (const industries of industryGroups.values()) {
    for (const industry of industries
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
  }

  return result;
}

export function aggregateMaineOutsideGroupContributions(
  input: MaineOutsideGroupContributionAggregationInput
): MaineOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const outsideGroupsByCommitteeId = new Map<string, MaineOutsideSpendingGroup[]>();
  for (const group of input.outsideGroups) {
    const committeeId = normalizeId(group.committeeId);
    if (!committeeId) {
      continue;
    }
    const existing = outsideGroupsByCommitteeId.get(committeeId);
    if (existing) {
      existing.push(group);
    } else {
      outsideGroupsByCommitteeId.set(committeeId, [group]);
    }
  }

  if (outsideGroupsByCommitteeId.size === 0) {
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
    const committeeId = normalizeId(row.OrgID);
    const matchingGroups = outsideGroupsByCommitteeId.get(committeeId) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(row["Receipt Amount"]);
    const displayName = organizationContributorDisplayName(row);
    const normalizedName = displayName ? normalizeFinanceLabel(displayName, "donor") : "";
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
