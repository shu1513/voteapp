import { classifyFinanceLabel, normalizeFinanceLabel } from "../finance/financeLabelClassifier.js";
import type { OklahomaGuardianContributionRow } from "./oklahomaGuardianContributionReader.js";
import type {
  OklahomaFinanceOutsideGroupBreakdownInput,
  OklahomaFinanceOutsideGroupInput,
} from "./oklahomaFinanceWriter.js";

export type OklahomaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly OklahomaFinanceOutsideGroupInput[];
  contributionRows: readonly OklahomaGuardianContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type OklahomaOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: OklahomaFinanceOutsideGroupBreakdownInput[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: "support" | "oppose";
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: "support" | "oppose";
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

const OUTSIDE_DONOR_RECEIPT_TYPES = new Set([
  "CONTRIBUTION",
  "IN KIND",
  "IN KIND CONTRIBUTION",
  "MONETARY",
  "MONETARY CONTRIBUTION",
]);

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2014 || value > 2100) {
    throw new Error(`Invalid Oklahoma outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Oklahoma outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Oklahoma outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
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

export function normalizeOklahomaOutsideGroupCommitteeNameKey(value: string): string {
  return normalizeTextKey(value);
}

function parseAmountCents(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (normalized.length === 0 || !/^-?\d+(?:\.\d{1,2})?$/.test(normalized)) {
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

function parseOklahomaGuardianDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  return isoMatch ? Number(isoMatch[1]) : null;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseOklahomaGuardianDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isOrganizationContributor(row: OklahomaGuardianContributionRow): boolean {
  const sourceType = normalizeTextKey(row["Receipt Source Type"]);
  if (!sourceType || /\b(INDIVIDUAL|CANDIDATE|SELF)\b/.test(sourceType)) {
    return false;
  }
  return /\b(BUSINESS|CORPORATION|COMPANY|LLC|ORGANIZATION|ASSOCIATION|PAC|COMMITTEE|PARTY|UNION|LABOR|CLUB|TRUST)\b/.test(
    sourceType
  );
}

function isOutsideDonorReceipt(input: { row: OklahomaGuardianContributionRow; electionYear: number }): boolean {
  const amountCents = parseAmountCents(input.row["Receipt Amount"]);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCycleYear({ rawDate: input.row["Receipt Date"], electionYear: input.electionYear }) &&
    OUTSIDE_DONOR_RECEIPT_TYPES.has(normalizeTextKey(input.row["Receipt Type"])) &&
    isOrganizationContributor(input.row)
  );
}

function committeeNameKey(value: string): string {
  return normalizeOklahomaOutsideGroupCommitteeNameKey(value);
}

function donorDisplayName(row: OklahomaGuardianContributionRow): string {
  const lastName = row["Last Name"].trim();
  const firstName = row["First Name"].trim();
  const middleName = row["Middle Name"].trim();
  if (firstName || middleName) {
    return [firstName, middleName, lastName].filter(Boolean).join(" ");
  }
  return lastName || row.Employer.trim() || row.Description.trim();
}

function donorKey(input: { committeeId: string; supportOppose: "support" | "oppose"; normalizedName: string }): string {
  return `${input.committeeId.trim().toUpperCase()}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: { committeeId: string; supportOppose: "support" | "oppose"; industrySlug: string }): string {
  return `${input.committeeId.trim().toUpperCase()}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): OklahomaFinanceOutsideGroupBreakdownInput[] {
  const result: OklahomaFinanceOutsideGroupBreakdownInput[] = [];

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

export function aggregateOklahomaOutsideGroupContributions(
  input: OklahomaOutsideGroupContributionAggregationInput
): OklahomaOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const groupsByCommitteeName = new Map<string, OklahomaFinanceOutsideGroupInput[]>();
  for (const group of input.outsideGroups) {
    const key = committeeNameKey(group.committeeName);
    if (!key) {
      continue;
    }
    const existing = groupsByCommitteeName.get(key) ?? [];
    existing.push(group);
    groupsByCommitteeName.set(key, existing);
  }

  if (groupsByCommitteeName.size === 0) {
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
    const matchingGroups = groupsByCommitteeName.get(committeeNameKey(row["Committee Name"])) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(row["Receipt Amount"]);
    const displayName = donorDisplayName(row);
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    if (!displayName || !normalizedName || amountCents === null || !isOutsideDonorReceipt({ row, electionYear })) {
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
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
