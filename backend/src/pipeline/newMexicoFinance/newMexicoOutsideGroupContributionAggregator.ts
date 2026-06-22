import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import { mapNewMexicoContributorSourceType } from "./newMexicoDirectContributionAggregator.js";
import type { NewMexicoCfisContributionRow } from "./newMexicoCfisArtifactReader.js";
import type {
  NewMexicoFinanceOutsideGroupBreakdownInput,
  NewMexicoFinanceOutsideGroupInput,
} from "./newMexicoFinanceWriter.js";

export type NewMexicoOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly NewMexicoFinanceOutsideGroupInput[];
  contributionRows: readonly NewMexicoCfisContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type NewMexicoOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: NewMexicoFinanceOutsideGroupBreakdownInput[];
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
const OUTSIDE_DONOR_CONTRIBUTION_TYPES = new Set([
  "CONTRIBUTION",
  "CONTRIBUTIONS IN KIND",
  "CONTRIBUTIONS MONETARY",
  "IN KIND CONTRIBUTION",
  "IN KIND CONTRIBUTIONS",
  "MONETARY",
  "MONETARY CONTRIBUTION",
  "MONETARY CONTRIBUTIONS",
]);

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2020 || value > 2100) {
    throw new Error(`Invalid New Mexico outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Mexico outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid New Mexico outside group contribution minIndustryAmount: ${value}`);
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
    .replace(/\b(THE|OF|FOR)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAmountCents(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (normalized.length === 0 || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
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

function parseNewMexicoCfisDateYear(raw: string): number | null {
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
  const year = parseNewMexicoCfisDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function contributorDisplayName(row: NewMexicoCfisContributionRow): string {
  const lastName = row["Last Name"].trim();
  const firstName = row["First Name"].trim();
  const middleName = row["Middle Name"].trim();
  const name = [firstName, middleName, lastName].filter(Boolean).join(" ");
  return name || lastName || firstName;
}

function isOutsideCommitteeContribution(row: NewMexicoCfisContributionRow): boolean {
  const entityType = normalizeTextKey(row["Report Entity Type"]);
  return (
    /\bINDEPENDENT EXPENDITURE\b/.test(entityType) &&
    /\b(PAC|POLITICAL ACTION COMMITTEE|POLITICAL COMMITTEE)\b/.test(entityType)
  );
}

function isOrganizationContributor(row: NewMexicoCfisContributionRow): boolean {
  const sourceType = mapNewMexicoContributorSourceType(row["Contributor Code"]);
  return (
    sourceType === "business_nonprofit_entities" ||
    sourceType === "pac_independent" ||
    sourceType === "party_committee"
  );
}

function isOutsideDonorReceipt(input: { row: NewMexicoCfisContributionRow; electionYear: number }): boolean {
  const amountCents = parseAmountCents(input.row["Transaction Amount"]);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isOutsideCommitteeContribution(input.row) &&
    isCycleYear({ rawDate: input.row["Transaction Date"], electionYear: input.electionYear }) &&
    OUTSIDE_DONOR_CONTRIBUTION_TYPES.has(normalizeTextKey(input.row["Contribution Type"])) &&
    isOrganizationContributor(input.row)
  );
}

function groupKey(input: { committeeId: string; supportOppose: "support" | "oppose" }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}`;
}

function donorKey(input: { committeeId: string; supportOppose: "support" | "oppose"; normalizedName: string }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: { committeeId: string; supportOppose: "support" | "oppose"; industrySlug: string }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): NewMexicoFinanceOutsideGroupBreakdownInput[] {
  const result: NewMexicoFinanceOutsideGroupBreakdownInput[] = [];

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

export function aggregateNewMexicoOutsideGroupContributions(
  input: NewMexicoOutsideGroupContributionAggregationInput
): NewMexicoOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const outsideGroupKeys = new Map<string, NewMexicoFinanceOutsideGroupInput>();
  for (const group of input.outsideGroups) {
    const committeeId = normalizeId(group.committeeId);
    if (committeeId) {
      outsideGroupKeys.set(groupKey({ committeeId, supportOppose: group.supportOppose }), group);
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
    const committeeId = normalizeId(row.OrgID);
    const matchingGroups = [...outsideGroupKeys.values()].filter((group) => normalizeId(group.committeeId) === committeeId);
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(row["Transaction Amount"]);
    const displayName = contributorDisplayName(row);
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
