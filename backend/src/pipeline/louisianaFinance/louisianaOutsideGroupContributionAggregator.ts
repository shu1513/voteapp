import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";

import type { LouisianaCampaignFinanceCsvRow } from "./louisianaCampaignFinanceArtifactReader.js";
import type { LouisianaOutsideSupportGroup, LouisianaSupportOppose } from "./louisianaOutsideSupportAggregator.js";

export type LouisianaFinanceOutsideGroupBreakdown = {
  filerNumber: string;
  supportOppose: LouisianaSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type LouisianaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly LouisianaOutsideSupportGroup[];
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type LouisianaOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: LouisianaFinanceOutsideGroupBreakdown[];
  classifications: FinanceLabelClassification[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  filerNumber: string;
  supportOppose: LouisianaSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
  industrySlug: string | null;
};

type IndustryAggregate = {
  filerNumber: string;
  supportOppose: LouisianaSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Louisiana outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Louisiana outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
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

function firstNonEmpty(row: LouisianaCampaignFinanceCsvRow, keys: readonly string[]): string {
  for (const key of keys) {
    const value = row[key]?.trim() ?? "";
    if (value) {
      return value;
    }
  }
  return "";
}

function normalizeFilerNumber(value: string): string {
  return value.trim().replace(/\s+/g, "");
}

function parseAmountCents(raw: string): number | null {
  const trimmed = raw.trim();
  const isParentheticalNegative = /^\(.+\)$/.test(trimmed);
  const normalized = trimmed.replace(/[,$()]/g, "");
  if (!normalized || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
    return null;
  }
  const amount = Number(normalized) * (isParentheticalNegative ? -1 : 1);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseYearFromDate(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const isoMatch = /^(\d{4})-\d{1,2}-\d{1,2}\b/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number.parseInt(isoMatch[1], 10);
  }
  const slashMatch = /^\d{1,2}\/\d{1,2}\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[1]) {
    return Number.parseInt(slashMatch[1], 10);
  }
  return null;
}

function isElectionCycleDate(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseYearFromDate(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function contributorDisplayName(row: LouisianaCampaignFinanceCsvRow): string {
  return firstNonEmpty(row, ["ContributorName", "Contributor Name"]);
}

function isOrganizationContributor(row: LouisianaCampaignFinanceCsvRow): boolean {
  const type = normalizeTextKey(firstNonEmpty(row, ["ContributorTypeCode", "Contributor Type Code", "ContributorType"]));
  const name = normalizeTextKey(contributorDisplayName(row));
  if (!name) {
    return false;
  }
  if (/\b(IND|INDIVIDUAL|PERSON|CANDIDATE|SELF)\b/.test(type)) {
    return false;
  }
  if (/\b(PAC|POLITICAL|COMMITTEE|BUSINESS|CORP|CORPORATION|COMPANY|ORGANIZATION|ORG|UNION|ASSOCIATION|LLC|L L C|PARTNERSHIP|TRUST|FOUNDATION)\b/.test(type)) {
    return true;
  }
  return /\b(INC|LLC|CORP|CORPORATION|COMPANY|ASSOCIATION|PAC|COMMITTEE|UNION|FOUNDATION|FUND|TRUST|LOCAL)\b/.test(
    name
  );
}

function outsideGroupKey(input: { filerNumber: string; supportOppose: LouisianaSupportOppose }): string {
  return `${normalizeFilerNumber(input.filerNumber)}\u0000${input.supportOppose}`;
}

function donorKey(input: {
  filerNumber: string;
  supportOppose: LouisianaSupportOppose;
  normalizedName: string;
}): string {
  return `${normalizeFilerNumber(input.filerNumber)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  filerNumber: string;
  supportOppose: LouisianaSupportOppose;
  industrySlug: string;
}): string {
  return `${normalizeFilerNumber(input.filerNumber)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): LouisianaFinanceOutsideGroupBreakdown[] {
  const result: LouisianaFinanceOutsideGroupBreakdown[] = [];
  const sortedDonors = [...input.donors].sort(
    (left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName)
  );
  const selectedIndustries = [...input.industries]
    .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
    .slice(0, input.maxBreakdownsPerCategory);
  const selectedDonorKeys = new Set<string>();
  const selectedDonors: DonorAggregate[] = [];

  function selectDonor(donor: DonorAggregate): void {
    const key = donorKey({
      filerNumber: donor.filerNumber,
      supportOppose: donor.supportOppose,
      normalizedName: donor.normalizedName,
    });
    if (selectedDonorKeys.has(key)) {
      return;
    }
    selectedDonorKeys.add(key);
    selectedDonors.push(donor);
  }

  for (const donor of sortedDonors.slice(0, input.maxBreakdownsPerCategory)) {
    selectDonor(donor);
  }

  for (const industry of selectedIndustries) {
    for (const donor of sortedDonors
      .filter(
        (item) =>
          item.filerNumber === industry.filerNumber &&
          item.supportOppose === industry.supportOppose &&
          item.industrySlug === industry.industrySlug
      )
      .slice(0, 3)) {
      selectDonor(donor);
    }
  }

  for (const donor of selectedDonors.sort(
    (left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName)
  )) {
    result.push({
      filerNumber: donor.filerNumber,
      supportOppose: donor.supportOppose,
      categoryType: "donor",
      categoryName: donor.displayName,
      amount: centsToDollars(donor.amountCents),
      contributorCount: 1,
      sourceUrl: input.sourceUrl,
    });
  }

  for (const industry of selectedIndustries) {
    result.push({
      filerNumber: industry.filerNumber,
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

export function aggregateLouisianaOutsideGroupContributions(
  input: LouisianaOutsideGroupContributionAggregationInput
): LouisianaOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;
  const outsideGroupsByFilerNumber = new Map<string, LouisianaOutsideSupportGroup[]>();
  for (const group of input.outsideGroups) {
    const filerNumber = normalizeFilerNumber(group.filerNumber);
    if (!filerNumber) {
      continue;
    }
    const groups = outsideGroupsByFilerNumber.get(filerNumber) ?? [];
    groups.push({ ...group, filerNumber });
    outsideGroupsByFilerNumber.set(filerNumber, groups);
  }

  if (outsideGroupsByFilerNumber.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      classifications: [],
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
    const filerNumber = normalizeFilerNumber(firstNonEmpty(row, ["FilerNumber", "Filer Number"]));
    const matchingGroups = outsideGroupsByFilerNumber.get(filerNumber) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const supportOpposeValues = [...new Set(matchingGroups.map((group) => group.supportOppose))];
    const supportOppose = supportOpposeValues.length === 1 ? supportOpposeValues[0] ?? null : null;
    const displayName = contributorDisplayName(row);
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    const amountCents = parseAmountCents(firstNonEmpty(row, ["ContributionAmt", "Contribution Amount", "Amount"]));
    if (
      !supportOppose ||
      !displayName ||
      !normalizedName ||
      amountCents === null ||
      amountCents <= 0 ||
      !isElectionCycleDate({
        rawDate: firstNonEmpty(row, ["ContributionDate", "Contribution Date", "ReceiptDate", "Receipt Date"]),
        electionYear,
      }) ||
      !isOrganizationContributor(row)
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    const key = donorKey({ filerNumber, supportOppose, normalizedName });
    const existing = donors.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      continue;
    }
    donors.set(key, {
      filerNumber,
      supportOppose,
      displayName,
      normalizedName,
      amountCents,
      industrySlug: null,
    });
  }

  const industries = new Map<string, IndustryAggregate>();
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const donor of donors.values()) {
    const classification = classifyFinanceLabel({
      rawLabel: donor.displayName,
      labelType: "donor",
    });
    donor.industrySlug = classification.industrySlug ?? null;
    classifications.set(`${classification.labelType}\u0000${classification.normalizedLabel}`, classification);
    if (!classification.industrySlug) {
      continue;
    }
    const key = industryKey({
      filerNumber: donor.filerNumber,
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
      filerNumber: donor.filerNumber,
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
    classifications: [...classifications.values()],
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
