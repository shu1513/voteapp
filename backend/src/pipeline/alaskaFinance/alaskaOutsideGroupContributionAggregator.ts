import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";
import type { AlaskaApocIndependentContributionRow } from "./alaskaApocClient.js";
import { parseAlaskaApocDateYear } from "./alaskaApocClient.js";
import type { AlaskaOutsideSpendingGroup, AlaskaSupportOppose } from "./alaskaOutsideSpendingAggregator.js";

export type AlaskaFinanceOutsideGroupBreakdown = {
  committeeId: string;
  supportOppose: AlaskaSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type AlaskaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly AlaskaOutsideSpendingGroup[];
  contributionRows: readonly AlaskaApocIndependentContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type AlaskaOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: AlaskaFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: AlaskaSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
  classifications: FinanceLabelClassification[];
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: AlaskaSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Alaska outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Alaska outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Alaska outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
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

function rowYear(row: AlaskaApocIndependentContributionRow): number | null {
  return row.reportYear ?? parseAlaskaApocDateYear(row.date);
}

function isCycleYear(input: { row: AlaskaApocIndependentContributionRow; electionYear: number }): boolean {
  const year = rowYear(input.row);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isFiledStatus(status: string): boolean {
  const key = normalizeTextKey(status);
  return !/\b(REJECTED|VOID|VOIDED|DELETED|WITHDRAWN)\b/.test(key);
}

function committeeId(row: AlaskaApocIndependentContributionRow): string {
  const filerId = row.filerId.trim();
  return filerId || normalizeTextKey(row.filerName);
}

function groupKey(input: { committeeId: string; supportOppose: AlaskaSupportOppose }): string {
  return `${normalizeTextKey(input.committeeId)}\u0000${input.supportOppose}`;
}

function donorKey(input: { committeeId: string; supportOppose: AlaskaSupportOppose; normalizedName: string }): string {
  return `${normalizeTextKey(input.committeeId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  committeeId: string;
  supportOppose: AlaskaSupportOppose;
  industrySlug: string;
}): string {
  return `${normalizeTextKey(input.committeeId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

export function classifyAlaskaOutsideGroupContributionRow(
  row: Pick<AlaskaApocIndependentContributionRow, "contributor" | "employer" | "occupation">
): FinanceLabelClassification {
  const contributor = row.contributor.trim();
  if (contributor) {
    const donorClassification = classifyFinanceLabel({ rawLabel: contributor, labelType: "donor" });
    if (donorClassification.industrySlug) {
      return donorClassification;
    }
  }

  const employer = row.employer.trim();
  if (employer) {
    const employerClassification = classifyFinanceLabel({ rawLabel: employer, labelType: "employer" });
    if (employerClassification.industrySlug) {
      return employerClassification;
    }
  }

  const occupation = row.occupation.trim();
  if (occupation) {
    return classifyFinanceLabel({ rawLabel: occupation, labelType: "occupation" });
  }

  return classifyFinanceLabel({ rawLabel: contributor, labelType: "donor" });
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): AlaskaFinanceOutsideGroupBreakdown[] {
  const result: AlaskaFinanceOutsideGroupBreakdown[] = [];
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

export function aggregateAlaskaOutsideGroupContributions(
  input: AlaskaOutsideGroupContributionAggregationInput
): AlaskaOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const fallbackSourceUrl = input.sourceUrl ?? null;

  const outsideGroupsByCommitteeId = new Map<string, AlaskaOutsideSpendingGroup[]>();
  for (const group of input.outsideGroups) {
    const key = normalizeTextKey(group.committeeId);
    if (!key) {
      continue;
    }
    const existing = outsideGroupsByCommitteeId.get(key);
    if (existing) {
      existing.push(group);
    } else {
      outsideGroupsByCommitteeId.set(key, [group]);
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
    const rowCommitteeId = committeeId(row);
    const matchingGroups = outsideGroupsByCommitteeId.get(normalizeTextKey(rowCommitteeId)) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = amountToCents(row.amount);
    const displayName = row.contributor.trim().replace(/\s+/g, " ");
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !displayName ||
      !normalizedName ||
      !isCycleYear({ row, electionYear }) ||
      !isFiledStatus(row.status)
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    const classification = classifyAlaskaOutsideGroupContributionRow(row);
    for (const group of matchingGroups) {
      const key = donorKey({
        committeeId: group.committeeId,
        supportOppose: group.supportOppose,
        normalizedName,
      });
      const existing = donors.get(key);
      if (existing) {
        existing.amountCents += amountCents;
        existing.classifications.push(classification);
        continue;
      }
      donors.set(key, {
        committeeId: group.committeeId,
        supportOppose: group.supportOppose,
        displayName,
        normalizedName,
        amountCents,
        classifications: [classification],
      });
    }
  }

  const industries = new Map<string, IndustryAggregate>();
  for (const donor of donors.values()) {
    if (donor.amountCents < minIndustryAmountCents) {
      continue;
    }
    const classification = donor.classifications.find((rowClassification) => rowClassification.industrySlug);
    if (!classification?.industrySlug) {
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
      sourceUrl: fallbackSourceUrl,
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
