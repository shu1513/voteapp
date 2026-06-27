import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import {
  getVermontContributionDetails,
  type VermontCampaignFinanceClientOptions,
  type VermontContributionRow,
} from "./vermontCampaignFinanceClient.js";
import type { VermontOutsideSpendingGroup, VermontSupportOppose } from "./vermontOutsideSpendingAggregator.js";

export type VermontFinanceOutsideGroupBreakdown = {
  filerRegistrationGuid: string;
  supportOppose: VermontSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type VermontOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly VermontOutsideSpendingGroup[];
  contributionRows: readonly VermontContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type VermontOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: VermontFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

export type VermontOutsideGroupContributionFetchAndAggregationInput = Omit<
  VermontOutsideGroupContributionAggregationInput,
  "contributionRows"
> & {
  pageSize?: number;
  maxPagesPerGroup?: number;
};

export type VermontOutsideGroupContributionFetchAndAggregationResult =
  VermontOutsideGroupContributionAggregationResult & {
    fetchedContributionRowCount: number;
  };

type DonorAggregate = {
  filerRegistrationGuid: string;
  supportOppose: VermontSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  filerRegistrationGuid: string;
  supportOppose: VermontSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;
const DEFAULT_PAGE_SIZE = 100;
const DEFAULT_MAX_PAGES_PER_GROUP = 20;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Vermont outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Vermont outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function uniqueOutsideGroups(groups: readonly VermontOutsideSpendingGroup[]): VermontOutsideSpendingGroup[] {
  const result = new Map<string, VermontOutsideSpendingGroup>();
  for (const group of groups) {
    const key = groupKey({ filerRegistrationGuid: group.filerRegistrationGuid, supportOppose: group.supportOppose });
    if (!result.has(key)) {
      result.set(key, group);
    }
  }
  return [...result.values()];
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Vermont outside group contribution minIndustryAmount: ${value}`);
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

function isOrganizationContributor(row: VermontContributionRow): boolean {
  const contributorType = normalizeTextKey(`${row.transactionSourceTypeCode ?? ""} ${row.transactionSource ?? ""}`);
  const contributorName = normalizeTextKey(row.sourceName);
  if (!contributorName) {
    return false;
  }
  if (/\b(INDIVIDUAL|PERSON|CANDIDATE|SELF|TIND)\b/.test(contributorType)) {
    return false;
  }
  if (
    /\b(BUSINESS|CORPORATION|COMPANY|COMMITTEE|PAC|ORGANIZATION|ASSOCIATION|NONPROFIT|NON PROFIT|LABOR|UNION|PARTNERSHIP|LLC|L L C|TRUST|TBSN|TPAC)\b/.test(
      contributorType
    )
  ) {
    return true;
  }
  return /\b(INC|LLC|CORP|CORPORATION|COMPANY|ASSOCIATION|PAC|COMMITTEE|UNION|FOUNDATION|FUND|TRUST|LOCAL)\b/.test(
    contributorName
  );
}

function groupKey(input: { filerRegistrationGuid: string; supportOppose: VermontSupportOppose }): string {
  return `${input.filerRegistrationGuid.trim()}\u0000${input.supportOppose}`;
}

function donorKey(input: {
  filerRegistrationGuid: string;
  supportOppose: VermontSupportOppose;
  normalizedName: string;
}): string {
  return `${input.filerRegistrationGuid.trim()}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  filerRegistrationGuid: string;
  supportOppose: VermontSupportOppose;
  industrySlug: string;
}): string {
  return `${input.filerRegistrationGuid.trim()}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): VermontFinanceOutsideGroupBreakdown[] {
  const result: VermontFinanceOutsideGroupBreakdown[] = [];
  const bucketKey = (value: { filerRegistrationGuid: string; supportOppose: VermontSupportOppose }): string =>
    groupKey(value);

  function grouped<T extends { filerRegistrationGuid: string; supportOppose: VermontSupportOppose }>(values: Iterable<T>): T[][] {
    const buckets = new Map<string, T[]>();
    for (const value of values) {
      const key = bucketKey(value);
      const bucket = buckets.get(key) ?? [];
      bucket.push(value);
      buckets.set(key, bucket);
    }
    return [...buckets.values()].sort((left, right) =>
      (bucketKey(left[0] ?? { filerRegistrationGuid: "", supportOppose: "support" })).localeCompare(
        bucketKey(right[0] ?? { filerRegistrationGuid: "", supportOppose: "support" })
      )
    );
  }

  for (const bucket of grouped(input.donors)) {
    for (const donor of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        filerRegistrationGuid: donor.filerRegistrationGuid,
        supportOppose: donor.supportOppose,
        categoryType: "donor",
        categoryName: donor.displayName,
        amount: centsToDollars(donor.amountCents),
        contributorCount: 1,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  for (const bucket of grouped(input.industries)) {
    for (const industry of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        filerRegistrationGuid: industry.filerRegistrationGuid,
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

export function aggregateVermontOutsideGroupContributions(
  input: VermontOutsideGroupContributionAggregationInput
): VermontOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const sourceUrl = input.sourceUrl ?? null;

  const outsideGroupsByFiler = new Map<string, VermontOutsideSpendingGroup[]>();
  for (const group of input.outsideGroups) {
    const filerRegistrationGuid = group.filerRegistrationGuid.trim();
    if (!filerRegistrationGuid) {
      continue;
    }
    const groups = outsideGroupsByFiler.get(filerRegistrationGuid) ?? [];
    groups.push({ ...group, filerRegistrationGuid });
    outsideGroupsByFiler.set(filerRegistrationGuid, groups);
  }

  if (outsideGroupsByFiler.size === 0) {
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
    const filerRegistrationGuid = row.filerRegistrationGuid.trim();
    const matchingGroups = outsideGroupsByFiler.get(filerRegistrationGuid) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const supportOpposeValues = [...new Set(matchingGroups.map((group) => group.supportOppose))];
    const supportOppose = supportOpposeValues.length === 1 ? supportOpposeValues[0] ?? null : null;
    const amountCents = amountToCents(row.transactionAmount);
    const displayName = row.sourceName?.trim().replace(/\s+/g, " ") ?? "";
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    if (
      !supportOppose ||
      !displayName ||
      !normalizedName ||
      amountCents === null ||
      amountCents <= 0 ||
      row.electionYear !== electionYear ||
      !isOrganizationContributor(row)
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    const key = donorKey({ filerRegistrationGuid, supportOppose, normalizedName });
    const existing = donors.get(key);
    if (existing) {
      existing.amountCents += amountCents;
      continue;
    }
    donors.set(key, {
      filerRegistrationGuid,
      supportOppose,
      displayName,
      normalizedName,
      amountCents,
    });
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
      filerRegistrationGuid: donor.filerRegistrationGuid,
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
      filerRegistrationGuid: donor.filerRegistrationGuid,
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
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}

export async function fetchAndAggregateVermontOutsideGroupContributions(
  input: VermontOutsideGroupContributionFetchAndAggregationInput,
  options: VermontCampaignFinanceClientOptions = {}
): Promise<VermontOutsideGroupContributionFetchAndAggregationResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const pageSize = normalizePositiveInteger(input.pageSize, DEFAULT_PAGE_SIZE, "pageSize");
  const maxPagesPerGroup = normalizePositiveInteger(
    input.maxPagesPerGroup,
    DEFAULT_MAX_PAGES_PER_GROUP,
    "maxPagesPerGroup"
  );
  const rowsByGuid = new Map<string, VermontContributionRow>();

  for (const group of uniqueOutsideGroups(input.outsideGroups)) {
    const filerRegistrationGuid = group.filerRegistrationGuid.trim();
    if (!filerRegistrationGuid) {
      continue;
    }

    for (let pageNumber = 1; pageNumber <= maxPagesPerGroup; pageNumber += 1) {
      const page = await getVermontContributionDetails(
        {
          pageNumber,
          pageSize,
          filerRegistrationGuid,
          electionYear,
          transactionTypeCode: "TCON",
        },
        options
      );
      for (const row of page.items) {
        rowsByGuid.set(row.guid || `${row.transactionId}:${row.filerRegistrationGuid}`, row);
      }
      if (page.items.length < pageSize || pageNumber * pageSize >= page.totalItems) {
        break;
      }
    }
  }

  const result = aggregateVermontOutsideGroupContributions({
    ...input,
    electionYear,
    contributionRows: [...rowsByGuid.values()],
  });
  return {
    ...result,
    fetchedContributionRowCount: rowsByGuid.size,
  };
}
