import { classifyFinanceLabel, normalizeFinanceLabel } from "../finance/financeLabelClassifier.js";
import type { TennesseeCampContributionRecord } from "./tennesseeCampClient.js";
import type { TennesseeOutsideSpendingGroup, TennesseeSupportOppose } from "./tennesseeOutsideSpendingAggregator.js";

export type TennesseeFinanceOutsideGroupBreakdown = {
  committeeKey: string;
  supportOppose: TennesseeSupportOppose;
  categoryType: "donor" | "employer" | "occupation" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type TennesseeOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly TennesseeOutsideSpendingGroup[];
  contributionRecords: readonly TennesseeCampContributionRecord[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type TennesseeOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: TennesseeFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type LabelCategoryType = "donor" | "employer" | "occupation";

type LabelAggregate = {
  committeeKey: string;
  supportOppose: TennesseeSupportOppose;
  categoryType: LabelCategoryType;
  displayName: string;
  normalizedName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

type IndustryAggregate = {
  committeeKey: string;
  supportOppose: TennesseeSupportOppose;
  industrySlug: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Tennessee outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Tennessee outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Tennessee outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
}

function normalizeCommitteeKey(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ").toUpperCase();
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

function cleanDisplayLabel(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ");
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

function parseDateYear(raw: string | null | undefined): number | null {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch?.[3]) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch?.[1]) {
    return Number(isoMatch[1]);
  }
  return null;
}

function isCycleRecord(input: { record: TennesseeCampContributionRecord; electionYear: number }): boolean {
  const year = parseDateYear(input.record.date) ?? input.record.electionYear;
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isOrganizationContributor(record: TennesseeCampContributionRecord): boolean {
  const rawContributorName = record.contributorName?.trim() ?? "";
  const contributorName = normalizeTextKey(record.contributorName);
  if (!contributorName) {
    return false;
  }

  if (rawContributorName.includes(",") && !/\b(INC|LLC|CORP|COMPANY|PAC|COMMITTEE|ASSOCIATION|UNION)\b/.test(contributorName)) {
    return false;
  }

  return /\b(INC|INCORPORATED|LLC|L L C|LP|L P|LLP|L L P|LTD|LIMITED|CO|COMPANY|CORP|CORPORATION|PLC|PAC|COMMITTEE|ASSOCIATION|UNION|FOUNDATION|FUND|TRUST|PARTNERS|PARTNERSHIP|BANK|GROUP|COALITION|CLUB|PARTY|LOCAL|ENTERPRISES|INDUSTRIES|HOLDINGS?)\b/.test(
    contributorName
  );
}

function isOutsideReceiptBase(input: { record: TennesseeCampContributionRecord; electionYear: number }): boolean {
  const amountCents = amountToCents(input.record.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    normalizeTextKey(input.record.type) === "MONETARY" &&
    normalizeTextKey(input.record.adjustment) !== "Y" &&
    isCycleRecord(input)
  );
}

function groupKey(input: { committeeKey: string; supportOppose: TennesseeSupportOppose }): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}`;
}

function labelKey(input: {
  committeeKey: string;
  supportOppose: TennesseeSupportOppose;
  categoryType: LabelCategoryType;
  normalizedName: string;
}): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}\u0000${input.categoryType}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  committeeKey: string;
  supportOppose: TennesseeSupportOppose;
  industrySlug: string;
}): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function breakdownBucketKey(input: { committeeKey: string; supportOppose: TennesseeSupportOppose }): string {
  return `${normalizeCommitteeKey(input.committeeKey)}\u0000${input.supportOppose}`;
}

function isUsableIndividualLabel(value: string | null | undefined): boolean {
  const normalized = normalizeTextKey(value);
  return (
    normalized.length > 0 &&
    !/^(NONE|N A|NA|NOT APPLICABLE|UNKNOWN|UNEMPLOYED|RETIRED|SELF|SELF EMPLOYED|HOMEMAKER|STUDENT)$/.test(
      normalized
    )
  );
}

function contributorKey(record: TennesseeCampContributionRecord): string {
  const parts = [
    normalizeTextKey(record.contributorName),
    normalizeTextKey(record.contributorEmployer),
    normalizeTextKey(record.contributorOccupation),
  ].filter(Boolean);
  return parts.join("\u0000") || "UNKNOWN";
}

function labelTypeForClassification(categoryType: LabelCategoryType): "donor" | "employer" | null {
  if (categoryType === "donor" || categoryType === "employer") {
    return categoryType;
  }
  return null;
}

function addLabelAggregate(input: {
  labels: Map<string, LabelAggregate>;
  group: TennesseeOutsideSpendingGroup;
  committeeKey: string;
  categoryType: LabelCategoryType;
  displayName: string;
  normalizedName: string;
  amountCents: number;
  contributorKey: string;
}): void {
  const key = labelKey({
    committeeKey: input.committeeKey,
    supportOppose: input.group.supportOppose,
    categoryType: input.categoryType,
    normalizedName: input.normalizedName,
  });
  const existing = input.labels.get(key);
  if (existing) {
    existing.amountCents += input.amountCents;
    existing.contributorKeys.add(input.contributorKey);
    return;
  }
  input.labels.set(key, {
    committeeKey: input.group.committeeKey,
    supportOppose: input.group.supportOppose,
    categoryType: input.categoryType,
    displayName: input.displayName,
    normalizedName: input.normalizedName,
    amountCents: input.amountCents,
    contributorKeys: new Set([input.contributorKey]),
  });
}

function toBreakdowns(input: {
  labels: Iterable<LabelAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): TennesseeFinanceOutsideGroupBreakdown[] {
  const result: TennesseeFinanceOutsideGroupBreakdown[] = [];
  const labelsByBucket = new Map<string, LabelAggregate[]>();
  const industriesByBucket = new Map<string, IndustryAggregate[]>();

  for (const label of input.labels) {
    const key = breakdownBucketKey(label);
    const bucket = labelsByBucket.get(key) ?? [];
    bucket.push(label);
    labelsByBucket.set(key, bucket);
  }
  for (const industry of input.industries) {
    const key = breakdownBucketKey(industry);
    const bucket = industriesByBucket.get(key) ?? [];
    bucket.push(industry);
    industriesByBucket.set(key, bucket);
  }

  const bucketSortKey = (bucket: Array<{ committeeKey: string; supportOppose: TennesseeSupportOppose }>): string =>
    bucket[0] ? breakdownBucketKey(bucket[0]) : "";

  const categoryOrder: Record<LabelCategoryType, number> = {
    donor: 0,
    employer: 1,
    occupation: 2,
  };
  for (const bucket of [...labelsByBucket.values()].sort((left, right) =>
    bucketSortKey(left).localeCompare(bucketSortKey(right))
  )) {
    const labelsByCategory = new Map<LabelCategoryType, LabelAggregate[]>();
    for (const label of bucket) {
      const categoryBucket = labelsByCategory.get(label.categoryType) ?? [];
      categoryBucket.push(label);
      labelsByCategory.set(label.categoryType, categoryBucket);
    }

    for (const categoryType of ["donor", "employer", "occupation"] as const) {
      const categoryBucket = labelsByCategory.get(categoryType) ?? [];
      for (const label of categoryBucket
        .sort(
        (left, right) =>
          categoryOrder[left.categoryType] - categoryOrder[right.categoryType] ||
          right.amountCents - left.amountCents ||
          left.displayName.localeCompare(right.displayName)
        )
        .slice(0, input.maxBreakdownsPerCategory)) {
        result.push({
          committeeKey: label.committeeKey,
          supportOppose: label.supportOppose,
          categoryType: label.categoryType,
          categoryName: label.displayName,
          amount: centsToDollars(label.amountCents),
          contributorCount: label.contributorKeys.size,
          sourceUrl: input.sourceUrl,
        });
      }
    }
  }

  for (const bucket of [...industriesByBucket.values()].sort((left, right) =>
    bucketSortKey(left).localeCompare(bucketSortKey(right))
  )) {
    for (const industry of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
      .slice(0, input.maxBreakdownsPerCategory)) {
      result.push({
        committeeKey: industry.committeeKey,
        supportOppose: industry.supportOppose,
        categoryType: "industry",
        categoryName: industry.industrySlug,
        amount: centsToDollars(industry.amountCents),
        contributorCount: industry.contributorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateTennesseeOutsideGroupContributions(
  input: TennesseeOutsideGroupContributionAggregationInput
): TennesseeOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);

  const outsideGroupsByCommitteeKey = new Map<string, TennesseeOutsideSpendingGroup[]>();
  for (const group of input.outsideGroups) {
    const committeeKey = normalizeCommitteeKey(group.committeeKey || group.committeeName);
    if (!committeeKey) {
      continue;
    }
    const existing = outsideGroupsByCommitteeKey.get(committeeKey) ?? [];
    existing.push({ ...group, committeeKey });
    outsideGroupsByCommitteeKey.set(committeeKey, existing);
  }

  if (outsideGroupsByCommitteeKey.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    };
  }

  const labels = new Map<string, LabelAggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;

  for (const record of input.contributionRecords) {
    const committeeKey = normalizeCommitteeKey(record.recipientName);
    const matchingGroups = outsideGroupsByCommitteeKey.get(committeeKey) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = amountToCents(record.amount);
    if (amountCents === null || !isOutsideReceiptBase({ record, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    const recordContributorKey = contributorKey(record);
    const categoryLabels: Array<{
      categoryType: LabelCategoryType;
      displayName: string;
      normalizedName: string;
    }> = [];
    const donorDisplayName = cleanDisplayLabel(record.contributorName);
    const normalizedDonorName = normalizeFinanceLabel(donorDisplayName, "donor");
    if (donorDisplayName && normalizedDonorName && isOrganizationContributor(record)) {
      categoryLabels.push({
        categoryType: "donor",
        displayName: donorDisplayName,
        normalizedName: normalizedDonorName,
      });
    }

    const employerDisplayName = cleanDisplayLabel(record.contributorEmployer);
    const normalizedEmployerName = normalizeFinanceLabel(employerDisplayName, "employer");
    if (employerDisplayName && normalizedEmployerName && isUsableIndividualLabel(employerDisplayName)) {
      categoryLabels.push({
        categoryType: "employer",
        displayName: employerDisplayName,
        normalizedName: normalizedEmployerName,
      });
    }

    const occupationDisplayName = cleanDisplayLabel(record.contributorOccupation);
    const normalizedOccupationName = normalizeTextKey(occupationDisplayName);
    if (occupationDisplayName && normalizedOccupationName && isUsableIndividualLabel(occupationDisplayName)) {
      categoryLabels.push({
        categoryType: "occupation",
        displayName: occupationDisplayName,
        normalizedName: normalizedOccupationName,
      });
    }

    if (categoryLabels.length === 0) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    for (const group of matchingGroups) {
      for (const label of categoryLabels) {
        addLabelAggregate({
          labels,
          group,
          committeeKey,
          categoryType: label.categoryType,
          displayName: label.displayName,
          normalizedName: label.normalizedName,
          amountCents,
          contributorKey: recordContributorKey,
        });
      }
    }
  }

  const industries = new Map<string, IndustryAggregate>();
  for (const label of labels.values()) {
    const classificationLabelType = labelTypeForClassification(label.categoryType);
    if (!classificationLabelType || label.amountCents < minIndustryAmountCents) {
      continue;
    }
    const classification = classifyFinanceLabel({ rawLabel: label.displayName, labelType: classificationLabelType });
    if (!classification.industrySlug) {
      continue;
    }
    const key = industryKey({
      committeeKey: label.committeeKey,
      supportOppose: label.supportOppose,
      industrySlug: classification.industrySlug,
    });
    const existing = industries.get(key);
    if (existing) {
      existing.amountCents += label.amountCents;
      for (const contributorKey of label.contributorKeys) {
        existing.contributorKeys.add(contributorKey);
      }
      continue;
    }
    industries.set(key, {
      committeeKey: label.committeeKey,
      supportOppose: label.supportOppose,
      industrySlug: classification.industrySlug,
      amountCents: label.amountCents,
      contributorKeys: new Set(label.contributorKeys),
    });
  }

  return {
    outsideGroupBreakdowns: toBreakdowns({
      labels: labels.values(),
      industries: industries.values(),
      sourceUrl: input.sourceUrl ?? null,
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
