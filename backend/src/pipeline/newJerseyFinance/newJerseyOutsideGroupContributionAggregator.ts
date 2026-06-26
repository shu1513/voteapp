import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
  type FinanceLabelType,
} from "../finance/financeLabelClassifier.js";
import {
  getNewJerseyElecContributionRows,
  type NewJerseyElecClientOptions,
  type NewJerseyElecContributionRow,
} from "./newJerseyElecClient.js";
import type { NewJerseyOutsideSpendingGroup, NewJerseySupportOppose } from "./newJerseyOutsideSpendingAggregator.js";

export type NewJerseyFinanceOutsideGroupBreakdown = {
  entityS: number;
  supportOppose: NewJerseySupportOppose;
  categoryType: "donor" | "contributor_type" | "occupation" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type NewJerseyOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly NewJerseyOutsideSpendingGroup[];
  contributions: readonly NewJerseyElecContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type NewJerseyOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: NewJerseyFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

export type NewJerseyOutsideGroupContributionFromElecInput = Omit<
  NewJerseyOutsideGroupContributionAggregationInput,
  "contributions"
> & {
  clientOptions?: NewJerseyElecClientOptions;
  elecClient?: Partial<{
    getContributionRows: typeof getNewJerseyElecContributionRows;
  }>;
};

export type NewJerseyOutsideGroupContributionFromElecResult =
  NewJerseyOutsideGroupContributionAggregationResult & {
    fetchedOutsideGroupCount: number;
    fetchedContributionRowCount: number;
    skippedOutsideGroupCount: number;
  };

type BreakdownCategory = NewJerseyFinanceOutsideGroupBreakdown["categoryType"];

type Aggregate = {
  entityS: number;
  supportOppose: NewJerseySupportOppose;
  categoryType: BreakdownCategory;
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
  sourceUrl: string | null;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function normalizeEntityS(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid New Jersey outside group contribution entityS: ${value}`);
  }
  return value;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1980 || value > 2100) {
    throw new Error(`Invalid New Jersey outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Jersey outside group contribution ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid New Jersey outside group contribution minIndustryAmount: ${value}`);
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

function isContributionReceipt(contribution: NewJerseyElecContributionRow): boolean {
  const type = normalizeTextKey(contribution.contributionType);
  return !/\b(REFUND|LOAN|REVERSAL|RETURNED)\b/.test(type);
}

function isOutsideGroupFunderContribution(input: {
  contribution: NewJerseyElecContributionRow;
  electionYear: number;
}): boolean {
  const amountCents = amountToCents(input.contribution.amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isContributionReceipt(input.contribution) &&
    (input.contribution.electionYear === null || input.contribution.electionYear === input.electionYear)
  );
}

function contributionSourceLabel(contribution: NewJerseyElecContributionRow): {
  rawLabel: string;
  labelType: FinanceLabelType;
} | null {
  if (!contribution.isIndividual) {
    const donor = contribution.contributorName.trim().replace(/\s+/g, " ");
    return donor ? { rawLabel: donor, labelType: "donor" } : null;
  }

  const employer = contribution.employerName?.trim().replace(/\s+/g, " ");
  if (employer) {
    return { rawLabel: employer, labelType: "employer" };
  }

  const occupation = contribution.occupationName?.trim().replace(/\s+/g, " ");
  return occupation ? { rawLabel: occupation, labelType: "occupation" } : null;
}

function contributorIdentityKey(contribution: NewJerseyElecContributionRow): string {
  const parts = [
    contribution.contributorName,
    contribution.employerName,
    contribution.occupationName,
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : "unknown";
}

function breakdownKey(input: {
  entityS: number;
  supportOppose: NewJerseySupportOppose;
  categoryType: BreakdownCategory;
  categoryName: string;
}): string {
  return `${input.entityS}\u0000${input.supportOppose}\u0000${input.categoryType}\u0000${normalizeTextKey(input.categoryName)}`;
}

function breakdownBucketKey(input: { entityS: number; supportOppose: NewJerseySupportOppose }): string {
  return `${input.entityS}\u0000${input.supportOppose}`;
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  input: {
    entityS: number;
    supportOppose: NewJerseySupportOppose;
    categoryType: BreakdownCategory;
    categoryName: string | null | undefined;
    amountCents: number;
    contributorKey: string;
    sourceUrl: string | null;
  }
): void {
  const categoryName = input.categoryName?.trim().replace(/\s+/g, " ") ?? "";
  if (!categoryName) {
    return;
  }

  const key = breakdownKey({ ...input, categoryName });
  const existing = aggregates.get(key);
  if (!existing) {
    aggregates.set(key, {
      entityS: input.entityS,
      supportOppose: input.supportOppose,
      categoryType: input.categoryType,
      categoryName,
      amountCents: input.amountCents,
      contributorKeys: new Set([input.contributorKey]),
      sourceUrl: input.sourceUrl,
    });
    return;
  }

  existing.amountCents += input.amountCents;
  existing.contributorKeys.add(input.contributorKey);
  existing.sourceUrl = existing.sourceUrl ?? input.sourceUrl;
}

function toBreakdowns(input: {
  aggregates: Iterable<Aggregate>;
  maxBreakdownsPerCategory: number;
}): NewJerseyFinanceOutsideGroupBreakdown[] {
  const byBucketAndCategory = new Map<string, Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const key = `${breakdownBucketKey(aggregate)}\u0000${aggregate.categoryType}`;
    const bucket = byBucketAndCategory.get(key) ?? [];
    bucket.push(aggregate);
    byBucketAndCategory.set(key, bucket);
  }

  const result: NewJerseyFinanceOutsideGroupBreakdown[] = [];
  const categoryOrder: BreakdownCategory[] = ["donor", "contributor_type", "occupation", "industry"];
  const orderedBuckets = [...byBucketAndCategory.entries()].sort(([left], [right]) => left.localeCompare(right));
  for (const categoryType of categoryOrder) {
    for (const [bucketKey, bucket] of orderedBuckets) {
      if (!bucketKey.endsWith(`\u0000${categoryType}`)) {
        continue;
      }
      for (const aggregate of bucket
        .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))
        .slice(0, input.maxBreakdownsPerCategory)) {
        result.push({
          entityS: aggregate.entityS,
          supportOppose: aggregate.supportOppose,
          categoryType: aggregate.categoryType,
          categoryName: aggregate.categoryName,
          amount: centsToDollars(aggregate.amountCents),
          contributorCount: aggregate.contributorKeys.size,
          sourceUrl: aggregate.sourceUrl,
        });
      }
    }
  }

  return result;
}

export function aggregateNewJerseyOutsideGroupContributions(
  input: NewJerseyOutsideGroupContributionAggregationInput
): NewJerseyOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const sourceUrl = input.sourceUrl ?? null;

  const outsideGroupsByEntityS = new Map<number, NewJerseyOutsideSpendingGroup[]>();
  for (const group of input.outsideGroups) {
    const entityS = normalizeEntityS(group.entityS);
    const existing = outsideGroupsByEntityS.get(entityS) ?? [];
    existing.push({ ...group, entityS });
    outsideGroupsByEntityS.set(entityS, existing);
  }

  if (outsideGroupsByEntityS.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    };
  }

  const aggregates = new Map<string, Aggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;

  for (const contribution of input.contributions) {
    const matchingGroups = outsideGroupsByEntityS.get(contribution.entityS) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;
    const supportOpposeValues = [...new Set(matchingGroups.map((group) => group.supportOppose))];
    const supportOppose = supportOpposeValues.length === 1 ? supportOpposeValues[0] ?? null : null;
    const amountCents = amountToCents(contribution.amount);
    const sourceLabel = contributionSourceLabel(contribution);
    if (
      !supportOppose ||
      amountCents === null ||
      !sourceLabel ||
      !isOutsideGroupFunderContribution({ contribution, electionYear })
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    const group = matchingGroups.find((candidateGroup) => candidateGroup.supportOppose === supportOppose);
    if (!group) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    const contributorKey = contributorIdentityKey(contribution);
    const rowSourceUrl = contribution.sourceUrl ?? group.sourceUrl ?? sourceUrl;

    if (!contribution.isIndividual) {
      const normalizedName = normalizeFinanceLabel(contribution.contributorName, "donor");
      if (normalizedName) {
        addAggregate(aggregates, {
          entityS: group.entityS,
          supportOppose,
          categoryType: "donor",
          categoryName: contribution.contributorName,
          amountCents,
          contributorKey,
          sourceUrl: rowSourceUrl,
        });
      }
    }

    addAggregate(aggregates, {
      entityS: group.entityS,
      supportOppose,
      categoryType: "contributor_type",
      categoryName: contribution.contributorType,
      amountCents,
      contributorKey,
      sourceUrl: rowSourceUrl,
    });
    addAggregate(aggregates, {
      entityS: group.entityS,
      supportOppose,
      categoryType: "occupation",
      categoryName: contribution.occupationName,
      amountCents,
      contributorKey,
      sourceUrl: rowSourceUrl,
    });

    if (amountCents < minIndustryAmountCents) {
      continue;
    }
    const classification = classifyFinanceLabel(sourceLabel);
    if (!classification.industrySlug) {
      continue;
    }
    addAggregate(aggregates, {
      entityS: group.entityS,
      supportOppose,
      categoryType: "industry",
      categoryName: classification.industrySlug,
      amountCents,
      contributorKey,
      sourceUrl: rowSourceUrl,
    });
  }

  return {
    outsideGroupBreakdowns: toBreakdowns({
      aggregates: aggregates.values(),
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}

export async function aggregateNewJerseyOutsideGroupContributionsFromElec(
  input: NewJerseyOutsideGroupContributionFromElecInput
): Promise<NewJerseyOutsideGroupContributionFromElecResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const getContributionRows = input.elecClient?.getContributionRows ?? getNewJerseyElecContributionRows;
  const contributions: NewJerseyElecContributionRow[] = [];
  let fetchedOutsideGroupCount = 0;
  let skippedOutsideGroupCount = 0;

  const outsideGroupsByEntityS = new Map<number, NewJerseyOutsideSpendingGroup>();
  for (const group of input.outsideGroups) {
    const entityS = normalizeEntityS(group.entityS);
    if (!outsideGroupsByEntityS.has(entityS)) {
      outsideGroupsByEntityS.set(entityS, { ...group, entityS });
    }
  }

  for (const group of outsideGroupsByEntityS.values()) {
    try {
      const result = await getContributionRows(
        {
          entityS: group.entityS,
          electionYear,
          nonPacOnly: false,
        },
        input.clientOptions
      );
      fetchedOutsideGroupCount += 1;
      contributions.push(...result.rows);
    } catch {
      skippedOutsideGroupCount += 1;
    }
  }

  const aggregation = aggregateNewJerseyOutsideGroupContributions({
    electionYear,
    outsideGroups: input.outsideGroups,
    contributions,
    sourceUrl: input.sourceUrl,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
    minIndustryAmount: input.minIndustryAmount,
  });

  return {
    ...aggregation,
    fetchedOutsideGroupCount,
    fetchedContributionRowCount: contributions.length,
    skippedOutsideGroupCount,
  };
}
