import type { Pool, PoolClient } from "pg";

import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import {
  getWisconsinSunshineIndependentExpenditureGroups,
  getWisconsinSunshineOutsideSpenderOrganizationFunders,
  WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
  type WisconsinSunshineAggregate,
  type WisconsinSunshineClientOptions,
  type WisconsinSunshineIndependentSpendingGroup,
} from "./wisconsinSunshineClient.js";
import type {
  WisconsinFinanceOutsideGroupBreakdownInput,
  WisconsinFinanceOutsideGroupInput,
  WisconsinFinanceSummaryInput,
} from "./wisconsinFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type WisconsinOutsideSpendingDataClient = {
  getIndependentExpenditureGroups: (
    input: {
      candidateCommitteeName: string;
      electionYear: number;
      office?: string | null;
      district?: string | null;
      limit?: number;
    },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinSunshineIndependentSpendingGroup[]>;
  getOutsideSpenderOrganizationFunders: (
    input: { entityId: string | number; electionYear: number; limit?: number },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinSunshineAggregate[]>;
};

export type WisconsinOutsideSpendingAggregationInput = {
  candidateCommitteeName: string;
  electionYear: number;
  office?: string | null;
  district?: string | null;
  maxGroups?: number;
  maxFundersPerGroup?: number;
  aiClassificationMinAmount?: number;
  db?: Queryable;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  dryRun?: boolean;
  sunshineClientOptions?: WisconsinSunshineClientOptions;
  sunshineClient?: Partial<WisconsinOutsideSpendingDataClient>;
};

export type WisconsinOutsideSpendingAggregationResult = {
  summary: WisconsinFinanceSummaryInput;
  outsideGroups: WisconsinFinanceOutsideGroupInput[];
  outsideGroupBreakdowns: WisconsinFinanceOutsideGroupBreakdownInput[];
  classifications: FinanceLabelClassification[];
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  outsideGroupCount: number;
  outsideFunderRowCount: number;
  skippedOutsideGroupFunderLookupCount: number;
};

const DEFAULT_MAX_GROUPS = 20;
const DEFAULT_MAX_FUNDERS_PER_GROUP = 20;
const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;

const DEFAULT_SUNSHINE_CLIENT: WisconsinOutsideSpendingDataClient = {
  getIndependentExpenditureGroups: getWisconsinSunshineIndependentExpenditureGroups,
  getOutsideSpenderOrganizationFunders: getWisconsinSunshineOutsideSpenderOrganizationFunders,
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Wisconsin outside spending aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Wisconsin outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeNonnegativeAmount(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Wisconsin outside spending aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeNonEmptyText(value: string, fieldName: string): string {
  const normalized = value.trim().replace(/\s+/g, " ");
  if (normalized.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return normalized;
}

function normalizeOptionalText(value: string | null | undefined): string | undefined {
  const normalized = value?.trim().replace(/\s+/g, " ");
  return normalized && normalized.length > 0 ? normalized : undefined;
}

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function mergeSunshineClient(client: Partial<WisconsinOutsideSpendingDataClient> | undefined): WisconsinOutsideSpendingDataClient {
  return { ...DEFAULT_SUNSHINE_CLIENT, ...(client ?? {}) };
}

function outsideBreakdownKey(input: WisconsinFinanceOutsideGroupBreakdownInput): string {
  const categoryKey =
    input.categoryType === "donor"
      ? normalizeFinanceLabel(input.categoryName, "donor")
      : input.categoryName.trim().toUpperCase();
  return `${input.sponsorId.trim().toUpperCase()}\u0000${input.supportOppose}\u0000${input.categoryType}\u0000${categoryKey}`;
}

function addOutsideBreakdown(
  breakdowns: Map<string, WisconsinFinanceOutsideGroupBreakdownInput>,
  breakdown: WisconsinFinanceOutsideGroupBreakdownInput
): void {
  const key = outsideBreakdownKey(breakdown);
  const existing = breakdowns.get(key);
  if (!existing) {
    breakdowns.set(key, breakdown);
    return;
  }
  breakdowns.set(key, {
    ...existing,
    amount: roundCurrency(existing.amount + breakdown.amount),
    contributorCount:
      existing.contributorCount === null ||
      existing.contributorCount === undefined ||
      breakdown.contributorCount === null ||
      breakdown.contributorCount === undefined
        ? existing.contributorCount ?? breakdown.contributorCount ?? null
        : existing.contributorCount + breakdown.contributorCount,
    sourceUrl: existing.sourceUrl ?? breakdown.sourceUrl,
  });
}

function collectOutsideClassifications(
  breakdowns: Iterable<WisconsinFinanceOutsideGroupBreakdownInput>,
  minAmount: number
): Map<string, FinanceLabelClassification> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor" || breakdown.amount < minAmount) {
      continue;
    }
    mergeFinanceLabelClassification(
      classifications,
      classifyFinanceLabel({ rawLabel: breakdown.categoryName, labelType: "donor" })
    );
  }
  return classifications;
}

function asClassifiableOutsideBreakdowns(breakdowns: Iterable<WisconsinFinanceOutsideGroupBreakdownInput>) {
  return [...breakdowns].map((breakdown) => ({
    committeeId: breakdown.sponsorId,
    supportOppose: breakdown.supportOppose,
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount,
    sourceUrl: breakdown.sourceUrl,
  }));
}

export function toWisconsinOutsideGroups(
  groups: readonly WisconsinSunshineIndependentSpendingGroup[]
): WisconsinFinanceOutsideGroupInput[] {
  return groups.map((group) => ({
    sponsorId: group.sponsorId,
    sponsorName: group.sponsorName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl ?? WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
  }));
}

export function toWisconsinOutsideSummary(input: {
  outsideGroups: readonly WisconsinFinanceOutsideGroupInput[];
}): WisconsinFinanceSummaryInput {
  const outsideSupportTotal = input.outsideGroups
    .filter((group) => group.supportOppose === "support")
    .reduce((sum, group) => sum + group.amount, 0);
  const outsideOpposeTotal = input.outsideGroups
    .filter((group) => group.supportOppose === "oppose")
    .reduce((sum, group) => sum + group.amount, 0);

  return {
    totalReceipts: null,
    directContributionTotal: null,
    totalDisbursements: null,
    cashOnHand: null,
    outsideSupportTotal: roundCurrency(outsideSupportTotal),
    outsideOpposeTotal: roundCurrency(outsideOpposeTotal),
    sourceUrl: WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
  };
}

export async function buildWisconsinOutsideGroupBreakdowns(input: {
  outsideGroups: readonly WisconsinFinanceOutsideGroupInput[];
  electionYear: number;
  maxFundersPerGroup?: number;
  sunshineClientOptions?: WisconsinSunshineClientOptions;
  sunshineClient?: Partial<WisconsinOutsideSpendingDataClient>;
}): Promise<{
  outsideGroupBreakdowns: WisconsinFinanceOutsideGroupBreakdownInput[];
  outsideFunderRowCount: number;
  skippedOutsideGroupFunderLookupCount: number;
}> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxFundersPerGroup = normalizePositiveInteger(
    input.maxFundersPerGroup,
    DEFAULT_MAX_FUNDERS_PER_GROUP,
    "maxFundersPerGroup"
  );
  const sunshineClient = mergeSunshineClient(input.sunshineClient);
  const breakdowns = new Map<string, WisconsinFinanceOutsideGroupBreakdownInput>();
  let outsideFunderRowCount = 0;
  let skippedOutsideGroupFunderLookupCount = 0;

  for (const group of input.outsideGroups) {
    let funders: WisconsinSunshineAggregate[];
    try {
      funders = await sunshineClient.getOutsideSpenderOrganizationFunders(
        {
          entityId: group.sponsorId,
          electionYear,
          limit: maxFundersPerGroup,
        },
        input.sunshineClientOptions
      );
    } catch {
      skippedOutsideGroupFunderLookupCount += 1;
      continue;
    }

    outsideFunderRowCount += funders.length;
    for (const funder of funders) {
      addOutsideBreakdown(breakdowns, {
        sponsorId: group.sponsorId,
        supportOppose: group.supportOppose,
        categoryType: "donor",
        categoryName: funder.categoryName,
        amount: funder.amount,
        contributorCount: funder.count,
        sourceUrl: funder.sourceUrl ?? WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
      });
    }
  }

  return {
    outsideGroupBreakdowns: [...breakdowns.values()],
    outsideFunderRowCount,
    skippedOutsideGroupFunderLookupCount,
  };
}

export async function enrichWisconsinOutsideGroupIndustryBreakdowns(input: {
  db?: Queryable;
  outsideGroupBreakdowns: readonly WisconsinFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  dryRun?: boolean;
}): Promise<{
  outsideGroupBreakdowns: WisconsinFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.outsideGroupBreakdowns) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const aiClassificationMinAmount = normalizeNonnegativeAmount(
    input.aiClassificationMinAmount,
    DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT,
    "aiClassificationMinAmount"
  );
  const breakdowns = new Map<string, WisconsinFinanceOutsideGroupBreakdownInput>();
  for (const breakdown of input.outsideGroupBreakdowns) {
    if (breakdown.categoryType !== "industry") {
      addOutsideBreakdown(breakdowns, breakdown);
    }
  }

  const classifiableOutsideBreakdowns = asClassifiableOutsideBreakdowns(breakdowns.values());
  const classifications = collectOutsideClassifications(breakdowns.values(), aiClassificationMinAmount);
  if (input.db) {
    await resolveFinanceIndustryClassifications({
      db: input.db,
      directBreakdowns: [],
      outsideBreakdowns: classifiableOutsideBreakdowns,
      classifications,
      classifier: input.classifier,
      minAmount: aiClassificationMinAmount,
      dryRun: input.dryRun ?? false,
    });
  }

  const industryBreakdowns = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: [],
    outsideBreakdowns: classifiableOutsideBreakdowns,
    classifications,
  });
  for (const breakdown of industryBreakdowns.outsideIndustryBreakdowns) {
    addOutsideBreakdown(breakdowns, {
      sponsorId: breakdown.committeeId,
      supportOppose: breakdown.supportOppose,
      categoryType: "industry",
      categoryName: breakdown.categoryName,
      amount: breakdown.amount,
      contributorCount: breakdown.contributorCount,
      sourceUrl: breakdown.sourceUrl,
    });
  }

  return {
    outsideGroupBreakdowns: [...breakdowns.values()],
    classifications: [...classifications.values()],
  };
}

export async function aggregateWisconsinOutsideSpending(
  input: WisconsinOutsideSpendingAggregationInput
): Promise<WisconsinOutsideSpendingAggregationResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxGroups = normalizePositiveInteger(input.maxGroups, DEFAULT_MAX_GROUPS, "maxGroups");
  const maxFundersPerGroup = normalizePositiveInteger(
    input.maxFundersPerGroup,
    DEFAULT_MAX_FUNDERS_PER_GROUP,
    "maxFundersPerGroup"
  );
  const candidateCommitteeName = normalizeNonEmptyText(input.candidateCommitteeName, "candidateCommitteeName");
  const office = normalizeOptionalText(input.office);
  const district = normalizeOptionalText(input.district);
  const sunshineClient = mergeSunshineClient(input.sunshineClient);

  const sunshineGroups = await sunshineClient.getIndependentExpenditureGroups(
    {
      candidateCommitteeName,
      electionYear,
      office,
      district,
      limit: maxGroups,
    },
    input.sunshineClientOptions
  );
  const outsideGroups = toWisconsinOutsideGroups(sunshineGroups);
  const summary = toWisconsinOutsideSummary({ outsideGroups });
  const outsideGroupBreakdowns = await buildWisconsinOutsideGroupBreakdowns({
    outsideGroups,
    electionYear,
    maxFundersPerGroup,
    sunshineClientOptions: input.sunshineClientOptions,
    sunshineClient,
  });
  const outsideIndustryFinance = await enrichWisconsinOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns: outsideGroupBreakdowns.outsideGroupBreakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount: input.aiClassificationMinAmount,
    dryRun: input.dryRun,
  });

  return {
    summary,
    outsideGroups,
    outsideGroupBreakdowns: outsideIndustryFinance.outsideGroupBreakdowns ?? [],
    classifications: outsideIndustryFinance.classifications,
    outsideSupportTotal: summary.outsideSupportTotal ?? 0,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? 0,
    outsideGroupCount: outsideGroups.length,
    outsideFunderRowCount: outsideGroupBreakdowns.outsideFunderRowCount,
    skippedOutsideGroupFunderLookupCount: outsideGroupBreakdowns.skippedOutsideGroupFunderLookupCount,
  };
}
