import {
  getWisconsinSunshineContributionSizeAggregates,
  getWisconsinSunshineDirectOccupationAggregates,
  WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
  type WisconsinSunshineAggregate,
  type WisconsinSunshineClientOptions,
} from "./wisconsinSunshineClient.js";
import type { WisconsinFinanceDirectBreakdownInput, WisconsinFinanceSummaryInput } from "./wisconsinFinanceWriter.js";

export type WisconsinDirectContributionDataClient = {
  getDirectOccupationAggregates: (
    input: { entityId: string | number; electionYear: number; limit?: number },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinSunshineAggregate[]>;
  getContributionSizeAggregates: (
    input: { entityId: string | number; electionYear: number; limit?: number },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinSunshineAggregate[]>;
};

export type WisconsinDirectContributionAggregationInput = {
  entityId: string | number;
  electionYear: number;
  maxBreakdownsPerCategory?: number;
  sunshineClientOptions?: WisconsinSunshineClientOptions;
  sunshineClient?: Partial<WisconsinDirectContributionDataClient>;
};

export type WisconsinDirectContributionAggregationResult = {
  summary: WisconsinFinanceSummaryInput;
  directBreakdowns: WisconsinFinanceDirectBreakdownInput[];
  directOccupationRowCount: number;
  directContributionSizeRowCount: number;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;

const DEFAULT_SUNSHINE_CLIENT: WisconsinDirectContributionDataClient = {
  getDirectOccupationAggregates: getWisconsinSunshineDirectOccupationAggregates,
  getContributionSizeAggregates: getWisconsinSunshineContributionSizeAggregates,
};

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Wisconsin direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Wisconsin direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function mergeSunshineClient(client: Partial<WisconsinDirectContributionDataClient> | undefined): WisconsinDirectContributionDataClient {
  return { ...DEFAULT_SUNSHINE_CLIENT, ...(client ?? {}) };
}

export function toWisconsinDirectBreakdowns(input: {
  occupations: readonly WisconsinSunshineAggregate[];
  contributionSizes: readonly WisconsinSunshineAggregate[];
}): WisconsinFinanceDirectBreakdownInput[] {
  return [
    ...input.occupations.map((row) => ({
      categoryType: "occupation" as const,
      categoryName: row.categoryName,
      amount: row.amount,
      contributorCount: row.count,
      sourceUrl: row.sourceUrl ?? WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
    })),
    ...input.contributionSizes.map((row) => ({
      categoryType: "contribution_size" as const,
      categoryName: row.categoryName,
      amount: row.amount,
      contributorCount: row.count,
      sourceUrl: row.sourceUrl ?? WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
    })),
  ];
}

export function toWisconsinDirectSummary(input: {
  contributionSizes: readonly WisconsinSunshineAggregate[];
}): WisconsinFinanceSummaryInput {
  const directContributionTotal = input.contributionSizes.reduce((sum, row) => sum + row.amount, 0);
  return {
    totalReceipts: null,
    directContributionTotal: input.contributionSizes.length > 0 ? roundCurrency(directContributionTotal) : null,
    totalDisbursements: null,
    cashOnHand: null,
    outsideSupportTotal: null,
    outsideOpposeTotal: null,
    sourceUrl: WISCONSIN_SUNSHINE_TRANSACTIONS_URL,
  };
}

export async function aggregateWisconsinDirectContributions(
  input: WisconsinDirectContributionAggregationInput
): Promise<WisconsinDirectContributionAggregationResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sunshineClient = mergeSunshineClient(input.sunshineClient);

  const [occupations, contributionSizes] = await Promise.all([
    sunshineClient.getDirectOccupationAggregates(
      { entityId: input.entityId, electionYear, limit: maxBreakdownsPerCategory },
      input.sunshineClientOptions
    ),
    sunshineClient.getContributionSizeAggregates(
      { entityId: input.entityId, electionYear, limit: maxBreakdownsPerCategory },
      input.sunshineClientOptions
    ),
  ]);

  return {
    summary: toWisconsinDirectSummary({ contributionSizes }),
    directBreakdowns: toWisconsinDirectBreakdowns({ occupations, contributionSizes }),
    directOccupationRowCount: occupations.length,
    directContributionSizeRowCount: contributionSizes.length,
  };
}
