import {
  searchArizonaSpotlightIncomeTransactions,
  searchArizonaSpotlightIndependentExpenditures,
  type ArizonaSpotlightClientOptions,
  type ArizonaSpotlightIncomeTransaction,
  type ArizonaSpotlightIndependentExpenditure,
} from "./arizonaSpotlightClient.js";
import {
  aggregateArizonaDirectContributions,
  type ArizonaDirectContributionAggregationResult,
} from "./arizonaDirectContributionAggregator.js";
import {
  aggregateArizonaOutsideGroupContributions,
  type ArizonaOutsideGroupContributionAggregationResult,
} from "./arizonaOutsideGroupContributionAggregator.js";
import {
  aggregateArizonaOutsideSpending,
  type ArizonaOutsideSpendingAggregationResult,
  type ArizonaOutsideSpendingGroup,
} from "./arizonaOutsideSpendingAggregator.js";

export type ArizonaCandidateFinanceSnapshotClient = {
  searchIncomeTransactions: typeof searchArizonaSpotlightIncomeTransactions;
  searchIndependentExpenditures: typeof searchArizonaSpotlightIndependentExpenditures;
};

export type ArizonaCandidateFinanceSnapshotInput = {
  candidateName: string;
  candidateCommitteeId: string;
  electionYear: number;
  candidateFilerId?: string | null;
  spotlightClientOptions?: ArizonaSpotlightClientOptions;
  spotlightClient?: Partial<ArizonaCandidateFinanceSnapshotClient>;
  includeOutside?: boolean;
  directIncomeLimit?: number;
  independentExpenditureLimitPerPosition?: number;
  outsideGroupIncomeLimitPerGroup?: number;
  outsideMaxGroups?: number;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

export type ArizonaCandidateFinanceSnapshot = {
  candidateName: string;
  candidateCommitteeId: string;
  candidateFilerId: string | null;
  electionYear: number;
  directFinance: ArizonaDirectContributionAggregationResult;
  outsideSpending: ArizonaOutsideSpendingAggregationResult;
  outsideGroupContributions: ArizonaOutsideGroupContributionAggregationResult;
  fetched: {
    directIncomeTransactionCount: number;
    supportIndependentExpenditureCount: number;
    opposeIndependentExpenditureCount: number;
    outsideGroupIncomeTransactionCount: number;
    outsideGroupIncomeCommitteeCount: number;
  };
};

const DEFAULT_DIRECT_INCOME_LIMIT = 5_000;
const DEFAULT_INDEPENDENT_EXPENDITURE_LIMIT_PER_POSITION = 5_000;
const DEFAULT_OUTSIDE_GROUP_INCOME_LIMIT_PER_GROUP = 5_000;
const DEFAULT_OUTSIDE_MAX_GROUPS = 20;
const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;
const OUTSIDE_GROUP_INCOME_FETCH_CONCURRENCY = 5;

const DEFAULT_SPOTLIGHT_CLIENT: ArizonaCandidateFinanceSnapshotClient = {
  searchIncomeTransactions: searchArizonaSpotlightIncomeTransactions,
  searchIndependentExpenditures: searchArizonaSpotlightIndependentExpenditures,
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim().replace(/\s+/g, " ");
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2002 || value > 2100) {
    throw new Error(`Invalid Arizona candidate finance snapshot election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Arizona candidate finance snapshot ${fieldName}: ${value}`);
  }
  return normalized;
}

function mergeClient(client: Partial<ArizonaCandidateFinanceSnapshotClient> | undefined): ArizonaCandidateFinanceSnapshotClient {
  return { ...DEFAULT_SPOTLIGHT_CLIENT, ...(client ?? {}) };
}

function uniqueOutsideGroups(groups: readonly ArizonaOutsideSpendingGroup[]): ArizonaOutsideSpendingGroup[] {
  const byCommitteeId = new Map<string, ArizonaOutsideSpendingGroup>();
  for (const group of groups) {
    const key = group.committeeId.trim().toUpperCase();
    if (!key) {
      continue;
    }
    const existing = byCommitteeId.get(key);
    if (!existing || group.amount > existing.amount) {
      byCommitteeId.set(key, group);
    }
  }
  return [...byCommitteeId.values()];
}

async function fetchOutsideGroupIncomeTransactions(input: {
  spotlightClient: ArizonaCandidateFinanceSnapshotClient;
  spotlightClientOptions?: ArizonaSpotlightClientOptions;
  electionYear: number;
  outsideGroups: readonly ArizonaOutsideSpendingGroup[];
  outsideGroupIncomeLimitPerGroup: number;
}): Promise<ArizonaSpotlightIncomeTransaction[]> {
  const batches: ArizonaSpotlightIncomeTransaction[][] = [];
  for (let index = 0; index < input.outsideGroups.length; index += OUTSIDE_GROUP_INCOME_FETCH_CONCURRENCY) {
    const groupBatch = input.outsideGroups.slice(index, index + OUTSIDE_GROUP_INCOME_FETCH_CONCURRENCY);
    batches.push(
      ...(await Promise.all(
        groupBatch.map((group) =>
          input.spotlightClient.searchIncomeTransactions(
            {
              electionYear: input.electionYear,
              filerId: group.committeeId,
              limit: input.outsideGroupIncomeLimitPerGroup,
            },
            input.spotlightClientOptions
          )
        )
      ))
    );
  }
  return batches.flat();
}

export async function buildArizonaCandidateFinanceSnapshot(
  input: ArizonaCandidateFinanceSnapshotInput
): Promise<ArizonaCandidateFinanceSnapshot> {
  const candidateName = requireNonEmpty(input.candidateName, "Arizona candidate name");
  const candidateCommitteeId = requireNonEmpty(input.candidateCommitteeId, "Arizona candidate committee id");
  const candidateFilerId = input.candidateFilerId?.trim() || candidateCommitteeId;
  const electionYear = normalizeElectionYear(input.electionYear);
  const includeOutside = input.includeOutside !== false;
  const directIncomeLimit = normalizePositiveInteger(input.directIncomeLimit, DEFAULT_DIRECT_INCOME_LIMIT, "directIncomeLimit");
  const independentExpenditureLimitPerPosition = normalizePositiveInteger(
    input.independentExpenditureLimitPerPosition,
    DEFAULT_INDEPENDENT_EXPENDITURE_LIMIT_PER_POSITION,
    "independentExpenditureLimitPerPosition"
  );
  const outsideGroupIncomeLimitPerGroup = normalizePositiveInteger(
    input.outsideGroupIncomeLimitPerGroup,
    DEFAULT_OUTSIDE_GROUP_INCOME_LIMIT_PER_GROUP,
    "outsideGroupIncomeLimitPerGroup"
  );
  const outsideMaxGroups = normalizePositiveInteger(input.outsideMaxGroups, DEFAULT_OUTSIDE_MAX_GROUPS, "outsideMaxGroups");
  const directMaxBreakdownsPerCategory = normalizePositiveInteger(
    input.directMaxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "directMaxBreakdownsPerCategory"
  );
  const outsideMaxBreakdownsPerCategory = normalizePositiveInteger(
    input.outsideMaxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "outsideMaxBreakdownsPerCategory"
  );
  const spotlightClient = mergeClient(input.spotlightClient);

  const directIncomeTransactions = await spotlightClient.searchIncomeTransactions(
    {
      electionYear,
      filerId: candidateCommitteeId,
      limit: directIncomeLimit,
    },
    input.spotlightClientOptions
  );

  const directFinance = aggregateArizonaDirectContributions({
    committeeId: candidateCommitteeId,
    electionYear,
    incomeTransactions: directIncomeTransactions,
    maxBreakdownsPerCategory: directMaxBreakdownsPerCategory,
    sourceUrl: directIncomeTransactions[0]?.sourceUrl ?? null,
  });

  let supportIndependentExpenditures: ArizonaSpotlightIndependentExpenditure[] = [];
  let opposeIndependentExpenditures: ArizonaSpotlightIndependentExpenditure[] = [];
  let outsideSpending: ArizonaOutsideSpendingAggregationResult = {
    summary: null,
    matchedIndependentExpenditureCount: 0,
    includedIndependentExpenditureCount: 0,
    skippedIndependentExpenditureCount: 0,
  };
  let outsideGroupIncomeTransactions: ArizonaSpotlightIncomeTransaction[] = [];

  if (includeOutside) {
    [supportIndependentExpenditures, opposeIndependentExpenditures] = await Promise.all([
      spotlightClient.searchIndependentExpenditures(
        {
          electionYear,
          candidateName,
          candidateFilerId,
          position: "Support",
          limit: independentExpenditureLimitPerPosition,
        },
        input.spotlightClientOptions
      ),
      spotlightClient.searchIndependentExpenditures(
        {
          electionYear,
          candidateName,
          candidateFilerId,
          position: "Oppose",
          limit: independentExpenditureLimitPerPosition,
        },
        input.spotlightClientOptions
      ),
    ]);

    outsideSpending = aggregateArizonaOutsideSpending({
      electionYear,
      independentExpenditures: [...supportIndependentExpenditures, ...opposeIndependentExpenditures],
      maxGroups: outsideMaxGroups,
      sourceUrl: supportIndependentExpenditures[0]?.sourceUrl ?? opposeIndependentExpenditures[0]?.sourceUrl ?? null,
    });

    outsideGroupIncomeTransactions = await fetchOutsideGroupIncomeTransactions({
      spotlightClient,
      spotlightClientOptions: input.spotlightClientOptions,
      electionYear,
      outsideGroups: uniqueOutsideGroups(outsideSpending.summary?.groups ?? []),
      outsideGroupIncomeLimitPerGroup,
    });
  }

  const outsideGroupContributions = aggregateArizonaOutsideGroupContributions({
    electionYear,
    outsideGroups: outsideSpending.summary?.groups ?? [],
    incomeTransactions: outsideGroupIncomeTransactions,
    maxBreakdownsPerCategory: outsideMaxBreakdownsPerCategory,
    minIndustryAmount: input.minIndustryAmount,
    sourceUrl: outsideGroupIncomeTransactions[0]?.sourceUrl ?? null,
  });

  return {
    candidateName,
    candidateCommitteeId,
    candidateFilerId: candidateFilerId || null,
    electionYear,
    directFinance,
    outsideSpending,
    outsideGroupContributions,
    fetched: {
      directIncomeTransactionCount: directIncomeTransactions.length,
      supportIndependentExpenditureCount: supportIndependentExpenditures.length,
      opposeIndependentExpenditureCount: opposeIndependentExpenditures.length,
      outsideGroupIncomeTransactionCount: outsideGroupIncomeTransactions.length,
      outsideGroupIncomeCommitteeCount: uniqueOutsideGroups(outsideSpending.summary?.groups ?? []).length,
    },
  };
}
