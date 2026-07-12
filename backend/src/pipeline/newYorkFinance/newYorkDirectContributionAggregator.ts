import {
  isNewYorkOrganizationReceipt,
  normalizeNewYorkFunderKey,
} from "./newYorkOutsideGroupContributionAggregator.js";
import {
  getNewYorkCommitteeExpenditureTotal,
  getNewYorkCommitteeItemizedReceipts,
  NEW_YORK_SODA_DISCLOSURES_PAGE_URL,
  type NewYorkCommitteeReceiptRow,
  type NewYorkCycleYears,
  type NewYorkSodaClientOptions,
} from "./newYorkSodaClient.js";

// Direct-campaign receipts for the candidate's own authorized committee
// (Phase 2 of plan-new-york-finance.md). NYSBOE never collects donor
// occupation/employer, so New York's direct breakdowns are size buckets,
// contributor types, and organization donors only. "Unitemized" lumps appear
// as itemized Schedule A rows with no contributor identity (verified live:
// trans_explntn "A-Unitemized"); they count toward totals but never toward
// per-contribution breakdowns.

export type NewYorkDirectBreakdown = {
  categoryType: "contribution_size" | "contributor_type" | "donor";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type NewYorkDirectContributionResult = {
  directContributionTotal: number;
  breakdowns: NewYorkDirectBreakdown[];
  receiptRowCount: number;
  lumpRowCount: number;
};

export type NewYorkDirectCampaignResult = NewYorkDirectContributionResult & {
  totalDisbursements: number | null;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function normalizeLimit(value: number | undefined): number {
  const normalized = value ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New York direct breakdown limit: ${value}`);
  }
  return normalized;
}

// Matches the bucket labels the other newer state modules present.
function contributionSizeBucket(amount: number): string {
  if (amount < 100) {
    return "$1-$99";
  }
  if (amount < 250) {
    return "$100-$249";
  }
  if (amount < 500) {
    return "$250-$499";
  }
  if (amount < 1_000) {
    return "$500-$999";
  }
  if (amount < 5_000) {
    return "$1,000-$4,999";
  }
  return "$5,000+";
}

function hasContributorIdentity(receipt: NewYorkCommitteeReceiptRow): boolean {
  return receipt.entityName.length > 0 || receipt.entityFirstName.length > 0 || receipt.entityLastName.length > 0;
}

function addToBucket(
  buckets: Map<string, { amount: number; contributorCount: number }>,
  key: string,
  amount: number
): void {
  const existing = buckets.get(key);
  if (existing) {
    existing.amount = roundCurrency(existing.amount + amount);
    existing.contributorCount += 1;
    return;
  }
  buckets.set(key, { amount: roundCurrency(amount), contributorCount: 1 });
}

function toBreakdowns(
  categoryType: NewYorkDirectBreakdown["categoryType"],
  buckets: ReadonlyMap<string, { amount: number; contributorCount: number }>,
  nameByKey: ReadonlyMap<string, string> | null,
  limit: number
): NewYorkDirectBreakdown[] {
  return [...buckets.entries()]
    .map(([key, bucket]) => ({
      categoryType,
      categoryName: nameByKey?.get(key) ?? key,
      amount: bucket.amount,
      contributorCount: bucket.contributorCount,
      sourceUrl: NEW_YORK_SODA_DISCLOSURES_PAGE_URL,
    }))
    .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
    .slice(0, limit);
}

export function aggregateNewYorkDirectContributions(input: {
  receipts: readonly NewYorkCommitteeReceiptRow[];
  maxBreakdownsPerCategory?: number;
}): NewYorkDirectContributionResult {
  const limit = normalizeLimit(input.maxBreakdownsPerCategory);
  const sizeBuckets = new Map<string, { amount: number; contributorCount: number }>();
  const typeBuckets = new Map<string, { amount: number; contributorCount: number }>();
  const donorBuckets = new Map<string, { amount: number; contributorCount: number }>();
  const donorNamesByKey = new Map<string, string>();
  let totalCents = 0;
  let lumpRowCount = 0;

  for (const receipt of input.receipts) {
    totalCents += Math.round(receipt.amount * 100);

    if (!hasContributorIdentity(receipt)) {
      // Unitemized lump: totals only.
      lumpRowCount += 1;
      continue;
    }

    addToBucket(sizeBuckets, contributionSizeBucket(receipt.amount), receipt.amount);

    if (receipt.contributorType) {
      addToBucket(typeBuckets, receipt.contributorType, receipt.amount);
    }

    if (isNewYorkOrganizationReceipt(receipt)) {
      const donorKey = normalizeNewYorkFunderKey(receipt.entityName);
      if (!donorNamesByKey.has(donorKey)) {
        donorNamesByKey.set(donorKey, receipt.entityName);
      }
      addToBucket(donorBuckets, donorKey, receipt.amount);
    }
  }

  return {
    directContributionTotal: roundCurrency(totalCents / 100),
    breakdowns: [
      ...toBreakdowns("contribution_size", sizeBuckets, null, limit),
      ...toBreakdowns("contributor_type", typeBuckets, null, limit),
      ...toBreakdowns("donor", donorBuckets, donorNamesByKey, limit),
    ],
    receiptRowCount: input.receipts.length,
    lumpRowCount,
  };
}

export type NewYorkDirectCampaignDataClient = {
  getCommitteeItemizedReceipts: typeof getNewYorkCommitteeItemizedReceipts;
  getCommitteeExpenditureTotal: typeof getNewYorkCommitteeExpenditureTotal;
};

const DEFAULT_DIRECT_CAMPAIGN_CLIENT: NewYorkDirectCampaignDataClient = {
  getCommitteeItemizedReceipts: getNewYorkCommitteeItemizedReceipts,
  getCommitteeExpenditureTotal: getNewYorkCommitteeExpenditureTotal,
};

export async function collectNewYorkDirectCampaign(
  input: {
    filerId: string;
    electionYear: number;
    cycleYears: NewYorkCycleYears;
    maxBreakdownsPerCategory?: number;
  },
  options: NewYorkSodaClientOptions = {},
  client: Partial<NewYorkDirectCampaignDataClient> = {}
): Promise<NewYorkDirectCampaignResult> {
  const dataClient: NewYorkDirectCampaignDataClient = { ...DEFAULT_DIRECT_CAMPAIGN_CLIENT, ...client };
  const cycleScope = { filerId: input.filerId, electionYear: input.electionYear, cycleYears: input.cycleYears };
  const [receipts, totalDisbursements] = await Promise.all([
    dataClient.getCommitteeItemizedReceipts(cycleScope, options),
    dataClient.getCommitteeExpenditureTotal(cycleScope, options),
  ]);
  return {
    ...aggregateNewYorkDirectContributions({
      receipts,
      maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
    }),
    totalDisbursements,
  };
}
