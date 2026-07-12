import type { Pool, PoolClient } from "pg";
import {
  classifyFinanceLabel,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import {
  getLosAngelesIndependentSpending,
  type LosAngelesCityEthicsClientOptions,
  type LosAngelesEthicsCandidateTotal,
} from "./losAngelesCityEthicsClient.js";
import {
  aggregateLosAngelesDirectContributions,
  type LosAngelesDirectBreakdown,
} from "./losAngelesDirectContributionAggregator.js";
import { replaceLosAngelesCandidateFinanceSnapshot } from "./losAngelesFinanceWriter.js";
import {
  defaultLosAngelesOpenDataClientOptions,
  getLosAngelesCommitteeContributions,
  type LosAngelesOpenDataClientOptions,
} from "./losAngelesOpenDataClient.js";
import { aggregateLosAngelesOutsideSpending } from "./losAngelesOutsideSpendingAggregator.js";
import { normalizeLosAngelesCandidateName } from "./losAngelesCandidateCommitteeResolver.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type PoolLike = Queryable & Pick<Pool, "connect">;
export async function syncLosAngelesCandidateFinance(input: {
  db: PoolLike;
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateName: string;
  officeName: string;
  seatNumber?: number | null;
  total: LosAngelesEthicsCandidateTotal;
  ethicsClientOptions?: LosAngelesCityEthicsClientOptions;
  openDataClientOptions?: LosAngelesOpenDataClientOptions;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  dryRun?: boolean;
  now?: Date;
}): Promise<{
  linkWritten: boolean;
  directBreakdownCount: number;
  outsideGroupCount: number;
  reconciledContributionTotal: number;
  headlineContributionTotal: number;
  reconciliationDifference: number;
}> {
  const [records, supportRows, opposeRows] = await Promise.all([
    getLosAngelesCommitteeContributions(
      {
        committeeId: input.total.fppcCommitteeId,
        electionYear: input.electionYear,
      },
      {
        ...defaultLosAngelesOpenDataClientOptions(),
        ...input.openDataClientOptions,
      },
    ),
    getLosAngelesIndependentSpending(
      {
        electionSeatCandidateId: input.total.electionSeatCandidateId,
        supportOppose: "support",
      },
      input.ethicsClientOptions,
    ),
    getLosAngelesIndependentSpending(
      {
        electionSeatCandidateId: input.total.electionSeatCandidateId,
        supportOppose: "oppose",
      },
      input.ethicsClientOptions,
    ),
  ]);
  const direct = aggregateLosAngelesDirectContributions({ records });
  const outside = aggregateLosAngelesOutsideSpending([
    ...supportRows,
    ...opposeRows,
  ]);
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const row of direct.breakdowns)
    if (row.categoryType === "employer")
      mergeFinanceLabelClassification(
        classifications,
        classifyFinanceLabel({
          rawLabel: row.categoryName,
          labelType: "employer",
        }),
      );
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: direct.breakdowns,
    outsideBreakdowns: [],
    classifications,
    classifier: input.financeIndustryClassifier,
    minAmount: input.aiClassificationMinAmount ?? 25_000,
    dryRun: Boolean(input.dryRun),
  });
  const industryRaw = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: direct.breakdowns,
    outsideBreakdowns: [],
    classifications,
  }).directIndustryBreakdowns;
  const industries = new Map<string, LosAngelesDirectBreakdown>();
  for (const row of industryRaw) {
    const old = industries.get(row.categoryName);
    industries.set(row.categoryName, {
      categoryType: "industry",
      categoryName: row.categoryName,
      amount: Math.round(((old?.amount ?? 0) + row.amount) * 100) / 100,
      contributorCount:
        (old?.contributorCount ?? 0) + (row.contributorCount ?? 0),
      sourceUrl: row.sourceUrl ?? input.total.sourceUrl,
    });
  }
  const breakdowns = [
    ...direct.breakdowns,
    ...industries.values(),
  ] as LosAngelesDirectBreakdown[];
  if (!input.dryRun)
    await replaceLosAngelesCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId: input.candidateId,
        electionId: input.electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeLosAngelesCandidateName(
          input.candidateName,
        ),
        officeName: input.officeName,
        seatNumber: input.seatNumber,
        ethicsElectionId: input.total.electionId,
        ethicsCandidatePersonId: input.total.candidatePersonId,
        ethicsSeatCandidateId: input.total.electionSeatCandidateId,
        fppcCommitteeId: input.total.fppcCommitteeId,
        committeeName: input.total.committeeName,
        internalCommitteePersonId: input.total.internalCommitteePersonId,
        linkStatus: "active",
        linkSource: "lacity_ethics",
        sourceUrl: input.total.sourceUrl,
        lastVerifiedAt: input.now ?? new Date(),
      },
      summary: {
        totalReceipts: input.total.totalContributions,
        totalDisbursements: input.total.totalExpenditures,
        cashOnHand: input.total.cashOnHand,
        matchingFunds: input.total.matchingFunds,
        outsideSupportTotal: input.total.outsideSupportTotal,
        outsideOpposeTotal: input.total.outsideOpposeTotal,
        membershipSupportTotal: input.total.membershipSupportTotal,
        membershipOpposeTotal: input.total.membershipOpposeTotal,
        sourceUrl: input.total.sourceUrl,
        reportedThrough: input.total.reportedThrough,
      },
      directBreakdowns: breakdowns,
      outsideGroups: outside.groups,
      classifications: [...classifications.values()],
      syncedAt: input.now,
    });
  return {
    linkWritten: !input.dryRun,
    directBreakdownCount: breakdowns.length,
    outsideGroupCount: outside.groups.length,
    reconciledContributionTotal: direct.reconciledContributionTotal,
    headlineContributionTotal: input.total.totalContributions,
    reconciliationDifference:
      Math.round(
        (direct.reconciledContributionTotal - input.total.totalContributions) *
          100,
      ) / 100,
  };
}
