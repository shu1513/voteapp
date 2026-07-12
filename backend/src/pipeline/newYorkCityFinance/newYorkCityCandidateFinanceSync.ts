import type { Pool, PoolClient } from "pg";

import { classifyFinanceLabel, type FinanceLabelClassification } from "../finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import { NEW_YORK_CITY_CFB_DATA_LIBRARY_URL, type NewYorkCityCfbContributionRow } from "./newYorkCityCfbCsv.js";
import type { NewYorkCityCandidateFinanceResolution } from "./newYorkCityCandidateResolver.js";
import {
  aggregateNewYorkCityDirectContributions,
  type NewYorkCityFinanceDirectBreakdown,
} from "./newYorkCityDirectContributionAggregator.js";
import { replaceNewYorkCityCandidateFinanceSnapshot } from "./newYorkCityFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type Connectable = Queryable & Pick<Pool, "connect">;
type MatchedResolution = Extract<NewYorkCityCandidateFinanceResolution, { status: "matched" }>;

function normalizeName(value: string): string {
  return value.replace(/\s+/g, " ").trim().toUpperCase();
}

function mergeIndustryBreakdowns(rows: readonly NewYorkCityFinanceDirectBreakdown[]): NewYorkCityFinanceDirectBreakdown[] {
  const merged = new Map<string, NewYorkCityFinanceDirectBreakdown>();
  for (const row of rows) {
    const existing = merged.get(row.categoryName);
    if (!existing) merged.set(row.categoryName, { ...row });
    else {
      existing.amount = Math.round((existing.amount + row.amount) * 100) / 100;
      existing.contributorCount =
        existing.contributorCount === null || row.contributorCount === null
          ? existing.contributorCount ?? row.contributorCount
          : existing.contributorCount + row.contributorCount;
    }
  }
  return [...merged.values()].sort((a, b) => b.amount - a.amount || a.categoryName.localeCompare(b.categoryName));
}

async function addIndustryBreakdowns(input: {
  db: Queryable;
  breakdowns: readonly NewYorkCityFinanceDirectBreakdown[];
  classifier?: FinanceIndustryClassifier;
  minAmount: number;
  dryRun: boolean;
}): Promise<NewYorkCityFinanceDirectBreakdown[]> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const row of input.breakdowns) {
    if (row.categoryType !== "employer") continue;
    mergeFinanceLabelClassification(classifications, classifyFinanceLabel({ rawLabel: row.categoryName, labelType: "employer" }));
  }
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: input.breakdowns,
    outsideBreakdowns: [],
    classifications,
    classifier: input.classifier,
    minAmount: input.minAmount,
    dryRun: input.dryRun,
  });
  const generated = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: input.breakdowns,
    outsideBreakdowns: [],
    classifications,
  }).directIndustryBreakdowns;
  return [
    ...input.breakdowns,
    ...mergeIndustryBreakdowns(generated.map((row) => ({ ...row, categoryType: "industry" as const }))),
  ];
}

export async function syncNewYorkCityCandidateFinance(input: {
  db: Connectable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  resolution: MatchedResolution;
  contributionRows: readonly NewYorkCityCfbContributionRow[];
  now?: Date;
  dryRun?: boolean;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
}): Promise<{ dryRun: boolean; breakdownsWritten: number; acceptedContributionRows: number }> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) throw new Error("Invalid NYC finance sync timestamp");
  const direct = aggregateNewYorkCityDirectContributions({
    rows: input.contributionRows,
    candidateId: input.resolution.cfbCandidateId,
    electionYear: input.electionYear,
    officeCode: input.resolution.officeCode,
  });
  const breakdowns = await addIndustryBreakdowns({
    db: input.db,
    breakdowns: direct.breakdowns,
    classifier: input.financeIndustryClassifier,
    minAmount: input.aiClassificationMinAmount ?? 25_000,
    dryRun: Boolean(input.dryRun),
  });
  if (!input.dryRun) {
    await replaceNewYorkCityCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId: input.candidateId,
        electionId: input.electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeName(input.candidateName),
        officeCode: input.resolution.officeCode,
        boroughCode: input.resolution.boroughCode,
        cfbCandidateId: input.resolution.cfbCandidateId,
        cfbCandidateName: input.resolution.cfbCandidateName,
        linkSource: "cfb_csv",
        sourceUrl: NEW_YORK_CITY_CFB_DATA_LIBRARY_URL,
        lastVerifiedAt: now,
      },
      summary: {
        privateContributions: input.resolution.summary.privateContributions,
        netExpenditures: input.resolution.summary.netExpenditures,
        outstandingBills: input.resolution.summary.outstandingBills,
        publicFunds: input.resolution.summary.publicFunds,
        sourceUrl: NEW_YORK_CITY_CFB_DATA_LIBRARY_URL,
        lastSyncedAt: now,
      },
      breakdowns,
    });
  }
  return {
    dryRun: Boolean(input.dryRun),
    breakdownsWritten: input.dryRun ? 0 : breakdowns.length,
    acceptedContributionRows: direct.acceptedRowCount,
  };
}
