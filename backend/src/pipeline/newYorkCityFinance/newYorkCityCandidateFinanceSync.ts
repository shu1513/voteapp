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
import {
  buildNewYorkCityCfbIndependentSpendingSourceUrl,
  type NewYorkCityCfbIndependentSpenderFunderRow,
  type NewYorkCityCfbIndependentSpendingRow,
} from "./newYorkCityCfbIndependentSpendingClient.js";
import {
  aggregateNewYorkCityOutsideGroupFunders,
  type NewYorkCityOutsideGroupBreakdown,
} from "./newYorkCityOutsideGroupFunderAggregator.js";
import { aggregateNewYorkCityOutsideSpending } from "./newYorkCityOutsideSpendingAggregator.js";

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

async function addOutsideIndustryBreakdowns(input: {
  db: Queryable;
  breakdowns: readonly NewYorkCityOutsideGroupBreakdown[];
  classifier?: FinanceIndustryClassifier;
  minAmount: number;
  dryRun: boolean;
}): Promise<NewYorkCityOutsideGroupBreakdown[]> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const row of input.breakdowns) {
    if (row.categoryType !== "donor") continue;
    mergeFinanceLabelClassification(classifications, classifyFinanceLabel({ rawLabel: row.categoryName, labelType: "donor" }));
  }
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: [],
    outsideBreakdowns: input.breakdowns.map((row) => ({ ...row, committeeId: row.spenderId })),
    classifications,
    classifier: input.classifier,
    minAmount: input.minAmount,
    dryRun: input.dryRun,
  });
  const industries = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: [],
    outsideBreakdowns: input.breakdowns.map((row) => ({ ...row, committeeId: row.spenderId })),
    classifications,
  }).outsideIndustryBreakdowns;
  const merged = new Map<string, NewYorkCityOutsideGroupBreakdown>();
  for (const row of industries) {
    const key = `${row.committeeId}\u0000${row.supportOppose}\u0000${row.categoryName}`;
    const existing = merged.get(key);
    if (!existing) {
      const sourceUrl = row.sourceUrl ?? input.breakdowns.find(
        (breakdown) => breakdown.spenderId === row.committeeId && breakdown.supportOppose === row.supportOppose
      )?.sourceUrl;
      if (!sourceUrl) throw new Error(`NYC outside industry breakdown missing source URL: ${key}`);
      merged.set(key, {
        spenderId: row.committeeId,
        supportOppose: row.supportOppose,
        categoryType: "industry",
        categoryName: row.categoryName,
        amount: row.amount,
        contributorCount: row.contributorCount,
        sourceUrl,
      });
    } else {
      existing.amount = Math.round((existing.amount + row.amount) * 100) / 100;
      existing.contributorCount =
        existing.contributorCount === null || row.contributorCount === null
          ? existing.contributorCount ?? row.contributorCount
          : existing.contributorCount + row.contributorCount;
    }
  }
  return [...input.breakdowns, ...merged.values()];
}

export async function syncNewYorkCityCandidateFinance(input: {
  db: Connectable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  resolution: MatchedResolution;
  contributionRows: readonly NewYorkCityCfbContributionRow[];
  outsideSpendingRows?: readonly NewYorkCityCfbIndependentSpendingRow[];
  outsideFunderRows?: readonly NewYorkCityCfbIndependentSpenderFunderRow[];
  outsideElectionCycle?: string;
  now?: Date;
  dryRun?: boolean;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
}): Promise<{
  dryRun: boolean;
  breakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideUpdated: boolean;
  acceptedContributionRows: number;
}> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) throw new Error("Invalid NYC finance sync timestamp");
  const outsideInputs = [input.outsideSpendingRows, input.outsideFunderRows, input.outsideElectionCycle];
  if (outsideInputs.some((value) => value === undefined) && outsideInputs.some((value) => value !== undefined)) {
    throw new Error("NYC outside spending rows, funder rows, and election cycle must be supplied together");
  }
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
  const outside = input.outsideSpendingRows && input.outsideFunderRows
    ? aggregateNewYorkCityOutsideSpending({
        rows: input.outsideSpendingRows,
        electionYear: input.electionYear,
        electionCycle: input.outsideElectionCycle!,
        candidateId: input.resolution.cfbCandidateId,
        sourceUrl: buildNewYorkCityCfbIndependentSpendingSourceUrl({
          electionYear: input.electionYear,
          electionCycle: input.outsideElectionCycle,
          candidateId: input.resolution.cfbCandidateId,
        }),
      })
    : null;
  const outsideBreakdowns = outside && input.outsideFunderRows
    ? await addOutsideIndustryBreakdowns({
        db: input.db,
        breakdowns: aggregateNewYorkCityOutsideGroupFunders({
          rows: input.outsideFunderRows,
          groups: outside.groups,
          electionYear: input.electionYear,
          electionCycle: input.outsideElectionCycle!,
        }),
        classifier: input.financeIndustryClassifier,
        minAmount: input.aiClassificationMinAmount ?? 25_000,
        dryRun: Boolean(input.dryRun),
      })
    : [];
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
      ...(outside
        ? { outside: {
            supportTotal: outside.supportTotal,
            opposeTotal: outside.opposeTotal,
            groups: outside.groups,
            breakdowns: outsideBreakdowns,
          } }
        : {}),
    });
  }
  return {
    dryRun: Boolean(input.dryRun),
    breakdownsWritten: input.dryRun ? 0 : breakdowns.length,
    outsideGroupsWritten: input.dryRun || !outside ? 0 : outside.groups.length,
    outsideUpdated: outside !== null,
    acceptedContributionRows: direct.acceptedRowCount,
  };
}
