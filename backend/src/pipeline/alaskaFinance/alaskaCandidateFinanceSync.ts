import type { Pool, PoolClient } from "pg";

import {
  ALASKA_APOC_CAMPAIGN_INCOME_URL,
  ALASKA_APOC_IE_CONTRIBUTIONS_URL,
  ALASKA_APOC_IE_EXPENDITURES_URL,
  type AlaskaApocCampaignIncomeRow,
  type AlaskaApocIndependentContributionRow,
  type AlaskaApocIndependentExpenditureRow,
} from "./alaskaApocClient.js";
import { aggregateAlaskaDirectContributions } from "./alaskaDirectContributionAggregator.js";
import { aggregateAlaskaOutsideGroupContributions } from "./alaskaOutsideGroupContributionAggregator.js";
import { aggregateAlaskaOutsideSpending } from "./alaskaOutsideSpendingAggregator.js";
import {
  replaceAlaskaCandidateFinanceSnapshot,
  type AlaskaFinanceDirectBreakdownInput,
  type AlaskaFinanceLinkInput,
  type AlaskaFinanceOutsideGroupBreakdownInput,
  type AlaskaFinanceOutsideGroupInput,
  type AlaskaFinanceSummaryInput,
} from "./alaskaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type AlaskaCandidateFinanceResolution = {
  status: "matched";
  candidateFilerId: string;
  candidateFilerName: string;
  source: "manual" | "apoc_csv";
  sourceUrl: string | null;
};

export type AlaskaCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  incomeRows: readonly AlaskaApocCampaignIncomeRow[];
  independentExpenditureRows?: readonly AlaskaApocIndependentExpenditureRow[];
  independentContributionRows?: readonly AlaskaApocIndependentContributionRow[];
  sourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxBreakdownsPerCategory?: number;
  outsideMinIndustryAmount?: number;
  trustedCommittee: {
    candidateFilerId: string;
    candidateFilerName: string;
    sourceUrl?: string | null;
  };
};

export type AlaskaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: AlaskaCandidateFinanceResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
  matchedOutsideContributionRowCount: number;
  includedOutsideContributionRowCount: number;
  skippedOutsideContributionRowCount: number;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Alaska finance election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Alaska finance sync timestamp");
  }
  return normalized;
}

function normalizeNonnegativeAmount(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Alaska finance ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeCandidateNameForStorage(value: string): string {
  return requireNonEmpty(value, "candidate name")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toResolution(input: AlaskaCandidateFinanceSyncInput["trustedCommittee"]): AlaskaCandidateFinanceResolution {
  return {
    status: "matched",
    candidateFilerId: requireNonEmpty(input.candidateFilerId, "trusted Alaska candidate filer id"),
    candidateFilerName: requireNonEmpty(input.candidateFilerName, "trusted Alaska candidate filer name"),
    source: "apoc_csv",
    sourceUrl: input.sourceUrl ?? null,
  };
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  resolution: AlaskaCandidateFinanceResolution;
  fallbackSourceUrl?: string | null;
  verifiedAt: Date;
}): AlaskaFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    candidateFilerId: input.resolution.candidateFilerId,
    candidateFilerName: input.resolution.candidateFilerName,
    linkStatus: "active",
    linkSource: input.resolution.source,
    sourceUrl: input.resolution.sourceUrl ?? input.fallbackSourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function toSummary(input: {
  directSummary: { totalReceipts: number; directContributionTotal: number; sourceUrl: string | null };
  outsideSummary: { supportTotal: number; opposeTotal: number; sourceUrl: string | null } | null;
  fallbackSourceUrl?: string | null;
}): AlaskaFinanceSummaryInput {
  return {
    totalReceipts: input.directSummary.totalReceipts,
    directContributionTotal: input.directSummary.directContributionTotal,
    outsideSupportTotal: input.outsideSummary?.supportTotal ?? 0,
    outsideOpposeTotal: input.outsideSummary?.opposeTotal ?? 0,
    sourceUrl: input.directSummary.sourceUrl ?? input.outsideSummary?.sourceUrl ?? input.fallbackSourceUrl ?? null,
  };
}

function toDirectBreakdowns(
  breakdowns: readonly AlaskaFinanceDirectBreakdownInput[]
): AlaskaFinanceDirectBreakdownInput[] {
  return breakdowns.map((breakdown) => ({
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount ?? null,
    sourceUrl: breakdown.sourceUrl ?? null,
  }));
}

function toOutsideGroups(
  groups: readonly { committeeId: string; committeeName: string; supportOppose: "support" | "oppose"; amount: number; sourceUrl: string | null }[]
): AlaskaFinanceOutsideGroupInput[] {
  return groups.map((group) => ({
    outsideGroupId: group.committeeId,
    outsideGroupName: group.committeeName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl,
  }));
}

function toOutsideGroupBreakdowns(
  breakdowns: readonly { committeeId: string; supportOppose: "support" | "oppose"; categoryType: "donor" | "industry"; categoryName: string; amount: number; contributorCount: number; sourceUrl: string | null }[]
): AlaskaFinanceOutsideGroupBreakdownInput[] {
  return breakdowns.map((breakdown) => ({
    outsideGroupId: breakdown.committeeId,
    supportOppose: breakdown.supportOppose,
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount,
    sourceUrl: breakdown.sourceUrl,
  }));
}

export async function syncAlaskaCandidateFinance(
  input: AlaskaCandidateFinanceSyncInput
): Promise<AlaskaCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const resolution = toResolution(input.trustedCommittee);
  const outsideMinIndustryAmount = normalizeNonnegativeAmount(
    input.outsideMinIndustryAmount,
    25_000,
    "outsideMinIndustryAmount"
  );

  const directFinance = aggregateAlaskaDirectContributions({
    candidateName,
    electionYear,
    candidateFilerId: resolution.candidateFilerId,
    candidateFilerName: resolution.candidateFilerName,
    incomeRows: input.incomeRows,
    sourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
  });
  const outsideFinance = aggregateAlaskaOutsideSpending({
    candidateName,
    electionYear,
    expenditureRows: input.independentExpenditureRows ?? [],
    sourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL,
    maxGroups: input.outsideMaxGroups ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
  });
  const outsideGroupBreakdownFinance = aggregateAlaskaOutsideGroupContributions({
    electionYear,
    outsideGroups: outsideFinance.summary?.groups ?? [],
    contributionRows: input.independentContributionRows ?? [],
    sourceUrl: ALASKA_APOC_IE_CONTRIBUTIONS_URL,
    maxBreakdownsPerCategory: input.outsideMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    minIndustryAmount: outsideMinIndustryAmount,
  });

  const summary = toSummary({
    directSummary: directFinance.summary,
    outsideSummary: outsideFinance.summary,
    fallbackSourceUrl: input.sourceUrl,
  });
  const directBreakdowns = toDirectBreakdowns(directFinance.directBreakdowns);
  const outsideGroups = toOutsideGroups(outsideFinance.summary?.groups ?? []);
  const outsideGroupBreakdowns = toOutsideGroupBreakdowns(outsideGroupBreakdownFinance.outsideGroupBreakdowns);
  const link = toFinanceLink({
    candidateId,
    electionId,
    candidateName,
    electionYear,
    officeName,
    district: input.district,
    resolution,
    fallbackSourceUrl: input.sourceUrl,
    verifiedAt: syncedAt,
  });

  if (!dryRun) {
    await replaceAlaskaCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary,
      directBreakdowns,
      outsideGroups,
      outsideGroupBreakdowns,
    });
  }

  return {
    candidateId,
    electionId,
    electionYear,
    dryRun,
    resolution,
    linkWritten: !dryRun,
    summaryWritten: !dryRun,
    directBreakdownsWritten: dryRun ? 0 : directBreakdowns.length,
    outsideGroupsWritten: dryRun ? 0 : outsideGroups.length,
    outsideGroupBreakdownsWritten: dryRun ? 0 : outsideGroupBreakdowns.length,
    totalReceipts: summary.totalReceipts ?? null,
    directContributionTotal: summary.directContributionTotal ?? null,
    outsideSupportTotal: summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? null,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
    matchedExpenditureRowCount: outsideFinance.matchedExpenditureRowCount,
    includedExpenditureRowCount: outsideFinance.includedExpenditureRowCount,
    skippedExpenditureRowCount: outsideFinance.skippedExpenditureRowCount,
    matchedOutsideContributionRowCount: outsideGroupBreakdownFinance.matchedContributionRowCount,
    includedOutsideContributionRowCount: outsideGroupBreakdownFinance.includedContributionRowCount,
    skippedOutsideContributionRowCount: outsideGroupBreakdownFinance.skippedContributionRowCount,
  };
}
