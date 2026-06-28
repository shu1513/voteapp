import type { Pool, PoolClient } from "pg";

import {
  normalizeNewJerseyCandidateNameKeys,
  resolveNewJerseyCandidateCommittee,
  type NewJerseyCandidateCommitteeResolution,
} from "./newJerseyCandidateCommitteeResolver.js";
import {
  aggregateNewJerseyDirectContributions,
  type NewJerseyDirectContributionAggregationResult,
} from "./newJerseyDirectContributionAggregator.js";
import {
  getNewJerseyElecContributionRows,
  searchNewJerseyElecEntities,
  type NewJerseyElecClientOptions,
  type NewJerseyElecContributionRowsResult,
  type NewJerseyElecContributionRow,
  type NewJerseyElecEntity,
} from "./newJerseyElecClient.js";
import {
  aggregateNewJerseyOutsideSpending,
  type NewJerseyOutsideSpendingGroup,
  type NewJerseyOutsideSpendingReportText,
} from "./newJerseyOutsideSpendingAggregator.js";
import {
  aggregateNewJerseyOutsideGroupContributions,
  type NewJerseyFinanceOutsideGroupBreakdown,
} from "./newJerseyOutsideGroupContributionAggregator.js";
import {
  replaceNewJerseyCandidateFinanceSnapshot,
  type NewJerseyFinanceDirectBreakdownInput,
  type NewJerseyFinanceLinkInput,
  type NewJerseyFinanceOutsideGroupBreakdownInput,
  type NewJerseyFinanceOutsideGroupInput,
  type NewJerseyFinanceSummaryInput,
} from "./newJerseyFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type NewJerseyCandidateFinanceSyncOutsideGroupInput = {
  entityS: number;
  entityName: string;
  reportTexts: readonly NewJerseyOutsideSpendingReportText[];
  contributions: readonly NewJerseyElecContributionRow[];
  sourceUrl?: string | null;
};

export type NewJerseyCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  candidateEntityS: number;
  candidateEntityName: string;
  electionTypeCode?: string | null;
  sourceUrl?: string | null;
  contributions: readonly NewJerseyElecContributionRow[];
  contributionSourceUrl?: string | null;
  outsideGroups?: readonly NewJerseyCandidateFinanceSyncOutsideGroupInput[];
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxBreakdownsPerCategory?: number;
  outsideMinIndustryAmount?: number;
};

export type NewJerseyCandidateFinanceElecSyncInput = Omit<
  NewJerseyCandidateFinanceSyncInput,
  "candidateEntityS" | "candidateEntityName" | "contributions" | "contributionSourceUrl"
> & {
  officeScope: string;
  locationCode?: number | string | null;
  entityRows?: readonly NewJerseyElecEntity[];
  clientOptions?: NewJerseyElecClientOptions;
  elecClient?: Partial<{
    searchEntities: typeof searchNewJerseyElecEntities;
    getContributionRows: typeof getNewJerseyElecContributionRows;
  }>;
};

export type NewJerseyCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number;
  directContributionTotal: number;
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
  matchedAllocationRowCount: number;
  includedAllocationRowCount: number;
  skippedAllocationRowCount: number;
  matchedOutsideContributionRowCount: number;
  includedOutsideContributionRowCount: number;
  skippedOutsideContributionRowCount: number;
  outsideReportTextCount: number;
};

export type NewJerseyCandidateFinanceElecSyncResult =
  | {
      status: "matched";
      resolution: Extract<NewJerseyCandidateCommitteeResolution, { status: "matched" }>;
      contributionRowsResult: NewJerseyElecContributionRowsResult;
      syncResult: NewJerseyCandidateFinanceSyncResult;
    }
  | {
      status: "unmatched" | "ambiguous";
      resolution: Exclude<NewJerseyCandidateCommitteeResolution, { status: "matched" }>;
      contributionRowsResult: null;
      syncResult: null;
    };

type OutsideAggregationResult = {
  groups: NewJerseyOutsideSpendingGroup[];
  breakdowns: NewJerseyFinanceOutsideGroupBreakdown[];
  supportTotal: number;
  opposeTotal: number;
  matchedAllocationRowCount: number;
  includedAllocationRowCount: number;
  skippedAllocationRowCount: number;
  matchedOutsideContributionRowCount: number;
  includedOutsideContributionRowCount: number;
  skippedOutsideContributionRowCount: number;
  outsideReportTextCount: number;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1980 || value > 2100) {
    throw new Error(`Invalid New Jersey finance election year: ${value}`);
  }
  return value;
}

function normalizeEntityS(value: number, fieldName: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${fieldName} must be a positive integer`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid New Jersey finance sync timestamp");
  }
  return normalized;
}

function normalizeCandidateNameForStorage(value: string): string {
  return [...normalizeNewJerseyCandidateNameKeys(value)][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function candidateLastNameSearchTerm(candidateName: string): string | null {
  const firstKey = [...normalizeNewJerseyCandidateNameKeys(candidateName)][0];
  const parts = firstKey?.split(" ").filter(Boolean) ?? [];
  return parts.at(-1) ?? null;
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  candidateEntityS: number;
  candidateEntityName: string;
  electionTypeCode?: string | null;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): NewJerseyFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    candidateEntityS: normalizeEntityS(input.candidateEntityS, "New Jersey candidate ENTITY_S"),
    entityName: requireNonEmpty(input.candidateEntityName, "New Jersey candidate entity name"),
    electionTypeCode: input.electionTypeCode ?? null,
    linkStatus: "active",
    linkSource: "elec_api",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function toDirectBreakdowns(
  breakdowns: readonly NewJerseyFinanceDirectBreakdownInput[]
): NewJerseyFinanceDirectBreakdownInput[] {
  return breakdowns.map((breakdown) => ({
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount ?? null,
    sourceUrl: breakdown.sourceUrl ?? null,
  }));
}

function toOutsideGroups(groups: readonly NewJerseyOutsideSpendingGroup[]): NewJerseyFinanceOutsideGroupInput[] {
  return groups.map((group) => ({
    outsideEntityS: group.entityS,
    outsideEntityName: group.entityName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl ?? null,
  }));
}

function toOutsideGroupBreakdowns(
  breakdowns: readonly NewJerseyFinanceOutsideGroupBreakdown[]
): NewJerseyFinanceOutsideGroupBreakdownInput[] {
  return breakdowns.map((breakdown) => ({
    outsideEntityS: breakdown.entityS,
    supportOppose: breakdown.supportOppose,
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount,
    sourceUrl: breakdown.sourceUrl ?? null,
  }));
}

function aggregateDirect(input: {
  candidateEntityS: number;
  electionYear: number;
  contributions: readonly NewJerseyElecContributionRow[];
  contributionSourceUrl: string | null | undefined;
  linkSourceUrl: string | null | undefined;
  maxBreakdownsPerCategory: number | undefined;
}): NewJerseyDirectContributionAggregationResult {
  return aggregateNewJerseyDirectContributions({
    entityS: input.candidateEntityS,
    electionYear: input.electionYear,
    contributions: input.contributions,
    sourceUrl: input.contributionSourceUrl ?? input.linkSourceUrl ?? null,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
}

function aggregateOutside(input: {
  candidateName: string;
  electionYear: number;
  outsideGroups: readonly NewJerseyCandidateFinanceSyncOutsideGroupInput[] | undefined;
  maxGroups: number | undefined;
  maxBreakdownsPerCategory: number | undefined;
  minIndustryAmount: number | undefined;
}): OutsideAggregationResult {
  const groups: NewJerseyOutsideSpendingGroup[] = [];
  const breakdowns: NewJerseyFinanceOutsideGroupBreakdown[] = [];
  let supportTotal = 0;
  let opposeTotal = 0;
  let matchedAllocationRowCount = 0;
  let includedAllocationRowCount = 0;
  let skippedAllocationRowCount = 0;
  let matchedOutsideContributionRowCount = 0;
  let includedOutsideContributionRowCount = 0;
  let skippedOutsideContributionRowCount = 0;
  let outsideReportTextCount = 0;

  for (const outsideGroup of input.outsideGroups ?? []) {
    const outsideSpending = aggregateNewJerseyOutsideSpending({
      candidateName: input.candidateName,
      electionYear: input.electionYear,
      outsideGroupEntityS: outsideGroup.entityS,
      outsideGroupName: outsideGroup.entityName,
      reportTexts: outsideGroup.reportTexts,
      sourceUrl: outsideGroup.sourceUrl,
      maxGroups: input.maxGroups,
    });
    outsideReportTextCount += outsideGroup.reportTexts.length;
    matchedAllocationRowCount += outsideSpending.matchedAllocationRowCount;
    includedAllocationRowCount += outsideSpending.includedAllocationRowCount;
    skippedAllocationRowCount += outsideSpending.skippedAllocationRowCount;
    supportTotal += outsideSpending.summary?.supportTotal ?? 0;
    opposeTotal += outsideSpending.summary?.opposeTotal ?? 0;
    groups.push(...(outsideSpending.summary?.groups ?? []));

    const outsideGroupContributions = aggregateNewJerseyOutsideGroupContributions({
      electionYear: input.electionYear,
      outsideGroups: outsideSpending.summary?.groups ?? [],
      contributions: outsideGroup.contributions,
      sourceUrl: outsideGroup.sourceUrl,
      maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
      minIndustryAmount: input.minIndustryAmount,
    });
    matchedOutsideContributionRowCount += outsideGroupContributions.matchedContributionRowCount;
    includedOutsideContributionRowCount += outsideGroupContributions.includedContributionRowCount;
    skippedOutsideContributionRowCount += outsideGroupContributions.skippedContributionRowCount;
    breakdowns.push(...outsideGroupContributions.outsideGroupBreakdowns);
  }

  return {
    groups,
    breakdowns,
    supportTotal: Math.round(supportTotal * 100) / 100,
    opposeTotal: Math.round(opposeTotal * 100) / 100,
    matchedAllocationRowCount,
    includedAllocationRowCount,
    skippedAllocationRowCount,
    matchedOutsideContributionRowCount,
    includedOutsideContributionRowCount,
    skippedOutsideContributionRowCount,
    outsideReportTextCount,
  };
}

function toSummary(input: {
  directSummary: { totalReceipts: number; directContributionTotal: number; sourceUrl: string | null };
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  fallbackSourceUrl?: string | null;
}): NewJerseyFinanceSummaryInput {
  return {
    totalReceipts: input.directSummary.totalReceipts,
    directContributionTotal: input.directSummary.directContributionTotal,
    outsideSupportTotal: input.outsideSupportTotal,
    outsideOpposeTotal: input.outsideOpposeTotal,
    sourceUrl: input.directSummary.sourceUrl ?? input.fallbackSourceUrl ?? null,
  };
}

export async function syncNewJerseyCandidateFinance(
  input: NewJerseyCandidateFinanceSyncInput
): Promise<NewJerseyCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const candidateEntityS = normalizeEntityS(input.candidateEntityS, "New Jersey candidate ENTITY_S");
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const maxBreakdownsPerCategory = input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY;

  const directFinance = aggregateDirect({
    candidateEntityS,
    electionYear,
    contributions: input.contributions,
    contributionSourceUrl: input.contributionSourceUrl,
    linkSourceUrl: input.sourceUrl,
    maxBreakdownsPerCategory,
  });
  const outsideFinance = aggregateOutside({
    candidateName,
    electionYear,
    outsideGroups: input.outsideGroups,
    maxGroups: input.outsideMaxGroups ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    maxBreakdownsPerCategory: input.outsideMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    minIndustryAmount: input.outsideMinIndustryAmount,
  });
  const link = toFinanceLink({
    candidateId,
    electionId,
    candidateName,
    electionYear,
    officeName,
    district: input.district,
    candidateEntityS,
    candidateEntityName: input.candidateEntityName,
    electionTypeCode: input.electionTypeCode,
    sourceUrl: input.sourceUrl,
    verifiedAt: syncedAt,
  });
  const summary = toSummary({
    directSummary: directFinance.summary,
    outsideSupportTotal: outsideFinance.supportTotal,
    outsideOpposeTotal: outsideFinance.opposeTotal,
    fallbackSourceUrl: input.sourceUrl,
  });
  const directBreakdowns = toDirectBreakdowns(directFinance.directBreakdowns);
  const outsideGroups = toOutsideGroups(outsideFinance.groups);
  const outsideGroupBreakdowns = toOutsideGroupBreakdowns(outsideFinance.breakdowns);

  if (!dryRun) {
    await replaceNewJerseyCandidateFinanceSnapshot({
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
    linkWritten: !dryRun,
    summaryWritten: !dryRun,
    directBreakdownsWritten: dryRun ? 0 : directBreakdowns.length,
    outsideGroupsWritten: dryRun ? 0 : outsideGroups.length,
    outsideGroupBreakdownsWritten: dryRun ? 0 : outsideGroupBreakdowns.length,
    totalReceipts: summary.totalReceipts ?? 0,
    directContributionTotal: summary.directContributionTotal ?? 0,
    outsideSupportTotal: summary.outsideSupportTotal ?? 0,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? 0,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
    matchedAllocationRowCount: outsideFinance.matchedAllocationRowCount,
    includedAllocationRowCount: outsideFinance.includedAllocationRowCount,
    skippedAllocationRowCount: outsideFinance.skippedAllocationRowCount,
    matchedOutsideContributionRowCount: outsideFinance.matchedOutsideContributionRowCount,
    includedOutsideContributionRowCount: outsideFinance.includedOutsideContributionRowCount,
    skippedOutsideContributionRowCount: outsideFinance.skippedOutsideContributionRowCount,
    outsideReportTextCount: outsideFinance.outsideReportTextCount,
  };
}

export async function syncNewJerseyCandidateFinanceFromElec(
  input: NewJerseyCandidateFinanceElecSyncInput
): Promise<NewJerseyCandidateFinanceElecSyncResult> {
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const searchEntities = input.elecClient?.searchEntities ?? searchNewJerseyElecEntities;
  const getContributionRows = input.elecClient?.getContributionRows ?? getNewJerseyElecContributionRows;
  const lastName = candidateLastNameSearchTerm(candidateName);
  const entityRows = input.entityRows ?? (lastName ? await searchEntities({ lastName, nonPacOnly: true }, input.clientOptions) : []);
  const resolution = resolveNewJerseyCandidateCommittee({
    candidateName,
    officeScope: requireNonEmpty(input.officeScope, "office scope"),
    officeName: input.officeName,
    electionYear,
    electionTypeCode: input.electionTypeCode,
    locationCode: input.locationCode,
    entityRows,
  });

  if (resolution.status !== "matched") {
    return {
      status: resolution.status,
      resolution,
      contributionRowsResult: null,
      syncResult: null,
    };
  }

  const contributionRowsResult = await getContributionRows(
    {
      entityS: resolution.entityS,
      electionYear,
      firstName: resolution.firstName,
      lastName: resolution.lastName,
      officeCode: resolution.officeCode,
      partyCode: resolution.partyCode,
      locationCode: input.locationCode ?? resolution.locationCode,
      electionTypeCode: input.electionTypeCode ?? resolution.electionTypeCode,
      nonPacOnly: true,
    },
    input.clientOptions
  );

  const syncResult = await syncNewJerseyCandidateFinance({
    ...input,
    candidateEntityS: resolution.entityS,
    candidateEntityName: resolution.entityName,
    sourceUrl: input.sourceUrl ?? resolution.sourceUrl,
    contributions: contributionRowsResult.rows,
    contributionSourceUrl: contributionRowsResult.sourceUrl,
  });

  return {
    status: "matched",
    resolution,
    contributionRowsResult,
    syncResult,
  };
}
