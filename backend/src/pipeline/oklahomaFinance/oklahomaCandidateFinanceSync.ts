import type { Pool, PoolClient } from "pg";

import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import {
  classifyFinanceLabel,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";
import {
  normalizeOklahomaCandidateNameKeys,
  resolveOklahomaCandidateCommittee,
  type OklahomaCandidateCommitteeResolution,
} from "./oklahomaCandidateCommitteeResolver.js";
import {
  aggregateOklahomaDirectContributions,
  type OklahomaDirectContributionAggregationResult,
} from "./oklahomaDirectContributionAggregator.js";
import {
  replaceOklahomaCandidateFinanceSnapshot,
  type OklahomaFinanceLinkInput,
  type OklahomaFinanceOutsideGroupBreakdownInput,
  type OklahomaFinanceOutsideGroupInput,
  type OklahomaFinanceSummaryInput,
} from "./oklahomaFinanceWriter.js";
import { buildOklahomaGuardianIeOutsideFinanceSnapshot } from "./oklahomaGuardianIeOutsideSpendingAggregator.js";
import {
  discoverOklahomaGuardianIeOutsideSpendingReports,
  type OklahomaGuardianIeOutsideSpendingDiscoveryResult,
} from "./oklahomaGuardianIeOutsideSpendingDiscovery.js";
import type { OklahomaGuardianContributionRow } from "./oklahomaGuardianContributionReader.js";
import { aggregateOklahomaOutsideGroupContributions } from "./oklahomaOutsideGroupContributionAggregator.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type OklahomaOutsideSpendingDiscoveryFn = typeof discoverOklahomaGuardianIeOutsideSpendingReports;

export type OklahomaCandidateFinanceSyncInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  contributionRows: readonly OklahomaGuardianContributionRow[];
  outsideContributionRows?: readonly OklahomaGuardianContributionRow[];
  sourceUrl?: string | null;
  contributionSourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  includeOutsideSpending?: boolean;
  outsideMaxReports?: number;
  discoverOutsideSpendingReportsFn?: OklahomaOutsideSpendingDiscoveryFn;
  outsideDiscoveryResult?: OklahomaGuardianIeOutsideSpendingDiscoveryResult | null;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
};

export type OklahomaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: OklahomaCandidateCommitteeResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideIncluded: boolean;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  outsideReportsExamined: number;
  outsideUsableReports: number;
  outsideSkippedReports: number;
  outsideMatchedContributionRowCount: number;
  outsideIncludedContributionRowCount: number;
  outsideSkippedContributionRowCount: number;
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
const STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT = 25_000;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2014 || value > 2100) {
    throw new Error(`Invalid Oklahoma finance election year: ${value}`);
  }
  return value;
}

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeOklahomaCandidateNameKeys(value);
  return [...keys][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Oklahoma finance sync timestamp");
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Oklahoma finance AI classification minimum amount: ${value}`);
  }
  return normalized;
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  committeeId: string;
  committeeName: string;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): OklahomaFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    committeeId: requireNonEmpty(input.committeeId, "Oklahoma committee id"),
    committeeName: requireNonEmpty(input.committeeName, "Oklahoma committee name"),
    linkStatus: "active",
    linkSource: "guardian_bulk",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function aggregateDirect(input: {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly OklahomaGuardianContributionRow[];
  contributionSourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
}): OklahomaDirectContributionAggregationResult {
  return aggregateOklahomaDirectContributions({
    committeeId: input.committeeId,
    electionYear: input.electionYear,
    contributionRows: input.contributionRows,
    sourceUrl: input.contributionSourceUrl ?? null,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
}

function mergeSummaries(input: {
  directSummary: OklahomaFinanceSummaryInput;
  outsideSummary?: OklahomaFinanceSummaryInput;
}): OklahomaFinanceSummaryInput {
  if (!input.outsideSummary) {
    return input.directSummary;
  }
  return {
    totalReceipts: input.directSummary.totalReceipts,
    directContributionTotal: input.directSummary.directContributionTotal,
    outsideSupportTotal: input.outsideSummary.outsideSupportTotal,
    outsideOpposeTotal: input.outsideSummary.outsideOpposeTotal,
    sourceUrl: input.directSummary.sourceUrl ?? input.outsideSummary.sourceUrl ?? null,
  };
}

async function discoverOutsideSpending(input: {
  candidateName: string;
  electionYear: number;
  maxReports?: number;
  discoverFn: OklahomaOutsideSpendingDiscoveryFn;
}): Promise<OklahomaGuardianIeOutsideSpendingDiscoveryResult> {
  return await input.discoverFn({
    candidateName: input.candidateName,
    electionYear: input.electionYear,
    maxReports: input.maxReports,
  });
}

function outsideBreakdownKey(breakdown: OklahomaFinanceOutsideGroupBreakdownInput): string {
  return [
    breakdown.committeeId.trim().toUpperCase(),
    breakdown.supportOppose,
    breakdown.categoryType,
    breakdown.categoryName.trim().toUpperCase(),
  ].join("\u0000");
}

function toOutsideBreakdownMap(
  breakdowns: readonly OklahomaFinanceOutsideGroupBreakdownInput[] | undefined
): Map<string, OklahomaFinanceOutsideGroupBreakdownInput> {
  const mapped = new Map<string, OklahomaFinanceOutsideGroupBreakdownInput>();
  for (const breakdown of breakdowns ?? []) {
    if (breakdown.categoryType === "industry") {
      continue;
    }
    mapped.set(outsideBreakdownKey(breakdown), breakdown);
  }
  return mapped;
}

function collectOutsideClassifications(
  breakdowns: Iterable<OklahomaFinanceOutsideGroupBreakdownInput>
): Map<string, FinanceLabelClassification> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor") {
      continue;
    }
    if (breakdown.amount < STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT) {
      continue;
    }
    const classification = classifyFinanceLabel({
      rawLabel: breakdown.categoryName,
      labelType: "donor",
    });
    mergeFinanceLabelClassification(classifications, classification);
  }
  return classifications;
}

function addOutsideBreakdown(
  breakdowns: Map<string, OklahomaFinanceOutsideGroupBreakdownInput>,
  breakdown: OklahomaFinanceOutsideGroupBreakdownInput
): void {
  const key = outsideBreakdownKey(breakdown);
  const existing = breakdowns.get(key);
  if (!existing) {
    breakdowns.set(key, breakdown);
    return;
  }
  breakdowns.set(key, {
    ...existing,
    amount: Math.round((existing.amount + breakdown.amount) * 100) / 100,
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

async function enrichOutsideGroupIndustryBreakdowns(input: {
  db: Queryable;
  outsideGroupBreakdowns: readonly OklahomaFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: OklahomaFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.outsideGroupBreakdowns) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const breakdowns = toOutsideBreakdownMap(input.outsideGroupBreakdowns);
  const classifications = collectOutsideClassifications(breakdowns.values());
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: [],
    outsideBreakdowns: breakdowns.values(),
    classifications,
    classifier: input.classifier,
    minAmount: input.aiClassificationMinAmount,
    dryRun: input.dryRun,
  });

  const industryBreakdowns = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: [],
    outsideBreakdowns: breakdowns.values(),
    classifications,
  });
  for (const breakdown of industryBreakdowns.outsideIndustryBreakdowns) {
    addOutsideBreakdown(breakdowns, breakdown);
  }

  return {
    outsideGroupBreakdowns: [...breakdowns.values()],
    classifications: [...classifications.values()],
  };
}

function buildOutsideGroupBreakdowns(input: {
  outsideGroups: readonly OklahomaFinanceOutsideGroupInput[] | undefined;
  contributionRows: readonly OklahomaGuardianContributionRow[];
  contributionSourceUrl?: string | null;
  electionYear: number;
}): ReturnType<typeof aggregateOklahomaOutsideGroupContributions> | null {
  if (!input.outsideGroups) {
    return null;
  }
  return aggregateOklahomaOutsideGroupContributions({
    electionYear: input.electionYear,
    outsideGroups: input.outsideGroups,
    contributionRows: input.contributionRows,
    sourceUrl: input.contributionSourceUrl ?? null,
  });
}

export async function syncOklahomaCandidateFinance(
  input: OklahomaCandidateFinanceSyncInput
): Promise<OklahomaCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const aiClassificationMinAmount = normalizeAiClassificationMinAmount(input.aiClassificationMinAmount);
  const resolution = resolveOklahomaCandidateCommittee({
    candidateName,
    officeScope,
    officeName,
    electionYear,
    district: input.district,
    contributionRows: input.contributionRows,
    sourceUrl: input.sourceUrl ?? input.contributionSourceUrl ?? null,
  });

  if (resolution.status !== "matched") {
    return {
      candidateId,
      electionId,
      electionYear,
      dryRun: input.dryRun === true,
      resolution,
      linkWritten: false,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      outsideIncluded: input.includeOutsideSpending === true,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: null,
      directContributionTotal: null,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      outsideReportsExamined: 0,
      outsideUsableReports: 0,
      outsideSkippedReports: 0,
      outsideMatchedContributionRowCount: 0,
      outsideIncludedContributionRowCount: 0,
      outsideSkippedContributionRowCount: 0,
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    };
  }

  const directFinance = aggregateDirect({
    committeeId: resolution.committeeId,
    electionYear,
    contributionRows: input.contributionRows,
    contributionSourceUrl: input.contributionSourceUrl ?? resolution.sourceUrl,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory,
  });
  const link = toFinanceLink({
    candidateId,
    electionId,
    candidateName,
    electionYear,
    officeName,
    district: input.district,
    committeeId: resolution.committeeId,
    committeeName: resolution.committeeName,
    sourceUrl: resolution.sourceUrl ?? input.sourceUrl ?? input.contributionSourceUrl ?? null,
    verifiedAt: syncedAt,
  });
  const outsideIncluded = input.includeOutsideSpending === true;
  const outsideDiscovery = outsideIncluded
    ? input.outsideDiscoveryResult ??
      (await discoverOutsideSpending({
        candidateName,
        electionYear,
        maxReports: input.outsideMaxReports,
        discoverFn: input.discoverOutsideSpendingReportsFn ?? discoverOklahomaGuardianIeOutsideSpendingReports,
      }))
    : null;
  const outsideFinance = outsideDiscovery
    ? buildOklahomaGuardianIeOutsideFinanceSnapshot(outsideDiscovery.usableReports)
    : null;
  const outsideContributionFinance = buildOutsideGroupBreakdowns({
    outsideGroups: outsideFinance?.outsideGroups,
    contributionRows: input.outsideContributionRows ?? input.contributionRows,
    contributionSourceUrl: input.contributionSourceUrl,
    electionYear,
  });
  const outsideIndustryFinance = await enrichOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns: outsideContributionFinance?.outsideGroupBreakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    dryRun: input.dryRun === true,
  });

  if (!input.dryRun) {
    await replaceOklahomaCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary: mergeSummaries({
        directSummary: directFinance.summary,
        outsideSummary: outsideFinance?.summary,
      }),
      directBreakdowns: directFinance.directBreakdowns,
      outsideGroups: outsideFinance?.outsideGroups,
      outsideGroupBreakdowns: outsideIndustryFinance.outsideGroupBreakdowns,
      classifications: outsideIndustryFinance.classifications,
    });
  }

  return {
    candidateId,
    electionId,
    electionYear,
    dryRun: input.dryRun === true,
    resolution,
    linkWritten: !input.dryRun,
    summaryWritten: !input.dryRun,
    directBreakdownsWritten: input.dryRun ? 0 : directFinance.directBreakdowns.length,
    outsideIncluded,
    outsideGroupsWritten: input.dryRun ? 0 : (outsideFinance?.outsideGroups.length ?? 0),
    outsideGroupBreakdownsWritten: input.dryRun ? 0 : (outsideIndustryFinance.outsideGroupBreakdowns?.length ?? 0),
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    outsideSupportTotal: outsideFinance?.summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: outsideFinance?.summary.outsideOpposeTotal ?? null,
    outsideReportsExamined: outsideDiscovery?.reportsExamined ?? 0,
    outsideUsableReports: outsideDiscovery?.usableReports.length ?? 0,
    outsideSkippedReports: outsideDiscovery?.skippedReports.length ?? 0,
    outsideMatchedContributionRowCount: outsideContributionFinance?.matchedContributionRowCount ?? 0,
    outsideIncludedContributionRowCount: outsideContributionFinance?.includedContributionRowCount ?? 0,
    outsideSkippedContributionRowCount: outsideContributionFinance?.skippedContributionRowCount ?? 0,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
  };
}
