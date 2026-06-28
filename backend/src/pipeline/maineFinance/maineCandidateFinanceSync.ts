import type { Pool, PoolClient } from "pg";

import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import { classifyFinanceLabel, type FinanceLabelClassification } from "../finance/financeLabelClassifier.js";
import {
  normalizeMaineCandidateNameForStorage,
  resolveMaineCandidateCommittee,
  type MaineCandidateCommitteeResolution,
} from "./maineCandidateCommitteeResolver.js";
import {
  aggregateMaineDirectContributions,
  type MaineDirectContributionAggregationResult,
} from "./maineDirectContributionAggregator.js";
import type { MaineCfisContributionRow, MaineCfisExpenditureRow } from "./maineCfisArtifactReader.js";
import { aggregateMaineOutsideGroupContributions } from "./maineOutsideGroupContributionAggregator.js";
import {
  aggregateMaineOutsideSpending,
  type MaineOutsideSpendingAggregationResult,
  type MaineOutsideSpendingGroup,
} from "./maineOutsideSpendingAggregator.js";
import {
  replaceMaineCandidateFinanceSnapshot,
  type MaineFinanceLinkInput,
  type MaineFinanceOutsideGroupBreakdownInput,
  type MaineFinanceOutsideGroupInput,
  type MaineFinanceSummaryInput,
} from "./maineFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type MaineCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  contributionRows: readonly MaineCfisContributionRow[];
  expenditureRows?: readonly MaineCfisExpenditureRow[];
  cfisCandidateId?: string | null;
  sourceUrl?: string | null;
  contributionSourceUrl?: string | null;
  expenditureSourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxBreakdownsPerCategory?: number;
  outsideMinIndustryAmount?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  trustedCommittee?: {
    committeeId: string;
    committeeName: string;
    sourceUrl?: string | null;
  };
};

export type MaineCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: MaineCandidateCommitteeResolution;
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

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
const STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT = 0;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Maine finance election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Maine finance sync timestamp");
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Maine finance AI classification minimum amount: ${value}`);
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
}): MaineFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeMaineCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    committeeId: requireNonEmpty(input.committeeId, "Maine committee id"),
    committeeName: requireNonEmpty(input.committeeName, "Maine committee name"),
    linkStatus: "active",
    linkSource: "cfis_bulk",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function aggregateDirect(input: {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly MaineCfisContributionRow[];
  contributionSourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
}): MaineDirectContributionAggregationResult {
  return aggregateMaineDirectContributions({
    committeeId: input.committeeId,
    electionYear: input.electionYear,
    contributionRows: input.contributionRows,
    sourceUrl: input.contributionSourceUrl ?? null,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
}

function aggregateOutside(input: {
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  cfisCandidateId?: string | null;
  expenditureRows: readonly MaineCfisExpenditureRow[];
  expenditureSourceUrl?: string | null;
  maxGroups?: number;
}): MaineOutsideSpendingAggregationResult {
  return aggregateMaineOutsideSpending({
    candidateName: input.candidateName,
    candidateId: input.cfisCandidateId,
    officeName: input.officeName,
    district: input.district,
    electionYear: input.electionYear,
    expenditureRows: input.expenditureRows,
    sourceUrl: input.expenditureSourceUrl ?? null,
    maxGroups: input.maxGroups,
  });
}

function outsideBreakdownKey(breakdown: MaineFinanceOutsideGroupBreakdownInput): string {
  return [
    breakdown.committeeId.trim().toUpperCase(),
    breakdown.supportOppose,
    breakdown.categoryType,
    breakdown.categoryName.trim().toUpperCase(),
  ].join("\u0000");
}

function toOutsideBreakdownMap(
  breakdowns: readonly MaineFinanceOutsideGroupBreakdownInput[] | undefined
): Map<string, MaineFinanceOutsideGroupBreakdownInput> {
  const mapped = new Map<string, MaineFinanceOutsideGroupBreakdownInput>();
  for (const breakdown of breakdowns ?? []) {
    if (breakdown.categoryType === "industry") {
      continue;
    }
    mapped.set(outsideBreakdownKey(breakdown), breakdown);
  }
  return mapped;
}

function collectOutsideClassifications(
  breakdowns: Iterable<MaineFinanceOutsideGroupBreakdownInput>,
  minIndustryAmount: number
): Map<string, FinanceLabelClassification> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor" || breakdown.amount < minIndustryAmount) {
      continue;
    }
    mergeFinanceLabelClassification(
      classifications,
      classifyFinanceLabel({
        rawLabel: breakdown.categoryName,
        labelType: "donor",
      })
    );
  }
  return classifications;
}

function addOutsideBreakdown(
  breakdowns: Map<string, MaineFinanceOutsideGroupBreakdownInput>,
  breakdown: MaineFinanceOutsideGroupBreakdownInput
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
  outsideGroupBreakdowns: readonly MaineFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  minIndustryAmount: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: MaineFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.outsideGroupBreakdowns) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const breakdowns = toOutsideBreakdownMap(input.outsideGroupBreakdowns);
  const classifications = collectOutsideClassifications(breakdowns.values(), input.minIndustryAmount);
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

function toSummary(input: {
  directFinance: MaineDirectContributionAggregationResult;
  outsideFinance: MaineOutsideSpendingAggregationResult;
  outsideDataAvailable: boolean;
  fallbackSourceUrl?: string | null;
}): MaineFinanceSummaryInput {
  return {
    totalReceipts: input.directFinance.summary.totalReceipts,
    directContributionTotal: input.directFinance.summary.directContributionTotal,
    outsideSupportTotal: input.outsideDataAvailable ? input.outsideFinance.summary?.supportTotal ?? 0 : null,
    outsideOpposeTotal: input.outsideDataAvailable ? input.outsideFinance.summary?.opposeTotal ?? 0 : null,
    sourceUrl:
      input.directFinance.summary.sourceUrl ??
      input.outsideFinance.summary?.sourceUrl ??
      input.fallbackSourceUrl ??
      null,
  };
}

function emptyOutsideResult(): MaineOutsideSpendingAggregationResult {
  return {
    summary: null,
    matchedExpenditureRowCount: 0,
    includedExpenditureRowCount: 0,
    skippedExpenditureRowCount: 0,
  };
}

function resolveTrustedCommittee(input: {
  committeeId: string;
  committeeName: string;
  sourceUrl?: string | null;
}): MaineCandidateCommitteeResolution {
  return {
    status: "matched",
    committeeId: requireNonEmpty(input.committeeId, "trusted Maine committee id"),
    committeeName: requireNonEmpty(input.committeeName, "trusted Maine committee name"),
    confidence: "exact",
    source: "cfis_bulk",
    sourceUrl: input.sourceUrl ?? null,
    matchedContributionRowCount: 0,
  };
}

export async function syncMaineCandidateFinance(
  input: MaineCandidateFinanceSyncInput
): Promise<MaineCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const aiClassificationMinAmount = normalizeAiClassificationMinAmount(input.aiClassificationMinAmount);
  const resolution = input.trustedCommittee
    ? resolveTrustedCommittee(input.trustedCommittee)
    : resolveMaineCandidateCommittee({
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
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: null,
      directContributionTotal: null,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    };
  }

  const directFinance = aggregateDirect({
    committeeId: resolution.committeeId,
    electionYear,
    contributionRows: input.contributionRows,
    contributionSourceUrl: input.contributionSourceUrl ?? resolution.sourceUrl,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory,
  });
  const outsideFinance =
    input.expenditureRows !== undefined
      ? aggregateOutside({
          candidateName,
          cfisCandidateId: input.cfisCandidateId,
          officeName,
          district: input.district,
          electionYear,
          expenditureRows: input.expenditureRows,
          expenditureSourceUrl: input.expenditureSourceUrl,
          maxGroups: input.outsideMaxGroups,
        })
      : emptyOutsideResult();
  const outsideDataAvailable = input.expenditureRows !== undefined;
  const outsideGroups: MaineOutsideSpendingGroup[] | undefined =
    input.expenditureRows !== undefined ? outsideFinance.summary?.groups ?? [] : undefined;
  const outsideGroupContributionFinance =
    outsideGroups !== undefined
      ? aggregateMaineOutsideGroupContributions({
          electionYear,
          outsideGroups,
          contributionRows: input.contributionRows,
          sourceUrl: input.contributionSourceUrl ?? null,
          maxBreakdownsPerCategory: input.outsideMaxBreakdownsPerCategory,
          minIndustryAmount: input.outsideMinIndustryAmount ?? STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT,
        })
      : {
          outsideGroupBreakdowns: undefined,
          matchedContributionRowCount: 0,
          includedContributionRowCount: 0,
          skippedContributionRowCount: 0,
        };
  const outsideIndustryFinance = await enrichOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns: outsideGroupContributionFinance.outsideGroupBreakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    minIndustryAmount: input.outsideMinIndustryAmount ?? STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT,
    dryRun: input.dryRun === true,
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
  const summary = toSummary({
    directFinance,
    outsideFinance,
    outsideDataAvailable,
    fallbackSourceUrl: input.sourceUrl ?? input.contributionSourceUrl ?? input.expenditureSourceUrl ?? null,
  });

  if (!input.dryRun) {
    await replaceMaineCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary,
      directBreakdowns: directFinance.directBreakdowns,
      outsideGroups,
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
    outsideGroupsWritten: input.dryRun ? 0 : outsideGroups?.length ?? 0,
    outsideGroupBreakdownsWritten: input.dryRun ? 0 : outsideIndustryFinance.outsideGroupBreakdowns?.length ?? 0,
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    outsideSupportTotal: outsideDataAvailable ? outsideFinance.summary?.supportTotal ?? 0 : null,
    outsideOpposeTotal: outsideDataAvailable ? outsideFinance.summary?.opposeTotal ?? 0 : null,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
    matchedExpenditureRowCount: outsideFinance.matchedExpenditureRowCount,
    includedExpenditureRowCount: outsideFinance.includedExpenditureRowCount,
    skippedExpenditureRowCount: outsideFinance.skippedExpenditureRowCount,
    matchedOutsideContributionRowCount: outsideGroupContributionFinance.matchedContributionRowCount,
    includedOutsideContributionRowCount: outsideGroupContributionFinance.includedContributionRowCount,
    skippedOutsideContributionRowCount: outsideGroupContributionFinance.skippedContributionRowCount,
  };
}
