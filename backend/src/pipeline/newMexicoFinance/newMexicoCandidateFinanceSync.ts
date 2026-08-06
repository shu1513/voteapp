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
  normalizeNewMexicoCandidateNameKeys,
  resolveNewMexicoCandidateCommittee,
  type NewMexicoCandidateCommitteeResolution,
} from "./newMexicoCandidateCommitteeResolver.js";
import {
  aggregateNewMexicoDirectContributions,
  type NewMexicoDirectContributionAggregationResult,
} from "./newMexicoDirectContributionAggregator.js";
import type {
  NewMexicoCfisContributionRow,
  NewMexicoCfisExpenditureRow,
} from "./newMexicoCfisArtifactReader.js";
import {
  aggregateNewMexicoOutsideSpending,
  type NewMexicoOutsideSpendingAggregationResult,
} from "./newMexicoOutsideSpendingAggregator.js";
import { aggregateNewMexicoOutsideGroupContributions } from "./newMexicoOutsideGroupContributionAggregator.js";
import {
  replaceNewMexicoCandidateFinanceSnapshot,
  type NewMexicoFinanceLinkInput,
  type NewMexicoFinanceOutsideGroupBreakdownInput,
  type NewMexicoFinanceOutsideGroupInput,
  type NewMexicoFinanceSummaryInput,
} from "./newMexicoFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type NewMexicoCandidateFinanceSyncInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  contributionRows: readonly NewMexicoCfisContributionRow[];
  expenditureRows?: readonly NewMexicoCfisExpenditureRow[];
  sourceUrl?: string | null;
  contributionSourceUrl?: string | null;
  expenditureSourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  // Display cap on persisted donor rows per (committee, direction);
  // classification always sees every donor.
  outsideMaxDonorBreakdownsPerGroup?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  trustedCommittee?: {
    committeeId: string;
    committeeName: string;
    sourceUrl?: string | null;
  };
};

export type NewMexicoCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: NewMexicoCandidateCommitteeResolution;
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
};

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
const STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT = 25_000;
// Display cap on PERSISTED donor rows per (committee, direction), applied
// AFTER classification so a >cap-donor group still gets industry totals built
// from every donor. Industry rows are naturally bounded by the slug set and
// are never capped.
const DEFAULT_MAX_DONOR_BREAKDOWNS_PER_GROUP = 50;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2020 || value > 2100) {
    throw new Error(`Invalid New Mexico finance election year: ${value}`);
  }
  return value;
}

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeNewMexicoCandidateNameKeys(value);
  return [...keys][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid New Mexico finance sync timestamp");
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid New Mexico finance AI classification minimum amount: ${value}`);
  }
  return normalized;
}

function normalizeMaxDonorBreakdowns(value: number | undefined): number {
  const normalized = value ?? DEFAULT_MAX_DONOR_BREAKDOWNS_PER_GROUP;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Mexico finance outsideMaxDonorBreakdownsPerGroup: ${value}`);
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
}): NewMexicoFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    committeeId: requireNonEmpty(input.committeeId, "New Mexico committee id"),
    committeeName: requireNonEmpty(input.committeeName, "New Mexico committee name"),
    linkStatus: "active",
    linkSource: "cfis_bulk",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function aggregateDirect(input: {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly NewMexicoCfisContributionRow[];
  contributionSourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
}): NewMexicoDirectContributionAggregationResult {
  return aggregateNewMexicoDirectContributions({
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
  expenditureRows: readonly NewMexicoCfisExpenditureRow[];
  expenditureSourceUrl?: string | null;
  maxGroups?: number;
}): NewMexicoOutsideSpendingAggregationResult {
  return aggregateNewMexicoOutsideSpending({
    candidateName: input.candidateName,
    electionYear: input.electionYear,
    expenditureRows: input.expenditureRows,
    sourceUrl: input.expenditureSourceUrl ?? null,
    maxGroups: input.maxGroups,
  });
}

function toOutsideGroups(
  outsideFinance: NewMexicoOutsideSpendingAggregationResult
): NewMexicoFinanceOutsideGroupInput[] | undefined {
  return outsideFinance.summary?.groups.map((group) => ({
    committeeId: group.committeeId,
    committeeName: group.committeeName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl,
  }));
}

function toOutsideGroupBreakdowns(input: {
  outsideGroups: readonly NewMexicoFinanceOutsideGroupInput[] | undefined;
  contributionRows: readonly NewMexicoCfisContributionRow[];
  contributionSourceUrl?: string | null;
  electionYear: number;
}): NewMexicoFinanceOutsideGroupBreakdownInput[] | undefined {
  if (!input.outsideGroups) {
    return undefined;
  }
  return aggregateNewMexicoOutsideGroupContributions({
    electionYear: input.electionYear,
    outsideGroups: input.outsideGroups,
    contributionRows: input.contributionRows,
    sourceUrl: input.contributionSourceUrl ?? null,
  }).outsideGroupBreakdowns;
}

function outsideBreakdownKey(breakdown: NewMexicoFinanceOutsideGroupBreakdownInput): string {
  return [
    breakdown.committeeId.trim().toUpperCase(),
    breakdown.supportOppose,
    breakdown.categoryType,
    breakdown.categoryName.trim().toUpperCase(),
  ].join("\u0000");
}

function toOutsideBreakdownMap(
  breakdowns: readonly NewMexicoFinanceOutsideGroupBreakdownInput[] | undefined
): Map<string, NewMexicoFinanceOutsideGroupBreakdownInput> {
  const mapped = new Map<string, NewMexicoFinanceOutsideGroupBreakdownInput>();
  for (const breakdown of breakdowns ?? []) {
    if (breakdown.categoryType === "industry") {
      continue;
    }
    mapped.set(outsideBreakdownKey(breakdown), breakdown);
  }
  return mapped;
}

function collectOutsideClassifications(
  breakdowns: Iterable<NewMexicoFinanceOutsideGroupBreakdownInput>
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
  breakdowns: Map<string, NewMexicoFinanceOutsideGroupBreakdownInput>,
  breakdown: NewMexicoFinanceOutsideGroupBreakdownInput
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

function capDonorBreakdowns(
  breakdowns: readonly NewMexicoFinanceOutsideGroupBreakdownInput[],
  maxDonorsPerGroup: number
): NewMexicoFinanceOutsideGroupBreakdownInput[] {
  const donorsByGroup = new Map<string, NewMexicoFinanceOutsideGroupBreakdownInput[]>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor") {
      continue;
    }
    const key = [breakdown.committeeId.trim().toUpperCase(), breakdown.supportOppose].join("\u0000");
    const list = donorsByGroup.get(key) ?? [];
    list.push(breakdown);
    donorsByGroup.set(key, list);
  }
  const kept = new Set<NewMexicoFinanceOutsideGroupBreakdownInput>();
  for (const list of donorsByGroup.values()) {
    for (const donor of list
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, maxDonorsPerGroup)) {
      kept.add(donor);
    }
  }
  return breakdowns.filter((breakdown) => breakdown.categoryType !== "donor" || kept.has(breakdown));
}

async function enrichOutsideGroupIndustryBreakdowns(input: {
  db: Queryable;
  outsideGroupBreakdowns: readonly NewMexicoFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  maxDonorBreakdownsPerGroup: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: NewMexicoFinanceOutsideGroupBreakdownInput[] | undefined;
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
    // Capped only HERE, after every donor fed the classifications and the
    // rebuilt industry rows above.
    outsideGroupBreakdowns: capDonorBreakdowns([...breakdowns.values()], input.maxDonorBreakdownsPerGroup),
    classifications: [...classifications.values()],
  };
}

function toSummary(input: {
  directFinance: NewMexicoDirectContributionAggregationResult;
  outsideFinance: NewMexicoOutsideSpendingAggregationResult;
  outsideDataAvailable: boolean;
  fallbackSourceUrl?: string | null;
}): NewMexicoFinanceSummaryInput {
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

function emptyOutsideResult(): NewMexicoOutsideSpendingAggregationResult {
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
}): NewMexicoCandidateCommitteeResolution {
  return {
    status: "matched",
    committeeId: requireNonEmpty(input.committeeId, "trusted New Mexico committee id"),
    committeeName: requireNonEmpty(input.committeeName, "trusted New Mexico committee name"),
    confidence: "exact",
    source: "cfis_bulk",
    sourceUrl: input.sourceUrl ?? null,
    matchedContributionRowCount: 0,
  };
}

export async function syncNewMexicoCandidateFinance(
  input: NewMexicoCandidateFinanceSyncInput
): Promise<NewMexicoCandidateFinanceSyncResult> {
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
    : resolveNewMexicoCandidateCommittee({
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
          electionYear,
          expenditureRows: input.expenditureRows,
          expenditureSourceUrl: input.expenditureSourceUrl,
          maxGroups: input.outsideMaxGroups,
        })
      : emptyOutsideResult();
  const outsideDataAvailable = input.expenditureRows !== undefined;
  const outsideGroups = input.expenditureRows !== undefined ? toOutsideGroups(outsideFinance) ?? [] : undefined;
  const outsideGroupBreakdowns = toOutsideGroupBreakdowns({
    outsideGroups,
    contributionRows: input.contributionRows,
    contributionSourceUrl: input.contributionSourceUrl,
    electionYear,
  });
  const outsideIndustryFinance = await enrichOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    maxDonorBreakdownsPerGroup: normalizeMaxDonorBreakdowns(input.outsideMaxDonorBreakdownsPerGroup),
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
    await replaceNewMexicoCandidateFinanceSnapshot({
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
  };
}
