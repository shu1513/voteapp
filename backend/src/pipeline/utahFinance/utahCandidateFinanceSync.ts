import type { Pool, PoolClient } from "pg";

import {
  normalizeUtahCandidateNameKeys,
  resolveUtahCandidateCommittee,
  type UtahCandidateCommitteeResolution,
} from "./utahCandidateCommitteeResolver.js";
import {
  aggregateUtahDirectContributions,
  type UtahDirectContributionAggregationResult,
} from "./utahDirectContributionAggregator.js";
import type {
  UtahDisclosuresEntitySearchRow,
  UtahDisclosuresTransactionRow,
} from "./utahDisclosuresClient.js";
import {
  replaceUtahCandidateFinanceSnapshot,
  type UtahFinanceLinkInput,
} from "./utahFinanceWriter.js";
import {
  aggregateUtahSupportingCommitteeIndustries,
  type UtahSupportingCommitteeIndustryAggregationResult,
} from "./utahSupportingCommitteeIndustryAggregator.js";
import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type UtahCandidateFinanceSyncInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  entityRows: readonly UtahDisclosuresEntitySearchRow[];
  transactions: readonly UtahDisclosuresTransactionRow[];
  trustedCommittee?: {
    folderId: string;
    committeeName: string;
    reportYears?: readonly number[];
    sourceUrl?: string | null;
  };
  supportingCommitteeTransactions?: readonly UtahDisclosuresTransactionRow[];
  sourceUrl?: string | null;
  transactionSourceUrl?: string | null;
  supportingCommitteeSourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  supportingMaxCommittees?: number;
  supportingMaxIndustriesPerCommittee?: number;
  supportingCommitteeIndustryMinAmount?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  classifySupportingCommitteeIndustriesWithAi?: boolean;
};

export type UtahCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: UtahCandidateCommitteeResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  matchedTransactionRowCount: number;
  includedContributionRowCount: number;
  skippedTransactionRowCount: number;
  supportingCommitteeCount: number;
  supportingCommitteeIndustryCount: number;
  supportingCommitteeMatchedTransactionRowCount: number;
  supportingCommitteeIncludedOrganizationDonorRowCount: number;
  supportingCommitteeSkippedTransactionRowCount: number;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1998 || value > 2100) {
    throw new Error(`Invalid Utah finance election year: ${value}`);
  }
  return value;
}

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeUtahCandidateNameKeys(value);
  return [...keys][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Utah finance sync timestamp");
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
  folderId: string;
  committeeName: string;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): UtahFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    folderId: requireNonEmpty(input.folderId, "Utah disclosures folder id"),
    committeeName: requireNonEmpty(input.committeeName, "Utah committee name"),
    linkStatus: "active",
    linkSource: "disclosures_advanced_search",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function aggregateDirect(input: {
  committeeName: string;
  electionYear: number;
  transactions: readonly UtahDisclosuresTransactionRow[];
  transactionSourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
}): UtahDirectContributionAggregationResult {
  return aggregateUtahDirectContributions({
    committeeName: input.committeeName,
    electionYear: input.electionYear,
    transactions: input.transactions,
    sourceUrl: input.transactionSourceUrl ?? null,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
}

function resolveCommitteeForSync(input: {
  candidateName: string;
  electionYear: number;
  officeName?: string | null;
  district?: string | null;
  entityRows: readonly UtahDisclosuresEntitySearchRow[];
  sourceUrl?: string | null;
  trustedCommittee?: UtahCandidateFinanceSyncInput["trustedCommittee"];
}): UtahCandidateCommitteeResolution {
  if (input.trustedCommittee) {
    return {
      status: "matched",
      folderId: requireNonEmpty(input.trustedCommittee.folderId, "Utah disclosures folder id"),
      committeeName: requireNonEmpty(input.trustedCommittee.committeeName, "Utah committee name"),
      reportYears: [...(input.trustedCommittee.reportYears ?? [input.electionYear])],
      confidence: "exact",
      source: "disclosures_advanced_search",
      sourceUrl: input.trustedCommittee.sourceUrl ?? input.sourceUrl ?? null,
      matchedEntityRowCount: 1,
    };
  }

  return resolveUtahCandidateCommittee({
    candidateName: input.candidateName,
    electionYear: input.electionYear,
    officeName: input.officeName,
    district: input.district,
    entityRows: input.entityRows,
    sourceUrl: input.sourceUrl ?? null,
  });
}

function emptyResult(input: {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: UtahCandidateCommitteeResolution;
}): UtahCandidateFinanceSyncResult {
  return {
    candidateId: input.candidateId,
    electionId: input.electionId,
    electionYear: input.electionYear,
    dryRun: input.dryRun,
    resolution: input.resolution,
    linkWritten: false,
    summaryWritten: false,
    directBreakdownsWritten: 0,
    totalReceipts: null,
    directContributionTotal: null,
    totalDisbursements: null,
    matchedTransactionRowCount: 0,
    includedContributionRowCount: 0,
    skippedTransactionRowCount: 0,
    supportingCommitteeCount: 0,
    supportingCommitteeIndustryCount: 0,
    supportingCommitteeMatchedTransactionRowCount: 0,
    supportingCommitteeIncludedOrganizationDonorRowCount: 0,
    supportingCommitteeSkippedTransactionRowCount: 0,
  };
}

async function aggregateSupportingCommitteeIndustries(input: {
  candidateCommitteeName: string;
  electionYear: number;
  candidateTransactions: readonly UtahDisclosuresTransactionRow[];
  supportingCommitteeTransactions: readonly UtahDisclosuresTransactionRow[] | undefined;
  candidateSourceUrl?: string | null;
  committeeSourceUrl?: string | null;
  maxSupportingCommittees?: number;
  maxIndustriesPerCommittee?: number;
  minIndustryAmount?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  classifyIndustriesWithAi?: boolean;
}): Promise<UtahSupportingCommitteeIndustryAggregationResult | undefined> {
  if (input.supportingCommitteeTransactions === undefined) {
    return undefined;
  }
  return await aggregateUtahSupportingCommitteeIndustries({
    candidateCommitteeName: input.candidateCommitteeName,
    electionYear: input.electionYear,
    candidateTransactions: input.candidateTransactions,
    committeeTransactions: input.supportingCommitteeTransactions,
    candidateSourceUrl: input.candidateSourceUrl,
    committeeSourceUrl: input.committeeSourceUrl,
    maxSupportingCommittees: input.maxSupportingCommittees,
    maxIndustriesPerCommittee: input.maxIndustriesPerCommittee,
    minIndustryAmount: input.minIndustryAmount,
    financeIndustryClassifier: input.financeIndustryClassifier,
    classifyIndustriesWithAi: input.classifyIndustriesWithAi,
  });
}

export async function syncUtahCandidateFinance(
  input: UtahCandidateFinanceSyncInput
): Promise<UtahCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const resolution = resolveCommitteeForSync({
    candidateName,
    electionYear,
    officeName,
    district: input.district,
    entityRows: input.entityRows,
    sourceUrl: input.sourceUrl ?? input.transactionSourceUrl ?? null,
    trustedCommittee: input.trustedCommittee,
  });

  if (resolution.status !== "matched") {
    return emptyResult({ candidateId, electionId, electionYear, dryRun, resolution });
  }

  const directFinance = aggregateDirect({
    committeeName: resolution.committeeName,
    electionYear,
    transactions: input.transactions,
    transactionSourceUrl: input.transactionSourceUrl ?? resolution.sourceUrl,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory,
  });
  const supportingCommitteeFinance = await aggregateSupportingCommitteeIndustries({
    candidateCommitteeName: resolution.committeeName,
    electionYear,
    candidateTransactions: input.transactions,
    supportingCommitteeTransactions: input.supportingCommitteeTransactions,
    candidateSourceUrl: input.transactionSourceUrl ?? resolution.sourceUrl,
    committeeSourceUrl: input.supportingCommitteeSourceUrl,
    maxSupportingCommittees: input.supportingMaxCommittees,
    maxIndustriesPerCommittee: input.supportingMaxIndustriesPerCommittee,
    minIndustryAmount: input.supportingCommitteeIndustryMinAmount,
    financeIndustryClassifier: input.financeIndustryClassifier,
    classifyIndustriesWithAi: input.classifySupportingCommitteeIndustriesWithAi,
  });
  const link = toFinanceLink({
    candidateId,
    electionId,
    candidateName,
    electionYear,
    officeName,
    district: input.district,
    folderId: resolution.folderId,
    committeeName: resolution.committeeName,
    sourceUrl: resolution.sourceUrl ?? input.sourceUrl ?? input.transactionSourceUrl ?? null,
    verifiedAt: syncedAt,
  });

  if (!dryRun) {
    await replaceUtahCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary: directFinance.summary,
      directBreakdowns: directFinance.directBreakdowns,
      supportingCommittees: supportingCommitteeFinance?.supportingCommittees,
      supportingCommitteeIndustries: supportingCommitteeFinance?.supportingCommitteeIndustryBreakdowns,
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
    directBreakdownsWritten: dryRun ? 0 : directFinance.directBreakdowns.length,
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    totalDisbursements: directFinance.summary.totalDisbursements,
    matchedTransactionRowCount: directFinance.matchedTransactionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedTransactionRowCount: directFinance.skippedTransactionRowCount,
    supportingCommitteeCount: supportingCommitteeFinance?.supportingCommittees.length ?? 0,
    supportingCommitteeIndustryCount: supportingCommitteeFinance?.supportingCommitteeIndustryBreakdowns.length ?? 0,
    supportingCommitteeMatchedTransactionRowCount:
      supportingCommitteeFinance?.matchedCommitteeTransactionRowCount ?? 0,
    supportingCommitteeIncludedOrganizationDonorRowCount:
      supportingCommitteeFinance?.includedOrganizationDonorRowCount ?? 0,
    supportingCommitteeSkippedTransactionRowCount:
      supportingCommitteeFinance?.skippedCommitteeTransactionRowCount ?? 0,
  };
}
