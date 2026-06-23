import type { Pool, PoolClient } from "pg";

import {
  aggregateVirginiaDirectContributions,
  type VirginiaDirectContributionAggregationResult,
} from "./virginiaDirectContributionAggregator.js";
import type { VirginiaScheduleAContribution } from "./virginiaCampaignFinanceClient.js";
import {
  replaceVirginiaCandidateFinanceSnapshot,
  type VirginiaFinanceLinkInput,
} from "./virginiaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type VirginiaCandidateFinanceSyncInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  committeeId: string;
  committeeCode?: string | null;
  committeeName: string;
  sourceUrl?: string | null;
  contributions: readonly VirginiaScheduleAContribution[];
  contributionSourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
};

export type VirginiaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  totalReceipts: number;
  directContributionTotal: number;
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Virginia finance election year: ${value}`);
  }
  return value;
}

function normalizeCandidateNameForStorage(value: string): string {
  return requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Virginia finance sync timestamp");
  }
  return normalized;
}

function toFinanceLink(input: VirginiaCandidateFinanceSyncInput & {
  electionYear: number;
  now: Date;
}): VirginiaFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    committeeId: requireNonEmpty(input.committeeId, "Virginia committee id"),
    committeeCode: input.committeeCode ?? null,
    committeeName: requireNonEmpty(input.committeeName, "Virginia committee name"),
    linkSource: "manual",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.now,
  };
}

function aggregateDirect(input: {
  electionYear: number;
  contributions: readonly VirginiaScheduleAContribution[];
  contributionSourceUrl: string | null | undefined;
  linkSourceUrl: string | null | undefined;
  maxBreakdownsPerCategory: number | undefined;
}): VirginiaDirectContributionAggregationResult {
  return aggregateVirginiaDirectContributions({
    electionYear: input.electionYear,
    contributions: input.contributions,
    sourceUrl: input.contributionSourceUrl ?? input.linkSourceUrl ?? null,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
}

export async function syncVirginiaCandidateFinance(
  input: VirginiaCandidateFinanceSyncInput
): Promise<VirginiaCandidateFinanceSyncResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const link = toFinanceLink({ ...input, electionYear, now: syncedAt });
  const directFinance = aggregateDirect({
    electionYear,
    contributions: input.contributions,
    contributionSourceUrl: input.contributionSourceUrl,
    linkSourceUrl: link.sourceUrl,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory,
  });

  if (!input.dryRun) {
    await replaceVirginiaCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary: directFinance.summary,
      directBreakdowns: directFinance.directBreakdowns,
    });
  }

  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear,
    dryRun: input.dryRun === true,
    linkWritten: !input.dryRun,
    summaryWritten: !input.dryRun,
    directBreakdownsWritten: input.dryRun ? 0 : directFinance.directBreakdowns.length,
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
  };
}
