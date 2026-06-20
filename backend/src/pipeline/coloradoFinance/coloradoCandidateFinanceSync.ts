import type { Pool, PoolClient } from "pg";

import {
  aggregateColoradoDirectContributions,
  type ColoradoDirectContributionAggregationResult,
} from "./coloradoDirectContributionAggregator.js";
import type { ColoradoTracerContributionRow } from "./coloradoTracerContributionReader.js";
import {
  replaceColoradoCandidateFinanceSnapshot,
  type ColoradoFinanceLinkInput,
} from "./coloradoFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type ColoradoCandidateFinanceSyncInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  committeeId: string;
  committeeName: string;
  tracerCandidateId?: string | null;
  sourceUrl?: string | null;
  contributionRows: readonly ColoradoTracerContributionRow[];
  contributionSourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
};

export type ColoradoCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  totalReceipts: number;
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
  if (!Number.isInteger(value) || value < 2001 || value > 2100) {
    throw new Error(`Invalid Colorado finance election year: ${value}`);
  }
  return value;
}

function normalizeCandidateNameForStorage(value: string): string {
  return requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Colorado finance sync timestamp");
  }
  return normalized;
}

function toFinanceLink(input: ColoradoCandidateFinanceSyncInput & {
  electionYear: number;
  now: Date;
}): ColoradoFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    tracerCandidateId: input.tracerCandidateId ?? null,
    committeeId: requireNonEmpty(input.committeeId, "Colorado committee id"),
    committeeName: requireNonEmpty(input.committeeName, "Colorado committee name"),
    linkSource: "manual",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.now,
  };
}

function aggregateDirect(input: {
  link: ColoradoFinanceLinkInput;
  electionYear: number;
  contributionRows: readonly ColoradoTracerContributionRow[];
  contributionSourceUrl: string | null | undefined;
  maxBreakdownsPerCategory: number | undefined;
}): ColoradoDirectContributionAggregationResult {
  return aggregateColoradoDirectContributions({
    committeeId: input.link.committeeId,
    electionYear: input.electionYear,
    contributionRows: input.contributionRows,
    sourceUrl: input.contributionSourceUrl ?? input.link.sourceUrl ?? null,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
}

export async function syncColoradoCandidateFinance(
  input: ColoradoCandidateFinanceSyncInput
): Promise<ColoradoCandidateFinanceSyncResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const link = toFinanceLink({ ...input, electionYear, now: syncedAt });
  const directFinance = aggregateDirect({
    link,
    electionYear,
    contributionRows: input.contributionRows,
    contributionSourceUrl: input.contributionSourceUrl,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory,
  });

  if (!input.dryRun) {
    await replaceColoradoCandidateFinanceSnapshot({
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
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
  };
}
