import type { Pool, PoolClient } from "pg";

import {
  normalizeNebraskaCandidateNameKeys,
  resolveNebraskaCandidateCommittee,
  type NebraskaCandidateCommitteeResolution,
} from "./nebraskaCandidateCommitteeResolver.js";
import {
  aggregateNebraskaDirectContributions,
  type NebraskaDirectContributionAggregationResult,
} from "./nebraskaDirectContributionAggregator.js";
import {
  replaceNebraskaCandidateFinanceSnapshot,
  type NebraskaFinanceLinkInput,
} from "./nebraskaFinanceWriter.js";
import type { NebraskaNadcContributionRow } from "./nebraskaNadcArtifactReader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type NebraskaCandidateFinanceSyncInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  contributionRows: readonly NebraskaNadcContributionRow[];
  sourceUrl?: string | null;
  contributionSourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
};

export type NebraskaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: NebraskaCandidateCommitteeResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
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
  if (!Number.isInteger(value) || value < 2021 || value > 2100) {
    throw new Error(`Invalid Nebraska finance election year: ${value}`);
  }
  return value;
}

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeNebraskaCandidateNameKeys(value);
  return [...keys][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Nebraska finance sync timestamp");
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
}): NebraskaFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    committeeId: requireNonEmpty(input.committeeId, "Nebraska committee id"),
    committeeName: requireNonEmpty(input.committeeName, "Nebraska committee name"),
    linkStatus: "active",
    linkSource: "nadc_bulk",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function aggregateDirect(input: {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly NebraskaNadcContributionRow[];
  contributionSourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
}): NebraskaDirectContributionAggregationResult {
  return aggregateNebraskaDirectContributions({
    committeeId: input.committeeId,
    electionYear: input.electionYear,
    contributionRows: input.contributionRows,
    sourceUrl: input.contributionSourceUrl ?? null,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
}

export async function syncNebraskaCandidateFinance(
  input: NebraskaCandidateFinanceSyncInput
): Promise<NebraskaCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const resolution = resolveNebraskaCandidateCommittee({
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
      totalReceipts: null,
      directContributionTotal: null,
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

  if (!input.dryRun) {
    await replaceNebraskaCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary: directFinance.summary,
      directBreakdowns: directFinance.directBreakdowns,
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
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
  };
}
