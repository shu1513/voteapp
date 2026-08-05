import type { Pool, PoolClient } from "pg";

import { normalizeOhioCandidateNameForStorage } from "./ohioCandidateCommitteeResolver.js";
import type { OhioDirectContributionAggregationResult } from "./ohioDirectContributionAggregator.js";
import type { OhioFinanceOutsideGroup } from "./ohioOutsideSpendingAggregator.js";
import {
  replaceOhioCandidateFinanceSnapshot,
  type OhioFinanceLinkInput,
  type OhioFinanceOutsideGroupInput,
  type OhioFinanceSummaryInput,
} from "./ohioFinanceWriter.js";

// Per-candidate write step for Ohio finance (ohio_plan.md PR 7). Unlike the
// maryland sibling this takes aggregation RESULTS, not raw rows: the ~90 MB
// CAC_CON files must be streamed exactly once for every open accumulator
// (decision 10), so the batch layer owns loading and aggregation and this
// module only turns one candidate's results into a snapshot write.
//
// The committee identity comes from the active oh_candidate_finance_links
// row (written by the PR 5 auto-linker or manually), so there is no resolver
// call here — the due list only returns linked candidates.

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

// One candidate's slice of the year's outside-spending aggregation. Null
// totals never occur here — a candidate with no attributed rows gets zeros
// (the aggregation ran; the answer really is zero).
export type OhioCandidateOutsideFinanceInput = {
  supportTotal: number;
  opposeTotal: number;
  groups: readonly OhioFinanceOutsideGroup[];
};

export type OhioCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  // The linked committee from the due row — trusted, not re-resolved.
  committee: {
    committeeId: string;
    committeeName: string;
    sourceUrl?: string | null;
  };
  directFinance: OhioDirectContributionAggregationResult;
  // Null when Form 31-U data was unavailable this run: the summary's outside
  // totals are written as NULL (the writer's preserveWhenNull policy keeps
  // the stored values) and the outside-group rows are left untouched.
  outsideFinance: OhioCandidateOutsideFinanceInput | null;
  sourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
};

export type OhioCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  committeeId: string;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  totalReceipts: number;
  directContributionTotal: number;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  // Direct-aggregation diagnostics passed through for batch reporting.
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
  unknownShortDescriptionRowCount: number;
  coverReportCount: number;
  blankCoverRowCount: number;
  negativeBalanceOnHand: boolean;
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
    throw new Error(`Invalid Ohio finance sync election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Ohio finance sync timestamp");
  }
  return normalized;
}

export async function syncOhioCandidateFinance(
  input: OhioCandidateFinanceSyncInput
): Promise<OhioCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const committeeId = requireNonEmpty(input.committee.committeeId, "Ohio committee id");
  const committeeName = requireNonEmpty(input.committee.committeeName, "Ohio committee name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const { directFinance, outsideFinance } = input;

  const link: OhioFinanceLinkInput = {
    candidateId,
    electionId,
    electionYear,
    candidateNameNormalized: normalizeOhioCandidateNameForStorage(candidateName),
    officeName,
    district: input.district ?? null,
    committeeId,
    committeeName,
    linkStatus: "active",
    linkSource: "sos_bulk_export",
    sourceUrl: input.committee.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: syncedAt,
  };

  const summary: OhioFinanceSummaryInput = {
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    totalDisbursements: directFinance.summary.totalDisbursements,
    cashOnHand: directFinance.summary.cashOnHand,
    outsideSupportTotal: outsideFinance === null ? null : outsideFinance.supportTotal,
    outsideOpposeTotal: outsideFinance === null ? null : outsideFinance.opposeTotal,
    sourceUrl: directFinance.summary.sourceUrl ?? input.committee.sourceUrl ?? input.sourceUrl ?? null,
  };

  // Undefined (not []) when unavailable, so the writer leaves the stored
  // outside-group rows alone instead of deleting them.
  const outsideGroups: OhioFinanceOutsideGroupInput[] | undefined =
    outsideFinance === null ? undefined : [...outsideFinance.groups];

  if (!dryRun) {
    await replaceOhioCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary,
      directBreakdowns: directFinance.directBreakdowns,
      outsideGroups,
    });
  }

  return {
    candidateId,
    electionId,
    electionYear,
    dryRun,
    committeeId,
    linkWritten: !dryRun,
    summaryWritten: !dryRun,
    directBreakdownsWritten: dryRun ? 0 : directFinance.directBreakdowns.length,
    outsideGroupsWritten: dryRun ? 0 : outsideGroups?.length ?? 0,
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    totalDisbursements: directFinance.summary.totalDisbursements,
    cashOnHand: directFinance.summary.cashOnHand,
    outsideSupportTotal: outsideFinance === null ? null : outsideFinance.supportTotal,
    outsideOpposeTotal: outsideFinance === null ? null : outsideFinance.opposeTotal,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
    unknownShortDescriptionRowCount: directFinance.unknownShortDescriptionRowCount,
    coverReportCount: directFinance.coverReportCount,
    blankCoverRowCount: directFinance.blankCoverRowCount,
    negativeBalanceOnHand: directFinance.negativeBalanceOnHand,
  };
}
