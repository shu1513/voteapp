import type { Pool, PoolClient } from "pg";

import { normalizeNorthCarolinaCandidateNameForStorage } from "./northCarolinaCandidateCommitteeResolver.js";
import type { NorthCarolinaDirectAggregationResult } from "./northCarolinaDirectContributionAggregator.js";
import type { NorthCarolinaFinanceOutsideGroup } from "./northCarolinaOutsideSpendingAggregator.js";
import {
  replaceNorthCarolinaCandidateFinanceSnapshot,
  type NorthCarolinaFinanceLinkInput,
  type NorthCarolinaFinanceLinkSource,
  type NorthCarolinaFinanceOutsideGroupInput,
  type NorthCarolinaFinanceSummaryInput,
} from "./northCarolinaFinanceWriter.js";

// Per-candidate write step for North Carolina finance (north_carolina_plan.md
// PR 7), ohio shape: this module takes aggregation RESULTS, not artifacts —
// the batch layer owns the artifact cache and the aggregators, and this
// module turns one candidate's results into one snapshot write.
//
// It enforces the direct aggregator's three-status write contract (PR 6):
// - "ok"                   → write the aggregated snapshot.
// - "honest_null"          → write it too: the aggregator already produced
//                            the honest snapshot (null summary fields, empty
//                            direct breakdowns) because the portal PROVES a
//                            required period is superseded-unavailable or its
//                            lineage is ambiguous. The writer's
//                            preserve-when-null policy keeps outside totals.
// - "incomplete_artifacts" → REFUSE to write (throw): the cache, not the
//                            portal, is suspect — a missing cached report or
//                            a mispaired cover must keep the previous valid
//                            snapshot and be re-acquired, never become
//                            writable money or a fake honest-null.
//
// The committee identity comes from the active nc_candidate_finance_links row
// (auto-linker or manual) via the due row — trusted, not re-resolved. The
// link's original provenance is written back as-is: a manual link must stay
// "manual" or it would lose provenance and become eligible for auto-link
// supersession.

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

// One candidate's slice of the year's outside-spending aggregation. Null
// totals never occur here — a candidate with no attributed rows gets zeros
// (the aggregation ran; the answer really is zero). The batch layer passes
// outsideFinance null instead when the IE artifacts were unavailable, and the
// writer's preserveWhenNull policy keeps the stored outside totals.
export type NorthCarolinaCandidateOutsideFinanceInput = {
  supportTotal: number;
  opposeTotal: number;
  groups: readonly NorthCarolinaFinanceOutsideGroup[];
};

export type NorthCarolinaCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  committee: {
    committeeId: string;
    committeeName: string;
    linkSource?: NorthCarolinaFinanceLinkSource;
    sourceUrl?: string | null;
  };
  directFinance: NorthCarolinaDirectAggregationResult;
  outsideFinance: NorthCarolinaCandidateOutsideFinanceInput | null;
  sourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
};

export type NorthCarolinaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  committeeId: string;
  // Never "incomplete_artifacts" — that status throws instead of writing.
  directStatus: "ok" | "honest_null";
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  // Direct-aggregation diagnostics passed through for batch reporting.
  selectedReportCount: number;
  supersededUnavailablePeriodCount: number;
  quarantinedGroupCount: number;
  derivedBreakdownsQuarantined: boolean;
  unknownReceiptTypeCodeCount: number;
  cycleChainMismatchCount: number;
  negativeCashOnHand: boolean;
  ieTypedRegularReportRowCount: number;
  ieTypedRegularReportCents: number;
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
    throw new Error(`Invalid North Carolina finance sync election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid North Carolina finance sync timestamp");
  }
  return normalized;
}

export async function syncNorthCarolinaCandidateFinance(
  input: NorthCarolinaCandidateFinanceSyncInput
): Promise<NorthCarolinaCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const committeeId = requireNonEmpty(input.committee.committeeId, "North Carolina committee id");
  const committeeName = requireNonEmpty(input.committee.committeeName, "North Carolina committee name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const { directFinance, outsideFinance } = input;

  // Three-status contract: incomplete artifacts never write. The message
  // carries the exact suspects so the caller (or a human) can re-acquire
  // precisely what is broken.
  if (directFinance.status === "incomplete_artifacts") {
    const suspects = [
      ...directFinance.missingReportIds.map((reportId) => `missing report ${reportId}`),
      ...directFinance.coverPeriodMismatchReportIds.map((reportId) => `mispaired cover for report ${reportId}`),
    ];
    throw new Error(
      "North Carolina finance artifacts are incomplete for this candidate; keeping the previous snapshot " +
        `and requiring re-acquisition (run north-carolina-candidates:finance:raw:refresh): ${suspects.join(", ")}`
    );
  }

  const link: NorthCarolinaFinanceLinkInput = {
    candidateId,
    electionId,
    electionYear,
    candidateNameNormalized: normalizeNorthCarolinaCandidateNameForStorage(candidateName),
    officeName,
    district: input.district ?? null,
    committeeId,
    committeeName,
    linkStatus: "active",
    linkSource: input.committee.linkSource ?? "ncsbe_portal",
    sourceUrl: input.committee.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: syncedAt,
  };

  const summary: NorthCarolinaFinanceSummaryInput = {
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    totalDisbursements: directFinance.summary.totalDisbursements,
    cashOnHand: directFinance.summary.cashOnHand,
    outsideSupportTotal: outsideFinance === null ? null : outsideFinance.supportTotal,
    outsideOpposeTotal: outsideFinance === null ? null : outsideFinance.opposeTotal,
    sourceUrl: directFinance.summary.sourceUrl ?? input.committee.sourceUrl ?? input.sourceUrl ?? null,
  };

  // Undefined (not []) when the outside leg was unavailable, so the writer
  // leaves the stored outside-group rows alone instead of deleting them. An
  // available leg with zero groups passes [] and legitimately clears rows.
  const outsideGroups: NorthCarolinaFinanceOutsideGroupInput[] | undefined =
    outsideFinance === null ? undefined : [...outsideFinance.groups];

  if (!dryRun) {
    await replaceNorthCarolinaCandidateFinanceSnapshot({
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
    directStatus: directFinance.status,
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
    selectedReportCount: directFinance.selectedReportIds.length,
    supersededUnavailablePeriodCount: directFinance.supersededUnavailablePeriods.length,
    quarantinedGroupCount: directFinance.quarantinedGroups.length,
    derivedBreakdownsQuarantined: directFinance.derivedBreakdownsQuarantined,
    unknownReceiptTypeCodeCount: directFinance.unknownReceiptTypeCodes.length,
    cycleChainMismatchCount: directFinance.cycleChainMismatches.length,
    negativeCashOnHand: directFinance.negativeCashOnHand,
    ieTypedRegularReportRowCount: directFinance.ieTypedRegularReportRowCount,
    ieTypedRegularReportCents: directFinance.ieTypedRegularReportCents,
  };
}
