// Montana per-candidate finance sync (docs/plans/montana-finance.md, Phase 2a):
// read the cached artifact bundle, select canonical C5s, reconcile the
// cash-begin chain, aggregate, and write one snapshot.
//
// Presence semantics (SC precedent): the writer is only called with a
// chain-verified aggregation. No canonical C5s -> "no_filed_reports" and
// NOTHING is written (426 of the 2026 registrants declared sub-$500 and file
// no C-5 — absence is not a zero). Any read, selection, chain, or
// cross-check failure throws and writes nothing, preserving the prior
// snapshot. Cycle totals sum Primary + General across every canonical
// report — the whole candidacy, never a side-filtered slice.

import type { Pool, PoolClient } from "pg";

import {
  readMontanaCersArtifact,
  readMontanaCersOutsideSpendingArtifacts,
} from "./montanaCersArtifactCache.js";
import {
  parseMontanaCersContributionExport,
  parseMontanaCersExpenditureExport,
  parseMontanaCersReportDetailArtifact,
  parseMontanaCersReportInventory,
  type MontanaCersCandidateSearchRow,
  type MontanaCersExportRow,
  type MontanaCersIeSweepArtifact,
  type MontanaCersReportDetailArtifact,
  type MontanaCersReportInventoryRow,
} from "./montanaCersParsers.js";
import {
  aggregateMontanaDirectFinance,
  type MontanaDirectFinanceAggregationResult,
} from "./montanaDirectFinanceAggregator.js";
import {
  aggregateMontanaOutsideSpendingForCandidate,
  classifyMontanaOutsideSpendingRows,
  type MontanaOutsideSpendingAggregationResult,
} from "./montanaOutsideSpendingAggregator.js";
import { normalizeMontanaCandidateNameForStorage } from "./montanaCandidateCersResolver.js";
import { isMontanaFinanceEligibleOffice } from "./montanaFinanceEligibleOffices.js";
import { selectMontanaCanonicalReports } from "./montanaReportInventory.js";
import {
  normalizeMontanaCersEntityId,
  recordMontanaCandidateFinanceNoFiledReports,
  replaceMontanaCandidateFinanceSnapshot,
  type MontanaFinanceLinkSource,
} from "./montanaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

export class MontanaCandidateFinanceSyncError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MontanaCandidateFinanceSyncError";
  }
}

export type MontanaCandidateFinanceArtifacts = {
  inventory: readonly MontanaCersReportInventoryRow[];
  contributionRows: readonly MontanaCersExportRow[];
  expenditureRows: readonly MontanaCersExportRow[];
  detailArtifactsByReportId: ReadonlyMap<number, MontanaCersReportDetailArtifact>;
  sourceUrl: string | null;
};

export type MontanaOutsideSpendingArtifacts = {
  sweep: MontanaCersIeSweepArtifact;
  registrationRows: readonly MontanaCersCandidateSearchRow[];
  sourceUrl: string | null;
};

export type MontanaCandidateFinanceSyncResult = {
  dryRun: boolean;
  status: "synced" | "no_filed_reports";
  candidateId: string;
  electionId: string;
  cersCandidateId: number;
  canonicalReportCount: number;
  aggregation: MontanaDirectFinanceAggregationResult | null;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  /** True when a no_filed_reports pass stamped its checked-at rotation row. */
  checkedAtStamped: boolean;
  outsideSpending: MontanaOutsideSpendingAggregationResult | null;
  outsideSpendingSkippedReason: string | null;
  outsideGroupsWritten: number;
};

async function readCachedArtifacts(input: {
  cacheDir?: string;
  cersCandidateId: number;
  electionYear: number;
  canonicalReportIds: readonly number[];
}): Promise<Omit<MontanaCandidateFinanceArtifacts, "inventory"> & { vintages: Set<string> }> {
  const vintages = new Set<string>();
  const contributions = await readMontanaCersArtifact({
    cacheDir: input.cacheDir,
    key: { type: "contributions_export", candidateId: input.cersCandidateId, year: input.electionYear },
  });
  vintages.add(contributions.manifest.retrievedAt);
  const expenditures = await readMontanaCersArtifact({
    cacheDir: input.cacheDir,
    key: { type: "expenditures_export", candidateId: input.cersCandidateId, year: input.electionYear },
  });
  vintages.add(expenditures.manifest.retrievedAt);
  const detailArtifactsByReportId = new Map<number, MontanaCersReportDetailArtifact>();
  for (const reportId of input.canonicalReportIds) {
    const detail = await readMontanaCersArtifact({
      cacheDir: input.cacheDir,
      key: { type: "report_detail", candidateId: input.cersCandidateId, year: input.electionYear, reportId },
    });
    vintages.add(detail.manifest.retrievedAt);
    detailArtifactsByReportId.set(reportId, parseMontanaCersReportDetailArtifact(detail.body));
  }
  return {
    contributionRows: parseMontanaCersContributionExport(contributions.body),
    expenditureRows: parseMontanaCersExpenditureExport(expenditures.body),
    detailArtifactsByReportId,
    sourceUrl: contributions.manifest.sourceUrl,
    vintages,
  };
}

export async function syncMontanaCandidateFinance(input: {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  committee: {
    /** Numeric CERS candidateId stored as text on the link. */
    committeeId: string;
    committeeName: string;
    linkSource: MontanaFinanceLinkSource;
    sourceUrl?: string | null;
  };
  cacheDir?: string;
  artifacts?: MontanaCandidateFinanceArtifacts;
  /**
   * Yearly IE sweep bundle: undefined = read from the cache; null = the
   * batch already knows it is unavailable (skip the outside leg and
   * preserve any prior outside snapshot).
   */
  outsideArtifacts?: MontanaOutsideSpendingArtifacts | null;
  now?: Date;
  dryRun?: boolean;
  maxOccupationBreakdowns?: number;
  maxOutsideGroups?: number;
}): Promise<MontanaCandidateFinanceSyncResult> {
  const candidateName = input.candidateName.trim();
  const committeeName = input.committee.committeeName.trim();
  if (!candidateName || !committeeName) {
    throw new MontanaCandidateFinanceSyncError("candidateName and committee.committeeName are required");
  }
  if (!isMontanaFinanceEligibleOffice({ officeScope: input.officeScope, officeCanonicalName: input.officeName })) {
    throw new MontanaCandidateFinanceSyncError(
      `office ${input.officeScope}::${input.officeName} is not Montana-finance eligible`
    );
  }
  const cersCandidateId = Number(normalizeMontanaCersEntityId(input.committee.committeeId));
  if (!Number.isSafeInteger(input.electionYear) || input.electionYear < 2024 || input.electionYear > 2100) {
    throw new MontanaCandidateFinanceSyncError(`invalid Montana finance election year: ${input.electionYear}`);
  }
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new MontanaCandidateFinanceSyncError("invalid Montana finance sync timestamp");
  }
  const dryRun = input.dryRun === true;

  // Inventory first: a filer with no canonical C5s has no export or detail
  // artifacts at all, and must resolve to "no_filed_reports", not a miss.
  let inventory: readonly MontanaCersReportInventoryRow[];
  let inventoryVintage: string | null = null;
  if (input.artifacts !== undefined) {
    inventory = input.artifacts.inventory;
  } else {
    const artifact = await readMontanaCersArtifact({
      cacheDir: input.cacheDir,
      key: { type: "report_inventory", candidateId: cersCandidateId, year: input.electionYear },
    });
    inventory = parseMontanaCersReportInventory(artifact.body);
    inventoryVintage = artifact.manifest.retrievedAt;
  }

  const selection = selectMontanaCanonicalReports(inventory);
  const unexpectedStatuses = selection.diagnostics.filter(
    (diagnostic) => diagnostic.reason === "unexpected_status"
  );
  if (unexpectedStatuses.length > 0) {
    throw new MontanaCandidateFinanceSyncError(
      `Montana report inventory has rows in an unexpected status (possibly uncounted filings): ${unexpectedStatuses
        .map((diagnostic) => diagnostic.reportId)
        .join(", ")}`
    );
  }
  if (selection.hasOverlappingPeriods) {
    throw new MontanaCandidateFinanceSyncError("Montana canonical report periods overlap");
  }
  if (selection.reports.length === 0) {
    // Absence is not a zero: no money is written. But the check itself is
    // recorded (guarded, all-NULL summary) so the due list rotates past the
    // sub-$500 registrations instead of re-selecting them every batch.
    let checkedAtStamped = false;
    if (!dryRun) {
      const stamp = await recordMontanaCandidateFinanceNoFiledReports({
        db: input.db,
        link: {
          candidateId: input.candidateId,
          electionId: input.electionId,
          electionYear: input.electionYear,
          candidateNameNormalized: normalizeMontanaCandidateNameForStorage(candidateName),
          officeName: input.officeName,
          district: input.district ?? null,
          committeeId: String(cersCandidateId),
          committeeName,
          linkStatus: "active",
          linkSource: input.committee.linkSource,
          sourceUrl: input.committee.sourceUrl ?? null,
          lastVerifiedAt: now,
        },
        syncedAt: now,
      });
      checkedAtStamped = stamp.checkedAtStamped;
    }
    return {
      dryRun,
      status: "no_filed_reports",
      candidateId: input.candidateId,
      electionId: input.electionId,
      cersCandidateId,
      canonicalReportCount: 0,
      aggregation: null,
      summaryWritten: false,
      directBreakdownsWritten: 0,
      checkedAtStamped,
      // Documented v1 boundary: outside money is only published alongside a
      // filed direct snapshot. IE rows targeting a no-filing registrant stay
      // in the sweep's quarantine report rather than becoming a summary row
      // whose money columns would defeat the checked-at stamp's guard.
      outsideSpending: null,
      outsideSpendingSkippedReason: "no_filed_reports",
      outsideGroupsWritten: 0,
    };
  }

  let bundle: Omit<MontanaCandidateFinanceArtifacts, "inventory">;
  if (input.artifacts !== undefined) {
    bundle = input.artifacts;
  } else {
    const cached = await readCachedArtifacts({
      cacheDir: input.cacheDir,
      cersCandidateId,
      electionYear: input.electionYear,
      canonicalReportIds: selection.reports.map((report) => report.reportId),
    });
    if (inventoryVintage !== null) {
      cached.vintages.add(inventoryVintage);
    }
    if (cached.vintages.size !== 1) {
      throw new MontanaCandidateFinanceSyncError(
        `Mixed-vintage Montana artifact bundle for candidate ${cersCandidateId} ${input.electionYear} — partial refresh`
      );
    }
    bundle = cached;
  }

  const canonicalReports = selection.reports.map((report) => {
    const artifact = bundle.detailArtifactsByReportId.get(report.reportId);
    if (artifact === undefined) {
      throw new MontanaCandidateFinanceSyncError(
        `Missing Montana report detail for canonical report ${report.reportId}`
      );
    }
    return { inventory: report, artifact };
  });

  const sourceUrl = bundle.sourceUrl ?? input.committee.sourceUrl ?? null;
  const aggregation = aggregateMontanaDirectFinance({
    canonicalReports,
    contributionRows: bundle.contributionRows,
    expenditureRows: bundle.expenditureRows,
    sourceUrl,
    maxOccupationBreakdowns: input.maxOccupationBreakdowns,
  });

  // Outside leg (Phase 2b). A missing or stale sweep bundle skips the leg
  // rather than failing the direct sync: the writer's preserveWhenNull
  // policy plus an undefined outsideGroups input keeps any prior outside
  // snapshot intact. When the bundle IS present, its aggregation is
  // authoritative for this candidate — including an empty result, which
  // clears stale groups (totals stay null, never a fabricated zero).
  let outsideArtifacts = input.outsideArtifacts;
  let outsideSpendingSkippedReason: string | null = null;
  if (outsideArtifacts === null) {
    outsideSpendingSkippedReason = "yearly outside-spending artifacts unavailable";
  } else if (outsideArtifacts === undefined) {
    try {
      outsideArtifacts = await readMontanaCersOutsideSpendingArtifacts({
        cacheDir: input.cacheDir,
        year: input.electionYear,
      });
    } catch (error) {
      outsideArtifacts = null;
      outsideSpendingSkippedReason = error instanceof Error ? error.message : String(error);
    }
  }
  let outsideSpending: MontanaOutsideSpendingAggregationResult | null = null;
  if (outsideArtifacts) {
    const classifiedRows = classifyMontanaOutsideSpendingRows({
      sweep: outsideArtifacts.sweep,
      registrationRows: outsideArtifacts.registrationRows,
      electionYear: input.electionYear,
    });
    outsideSpending = aggregateMontanaOutsideSpendingForCandidate({
      classifiedRows,
      cersCandidateId,
      sourceUrl: outsideArtifacts.sourceUrl ?? sourceUrl,
      maxGroups: input.maxOutsideGroups,
    });
  }

  let summaryWritten = false;
  let directBreakdownsWritten = 0;
  let outsideGroupsWritten = 0;
  if (!dryRun) {
    const writeResult = await replaceMontanaCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId: input.candidateId,
        electionId: input.electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeMontanaCandidateNameForStorage(candidateName),
        officeName: input.officeName,
        district: input.district ?? null,
        committeeId: String(cersCandidateId),
        committeeName,
        linkStatus: "active",
        linkSource: input.committee.linkSource,
        sourceUrl: input.committee.sourceUrl ?? sourceUrl,
        lastVerifiedAt: now,
      },
      syncedAt: now,
      summary: {
        totalReceipts: null,
        directContributionTotal: aggregation.directContributionTotal,
        totalDisbursements: aggregation.totalDisbursements,
        cashOnHand: aggregation.cashOnHand,
        outsideSupportTotal: outsideSpending?.supportTotal ?? null,
        outsideOpposeTotal: outsideSpending?.opposeTotal ?? null,
        sourceUrl,
      },
      directBreakdowns: aggregation.directBreakdowns,
      outsideGroups: outsideSpending?.outsideGroups,
    });
    summaryWritten = writeResult.summaryWritten;
    directBreakdownsWritten = writeResult.directBreakdownsWritten;
    outsideGroupsWritten = writeResult.outsideGroupsWritten;
  }

  return {
    dryRun,
    status: "synced",
    candidateId: input.candidateId,
    electionId: input.electionId,
    cersCandidateId,
    canonicalReportCount: selection.reports.length,
    aggregation,
    summaryWritten,
    directBreakdownsWritten,
    checkedAtStamped: false,
    outsideSpending,
    outsideSpendingSkippedReason,
    outsideGroupsWritten,
  };
}
