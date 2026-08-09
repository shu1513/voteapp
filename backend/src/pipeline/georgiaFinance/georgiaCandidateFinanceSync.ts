import type { Pool, PoolClient } from "pg";

import {
  buildGeorgiaReportInventory,
  fetchGeorgiaCandidateIndexRows,
  fetchGeorgiaFiledReportRows,
  fetchGeorgiaIndependentExpenditureRows,
  fetchGeorgiaTransactionRowsWindowed,
  georgiaTransactionReportGroupGuid,
  GeorgiaEthicsClientError,
  GEORGIA_ETHICS_RECORDS_SEARCH_URL,
  type GeorgiaCandidateIndexRow,
  type GeorgiaEthicsHost,
  type GeorgiaEthicsTransport,
  type GeorgiaFiledReportRow,
  type GeorgiaIndependentExpenditureRow,
  type GeorgiaReportInventoryEntry,
  type GeorgiaWindowedTransactionFetchResult,
} from "./georgiaEthicsClient.js";
import {
  aggregateGeorgiaOutsideSpending,
  type GeorgiaOutsideSpendingAggregationResult,
} from "./georgiaOutsideSpendingAggregator.js";
import {
  georgiaCandidateNameMatchesRowNames,
  georgiaLastNameSearchToken,
  normalizeGeorgiaCandidateNameForStorage,
} from "./georgiaCandidateCommitteeResolver.js";
import {
  aggregateGeorgiaDirectContributions,
  type GeorgiaDirectContributionAggregationResult,
  type GeorgiaTaggedTransactionRow,
} from "./georgiaDirectContributionAggregator.js";
import { listGeorgiaFilerIdentityMapRowsByCanonicalCommittee } from "./georgiaFilerIdentityMap.js";
import {
  replaceGeorgiaCandidateFinanceSnapshot,
  type GeorgiaFinanceLinkInput,
  type GeorgiaFinanceLinkSource,
} from "./georgiaFinanceWriter.js";

// Per-candidate finance sync for Georgia (georgia_plan.md PR 4 direct leg,
// PR 5 outside leg): per-filer TCON pull across both systems with D8
// report-source selection, D5 aggregation, the official candidate-index
// summary (D4), the reconciliation guard that keeps the previous good
// snapshot when the synced rows do not explain the official total, and the
// PeachFile IE leg with D6 single-target allocation.
//
// Registration-chain scope:
// - The PeachFile side is always the linked committee (committee_id =
//   PeachFile filerEntityId, D2/D7).
// - The archive side comes from the D3 identity map when candidate-committee
//   rows exist for the canonical committee; otherwise it is DISCOVERED from
//   the archive candidate index: same person (middle-name-evidence match),
//   same election cycle (label leads with the election year), and not
//   terminated (`filerStatusCode` "T" — the string code, never the broken
//   isTerminated boolean; Carr's legacy committee is excluded exactly this
//   way while his re-keyed archive registration survives). Discovered
//   registrations are used in memory only — nothing is written to the map —
//   and the reconciliation guard arbitrates the assembled set every run.

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type GeorgiaCandidateFinanceSyncFetchers = {
  fetchCandidateIndexRows: typeof fetchGeorgiaCandidateIndexRows;
  fetchFiledReportRows: typeof fetchGeorgiaFiledReportRows;
  fetchTransactionRowsWindowed: typeof fetchGeorgiaTransactionRowsWindowed;
  fetchIndependentExpenditureRows: typeof fetchGeorgiaIndependentExpenditureRows;
};

export type GeorgiaCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  transport: GeorgiaEthicsTransport;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  // The linked committee from the due row — trusted, not re-resolved. The
  // link's original provenance is written back as-is.
  committee: {
    committeeId: string;
    committeeName: string;
    linkSource?: GeorgiaFinanceLinkSource;
    sourceUrl?: string | null;
  };
  now?: Date;
  dryRun?: boolean;
  maxBreakdownsPerCategory?: number;
  windowDays?: number;
  maxPasses?: number;
  // Reconciliation tolerance: max(absolute floor, relative share of the
  // official index total). The Carr decomposition drifted 0.40% purely from
  // PeachFile's migrated copies of pre-cutover reports (D4), so the default
  // relative tolerance leaves room above that while still catching a missing
  // registration or a double-counted report.
  reconciliationRelativeTolerance?: number;
  reconciliationAbsoluteToleranceFloor?: number;
  maxOutsideGroups?: number;
  // Pre-fetched PeachFile IE store rows (F5) — the batch layer pulls the
  // store once per run and shares it across candidates; when undefined the
  // sync fetches it itself. NULL is the explicit "store unavailable"
  // sentinel: the caller already tried and failed, so the outside leg is
  // skipped (stored outside data preserved via the partial-snapshot
  // contract) instead of every candidate retrying a known-dead fetch.
  independentExpenditureRows?: readonly GeorgiaIndependentExpenditureRow[] | null;
  fetchers?: Partial<GeorgiaCandidateFinanceSyncFetchers>;
};

export type GeorgiaCandidateFinanceHostPullDiagnostics = {
  fetchedRowCount: number;
  includedRowCount: number;
  supersededRowCount: number;
  unassignedRowCount: number;
  windowFilterIneffectiveCount: number;
  sweepOnlyRowCount: number;
  sweepMissedRowCount: number;
  filterIneffective: boolean;
};

export type GeorgiaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  committeeId: string;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  // Null when the outside leg was skipped (IE store unavailable) — stored
  // outside totals and groups were preserved, not zeroed.
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  totalReceipts: number;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  syncedRowSum: number;
  reconciliationDifference: number;
  reconciliationTolerance: number;
  archiveRegistrationGuids: string[];
  archiveRegistrationSource: "identity_map" | "discovered" | "none";
  reportInventorySize: number;
  peachfile: GeorgiaCandidateFinanceHostPullDiagnostics;
  archive: GeorgiaCandidateFinanceHostPullDiagnostics;
  aggregation: Omit<GeorgiaDirectContributionAggregationResult, "directBreakdowns">;
  // Null when the outside leg was skipped; the reason says why.
  outsideSpending: Omit<GeorgiaOutsideSpendingAggregationResult, "outsideGroups"> | null;
  outsideSpendingSkippedReason: string | null;
};

export class GeorgiaFinanceReconciliationError extends Error {
  constructor(
    message: string,
    public readonly details: {
      committeeId: string;
      indexTotalContributions: number;
      syncedRowSum: number;
      difference: number;
      tolerance: number;
      archiveRegistrationGuids: string[];
    }
  ) {
    super(message);
    this.name = "GeorgiaFinanceReconciliationError";
  }
}

const DEFAULT_RECONCILIATION_RELATIVE_TOLERANCE = 0.02;
// The relative share is the real absorber (migration drift scales with the
// money — Carr measured 0.40%); the absolute floor only exists so cent-level
// noise cannot fail a near-zero filer, and it must stay SMALL: the floor
// dominates the tolerance for every filer under floor/relative dollars, and a
// large floor would let a small campaign lose most of its rows and still
// "reconcile".
const DEFAULT_RECONCILIATION_ABSOLUTE_TOLERANCE_FLOOR = 100;
// Fallback range start when a chain has no dated reports at all: Georgia
// statewide cycles run four years, so the floor is generous by construction;
// out-of-range rows are the sweep's job either way (A4).
const FALLBACK_RANGE_YEARS_BEFORE_ELECTION = 4;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Georgia finance sync election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Georgia finance sync timestamp");
  }
  return normalized;
}

function normalizeTolerance(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Georgia finance sync ${fieldName}: ${value}`);
  }
  return normalized;
}

function isoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function centsRound(value: number): number {
  return Math.round(value * 100) / 100;
}

// A registration belongs to the requested cycle when either cycle label leads
// with the election year (resolver rule; the archive renders e.g.
// "2026 State/Statewide Election Cycle for Candidates (January and June)").
function rowMatchesElectionYear(row: GeorgiaCandidateIndexRow, electionYear: number): boolean {
  const yearPrefix = `${electionYear} `;
  return [row.electionCycleName, row.filingCycleName].some(
    (name) => typeof name === "string" && name.startsWith(yearPrefix)
  );
}

function indexRowNames(row: GeorgiaCandidateIndexRow): string[] {
  const structuredName =
    row.candidateFirstName && row.candidateLastName
      ? [row.candidateFirstName, row.candidateMiddleName, row.candidateLastName].filter(Boolean).join(" ")
      : null;
  return [row.filerName, row.ballotFullName, structuredName].filter((name): name is string =>
    Boolean(name && name.trim())
  );
}

// Discovers the archive side of the registration chain (D3): every archive
// candidate-index row for the same person and cycle whose registration is not
// terminated. Office match is corroboration, never a discovery filter — a
// legacy committee registered for a prior race's office can carry
// current-cycle rows, and the terminated-status gate plus the reconciliation
// guard are what keep separate ledgers out.
export function discoverGeorgiaArchiveRegistrations(input: {
  candidateName: string;
  electionYear: number;
  archiveIndexRows: readonly GeorgiaCandidateIndexRow[];
}): GeorgiaCandidateIndexRow[] {
  const byRegistration = new Map<string, GeorgiaCandidateIndexRow>();
  for (const row of input.archiveIndexRows) {
    if (!rowMatchesElectionYear(row, input.electionYear)) {
      continue;
    }
    if (row.filerStatusCode?.trim().toUpperCase() === "T") {
      continue;
    }
    if (!georgiaCandidateNameMatchesRowNames(input.candidateName, indexRowNames(row))) {
      continue;
    }
    const registrationGuid = row.guid.trim().toLowerCase();
    if (!byRegistration.has(registrationGuid)) {
      byRegistration.set(registrationGuid, row);
    }
  }
  return [...byRegistration.values()];
}

// Per-host selected report-group guids from the D8 inventory union: the
// winning report's guid plus every child-version guid (rows may reference
// either encoding), all lowercased. Superseded archive copies land in the
// superseded set so their rows are counted as expected exclusions rather
// than unexplained ones.
export function buildGeorgiaSelectedReportGuids(inventory: readonly GeorgiaReportInventoryEntry[]): {
  selectedByHost: Record<GeorgiaEthicsHost, Set<string>>;
  supersededArchiveGuids: Set<string>;
} {
  const selectedByHost: Record<GeorgiaEthicsHost, Set<string>> = {
    peachfile: new Set<string>(),
    efile_archive: new Set<string>(),
  };
  const supersededArchiveGuids = new Set<string>();

  function reportGuids(report: GeorgiaFiledReportRow): string[] {
    return [report.filerReportGuid, ...report.childVersions.map((version) => version.filerReportGuid)].map((guid) =>
      guid.trim().toLowerCase()
    );
  }

  for (const entry of inventory) {
    for (const guid of reportGuids(entry.report)) {
      selectedByHost[entry.source].add(guid);
    }
    if (entry.supersededArchiveReport) {
      for (const guid of reportGuids(entry.supersededArchiveReport)) {
        supersededArchiveGuids.add(guid);
      }
    }
  }
  return { selectedByHost, supersededArchiveGuids };
}

function emptyHostDiagnostics(): GeorgiaCandidateFinanceHostPullDiagnostics {
  return {
    fetchedRowCount: 0,
    includedRowCount: 0,
    supersededRowCount: 0,
    unassignedRowCount: 0,
    windowFilterIneffectiveCount: 0,
    sweepOnlyRowCount: 0,
    sweepMissedRowCount: 0,
    filterIneffective: false,
  };
}

function transactionRangeStart(inventory: readonly GeorgiaReportInventoryEntry[], electionYear: number): string {
  let earliest: string | null = null;
  for (const entry of inventory) {
    if (entry.periodStart && (earliest === null || entry.periodStart < earliest)) {
      earliest = entry.periodStart;
    }
  }
  return earliest ?? `${electionYear - FALLBACK_RANGE_YEARS_BEFORE_ELECTION}-01-01`;
}

export async function syncGeorgiaCandidateFinance(
  input: GeorgiaCandidateFinanceSyncInput
): Promise<GeorgiaCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const committeeId = requireNonEmpty(input.committee.committeeId, "Georgia committee id");
  const committeeName = requireNonEmpty(input.committee.committeeName, "Georgia committee name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const relativeTolerance = normalizeTolerance(
    input.reconciliationRelativeTolerance,
    DEFAULT_RECONCILIATION_RELATIVE_TOLERANCE,
    "reconciliationRelativeTolerance"
  );
  const absoluteToleranceFloor = normalizeTolerance(
    input.reconciliationAbsoluteToleranceFloor,
    DEFAULT_RECONCILIATION_ABSOLUTE_TOLERANCE_FLOOR,
    "reconciliationAbsoluteToleranceFloor"
  );
  const fetchers: GeorgiaCandidateFinanceSyncFetchers = {
    fetchCandidateIndexRows: input.fetchers?.fetchCandidateIndexRows ?? fetchGeorgiaCandidateIndexRows,
    fetchFiledReportRows: input.fetchers?.fetchFiledReportRows ?? fetchGeorgiaFiledReportRows,
    fetchTransactionRowsWindowed: input.fetchers?.fetchTransactionRowsWindowed ?? fetchGeorgiaTransactionRowsWindowed,
    fetchIndependentExpenditureRows:
      input.fetchers?.fetchIndependentExpenditureRows ?? fetchGeorgiaIndependentExpenditureRows,
  };
  const committeeEntityId = Number(committeeId);
  if (!Number.isInteger(committeeEntityId) || committeeEntityId <= 0) {
    throw new Error(`Invalid Georgia committee id (want the PeachFile filerEntityId): ${JSON.stringify(committeeId)}`);
  }

  // 1. Official summary + canonical registration from the PeachFile index
  //    (D4: the index totals are official and full-cycle; they are also the
  //    reconciliation anchor, so a missing index row fails the sync).
  const lastNameToken = georgiaLastNameSearchToken(candidateName);
  const peachfileIndexRows = await fetchers.fetchCandidateIndexRows(input.transport, "peachfile", {
    filerName: lastNameToken,
  });
  // Index rows are per REGISTRATION, and one filerEntityId gains a new
  // registration row per cycle (the archive shows the shape: filer 2750 has
  // one 2022-cycle and one 2026-cycle row). Matching on the entity id alone
  // would be API-order-dependent once a committee re-registers, so the
  // election-cycle gate that created the link (resolver rule) re-applies
  // here, and anything but exactly one surviving row fails closed.
  const indexRowCandidates = peachfileIndexRows.filter(
    (row) => row.filerEntityId === committeeEntityId && rowMatchesElectionYear(row, electionYear)
  );
  if (indexRowCandidates.length !== 1) {
    throw new Error(
      `Georgia PeachFile candidate index has ${indexRowCandidates.length} rows for filerEntityId ${committeeId} ` +
        `in the ${electionYear} cycle (search token ${JSON.stringify(lastNameToken)}) — ` +
        "cannot anchor summary or reconciliation"
    );
  }
  const indexRow = indexRowCandidates[0]!;
  const peachfileRegistrationGuid = indexRow.guid.trim().toLowerCase();
  // The official totals are the reconciliation anchor and overwrite the
  // stored summary (replace policy), so a null is an upstream anomaly that
  // must fail closed — no-money filers report 0.0, never null.
  if (indexRow.totalContributions === null || indexRow.totalExpenditures === null || indexRow.cashOnHand === null) {
    throw new Error(
      `Georgia PeachFile candidate index row for filerEntityId ${committeeId} is missing official totals ` +
        `(totalContributions ${indexRow.totalContributions}, totalExpenditures ${indexRow.totalExpenditures}, ` +
        `cashOnHand ${indexRow.cashOnHand}) — refusing to overwrite the stored summary`
    );
  }
  const indexTotalContributions = indexRow.totalContributions;

  // 2. Archive side of the registration chain: identity map first, discovery
  //    otherwise. Map rows marked include_in_candidate_totals=false are
  //    deliberate exclusions (separate ledgers) and are never pulled.
  const mapRows = await listGeorgiaFilerIdentityMapRowsByCanonicalCommittee(input.db, committeeId);
  // ANY archive candidate-committee map row makes the map authoritative for
  // the archive side — including a lone exclusion row
  // (include_in_candidate_totals=false), which must suppress discovery
  // instead of letting it re-propose the excluded registration.
  const archiveMapRows = mapRows.filter(
    (row) => row.sourceSystem === "efile_archive" && row.entityRole === "candidate_committee"
  );

  let archiveRegistrationSource: GeorgiaCandidateFinanceSyncResult["archiveRegistrationSource"];
  let archiveFilers: Array<{ filerEntityId: number; registrationGuid: string; searchName: string }>;
  if (archiveMapRows.length > 0) {
    archiveRegistrationSource = "identity_map";
    archiveFilers = archiveMapRows
      .filter((row) => row.includeInCandidateTotals)
      .map((row) => ({
        filerEntityId: Number(row.sourceFilerEntityId),
        registrationGuid: row.sourceRegistrationGuid,
        searchName: row.sourceFilerName,
      }));
  } else {
    const archiveIndexRows = await fetchers.fetchCandidateIndexRows(input.transport, "efile_archive", {
      filerName: lastNameToken,
    });
    const discovered = discoverGeorgiaArchiveRegistrations({
      candidateName,
      electionYear,
      archiveIndexRows,
    });
    archiveRegistrationSource = discovered.length > 0 ? "discovered" : "none";
    archiveFilers = discovered.map((row) => ({
      filerEntityId: row.filerEntityId,
      registrationGuid: row.guid.trim().toLowerCase(),
      searchName: row.filerName,
    }));
  }
  const archiveRegistrationGuids = archiveFilers.map((filer) => filer.registrationGuid);

  // 3. Report inventories, scoped to the chain's registrations (report rows
  //    fetched by name substring can include other filers).
  const peachfileReports = (
    await fetchers.fetchFiledReportRows(input.transport, "peachfile", { filerName: committeeName })
  ).filter((report) => report.filerRegistrationGuid.trim().toLowerCase() === peachfileRegistrationGuid);

  const archiveGuidSet = new Set(archiveRegistrationGuids);
  const archiveSearchNames = [...new Set(archiveFilers.map((filer) => filer.searchName.trim()).filter(Boolean))];
  const archiveReports: GeorgiaFiledReportRow[] = [];
  {
    const seenReportGuids = new Set<string>();
    for (const searchName of archiveSearchNames) {
      const reports = await fetchers.fetchFiledReportRows(input.transport, "efile_archive", { filerName: searchName });
      for (const report of reports) {
        const reportGuid = report.filerReportGuid.trim().toLowerCase();
        if (archiveGuidSet.has(report.filerRegistrationGuid.trim().toLowerCase()) && !seenReportGuids.has(reportGuid)) {
          seenReportGuids.add(reportGuid);
          archiveReports.push(report);
        }
      }
    }
  }

  const inventory = buildGeorgiaReportInventory({ peachfileReports, archiveReports });
  const { selectedByHost, supersededArchiveGuids } = buildGeorgiaSelectedReportGuids(inventory);
  const rangeStart = transactionRangeStart(inventory, electionYear);
  const rangeEnd = isoDate(syncedAt);

  // 4. TCON pulls per host (A4 windowed + mandatory sweep), then D8 row
  //    selection by report-group guid. Rows on superseded archive copies are
  //    expected exclusions; anything else unmatched is counted and left to
  //    the reconciliation guard.
  const taggedRows: GeorgiaTaggedTransactionRow[] = [];
  const peachfileDiagnostics = emptyHostDiagnostics();
  const archiveDiagnostics = emptyHostDiagnostics();
  // Cross-pull dedup: two archive search names for the same person can
  // return overlapping row sets.
  const seenTransactionIdsByHost: Record<GeorgiaEthicsHost, Set<number>> = {
    peachfile: new Set<number>(),
    efile_archive: new Set<number>(),
  };

  async function pullHostRows(
    host: GeorgiaEthicsHost,
    filerName: string,
    expectedFilerEntityIds: readonly number[],
    diagnostics: GeorgiaCandidateFinanceHostPullDiagnostics
  ): Promise<void> {
    let fetched: GeorgiaWindowedTransactionFetchResult;
    try {
      fetched = await fetchers.fetchTransactionRowsWindowed(input.transport, host, {
        filerName,
        fromDate: rangeStart,
        toDate: rangeEnd,
        windowDays: input.windowDays,
        expectedFilerEntityIds,
        maxPasses: input.maxPasses,
      });
    } catch (error) {
      // The whole-pull filter_ineffective shape — zero rows for the expected
      // filer while the name substring matched only foreign filers — is a
      // real possibility for a registration that never filed a transaction
      // (and, on the archive, for a person whose only other rows belong to a
      // deliberately excluded legacy ledger). Treated as zero rows; the
      // reconciliation guard fails the sync if real money went missing.
      if (error instanceof GeorgiaEthicsClientError && error.code === "filter_ineffective") {
        diagnostics.filterIneffective = true;
        return;
      }
      throw error;
    }
    // Accumulated — the archive side can run one pull per distinct source
    // filer name.
    diagnostics.fetchedRowCount += fetched.rows.length;
    diagnostics.windowFilterIneffectiveCount += fetched.windowFilterIneffectiveCount;
    diagnostics.sweepOnlyRowCount += fetched.sweepOnlyRowCount;
    diagnostics.sweepMissedRowCount += fetched.sweepMissedRowCount;
    const selected = selectedByHost[host];
    const seenTransactionIds = seenTransactionIdsByHost[host];
    for (const row of fetched.rows) {
      if (seenTransactionIds.has(row.transactionId)) {
        continue;
      }
      seenTransactionIds.add(row.transactionId);
      const groupGuid = georgiaTransactionReportGroupGuid(row);
      if (groupGuid && selected.has(groupGuid)) {
        diagnostics.includedRowCount += 1;
        taggedRows.push({ host, row });
      } else if (groupGuid && host === "efile_archive" && supersededArchiveGuids.has(groupGuid)) {
        diagnostics.supersededRowCount += 1;
      } else {
        diagnostics.unassignedRowCount += 1;
      }
    }
  }

  await pullHostRows("peachfile", committeeName, [committeeEntityId], peachfileDiagnostics);
  for (const searchName of archiveSearchNames) {
    await pullHostRows(
      "efile_archive",
      searchName,
      archiveFilers.map((filer) => filer.filerEntityId),
      archiveDiagnostics
    );
  }

  // 5. Aggregate (D5) and reconcile against the official index total (D4).
  const sourceUrl = input.committee.sourceUrl ?? GEORGIA_ETHICS_RECORDS_SEARCH_URL;
  const directFinance = aggregateGeorgiaDirectContributions({
    rows: taggedRows,
    sourceUrl,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
  const difference = centsRound(Math.abs(directFinance.syncedRowSum - indexTotalContributions));
  const tolerance = centsRound(Math.max(absoluteToleranceFloor, relativeTolerance * Math.abs(indexTotalContributions)));
  // Zero-coverage guard: the index total is the exact sum of the store's
  // rows (spike A6), so a nonzero total with NO selected rows proves the
  // pull or the report selection is broken — no tolerance can excuse it.
  // Writing through would replace the stored breakdowns with [] (the writer
  // deletes on empty arrays) on the say-so of a failed pull.
  if (taggedRows.length === 0 && indexTotalContributions !== 0) {
    throw new GeorgiaFinanceReconciliationError(
      `Georgia finance reconciliation failed for committee ${committeeId}: the official index total is ` +
        `$${indexTotalContributions.toFixed(2)} but the pull selected zero transaction rows; previous snapshot ` +
        `kept — review the registration chain (archive side: ${archiveRegistrationGuids.join(", ") || "none"})`,
      {
        committeeId,
        indexTotalContributions,
        syncedRowSum: 0,
        difference,
        tolerance,
        archiveRegistrationGuids,
      }
    );
  }
  if (difference > tolerance) {
    throw new GeorgiaFinanceReconciliationError(
      `Georgia finance reconciliation failed for committee ${committeeId}: synced rows sum to ` +
        `$${directFinance.syncedRowSum.toFixed(2)} but the official index total is ` +
        `$${indexTotalContributions.toFixed(2)} (difference $${difference.toFixed(2)} > tolerance ` +
        `$${tolerance.toFixed(2)}); previous snapshot kept — review the registration chain ` +
        `(archive side: ${archiveRegistrationGuids.join(", ") || "none"})`,
      {
        committeeId,
        indexTotalContributions,
        syncedRowSum: directFinance.syncedRowSum,
        difference,
        tolerance,
        archiveRegistrationGuids,
      }
    );
  }

  // 6. Outside spending (F5/D6): the IE leg runs AFTER the reconciliation
  //    guard so a failed direct pull never costs the store fetch. The
  //    PeachFile IE store is fetched whole (or arrives pre-fetched from the
  //    batch layer) and targets join to the candidate by registration guid —
  //    an ID join, no name matching. Archive IE rows are excluded by design:
  //    their targets carry neither a registration guid nor a reasonTypeCode
  //    (spike bytes), so no archive row can ever satisfy the D6 gates, and
  //    the coverage note discloses the gap (D12).
  //
  //    The IE leg is the LAST fetch — by now the direct leg's hundreds of
  //    paced requests have succeeded and reconciled — so an IE-side client
  //    failure (network, WAF, unstable paging, the empty-store guard)
  //    degrades to a direct-only sync instead of discarding that work: the
  //    outside leg is skipped and the stored outside totals and groups are
  //    preserved via the partial-snapshot contract (undefined, never []).
  //    Anything that is not a client error is a bug and still throws.
  let independentExpenditureRows = input.independentExpenditureRows;
  let outsideSpendingSkippedReason: string | null = null;
  if (independentExpenditureRows === null) {
    outsideSpendingSkippedReason = "IE store unavailable (batch-level fetch failed)";
  } else if (independentExpenditureRows === undefined) {
    try {
      independentExpenditureRows = (
        await fetchers.fetchIndependentExpenditureRows(input.transport, "peachfile", { maxPasses: input.maxPasses })
      ).rows;
    } catch (error) {
      if (!(error instanceof GeorgiaEthicsClientError)) {
        throw error;
      }
      outsideSpendingSkippedReason = error.message;
    }
  }
  const outsideSpending =
    independentExpenditureRows === null || independentExpenditureRows === undefined
      ? null
      : aggregateGeorgiaOutsideSpending({
          host: "peachfile",
          rows: independentExpenditureRows,
          candidateRegistrationGuid: peachfileRegistrationGuid,
          sourceUrl,
          maxGroups: input.maxOutsideGroups,
        });

  // 7. Snapshot write: official index totals as the summary (D4;
  //    direct_contribution_total stays NULL so the shared loader falls
  //    through to total_receipts), direct breakdowns, outside totals and
  //    groups from the IE leg (an empty group list is a truthful zero — the
  //    leg ran), outside-group BREAKDOWNS untouched (undefined, never [] —
  //    partial-snapshot contract; the funders leg is PR 6).
  const link: GeorgiaFinanceLinkInput = {
    candidateId,
    electionId,
    electionYear,
    candidateNameNormalized: normalizeGeorgiaCandidateNameForStorage(candidateName),
    officeName,
    district: input.district ?? null,
    committeeId,
    committeeName,
    linkStatus: "active",
    linkSource: input.committee.linkSource ?? "manual",
    sourceUrl,
    lastVerifiedAt: syncedAt,
  };

  if (!dryRun) {
    await replaceGeorgiaCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary: {
        totalReceipts: indexTotalContributions,
        directContributionTotal: null,
        totalDisbursements: indexRow.totalExpenditures,
        cashOnHand: indexRow.cashOnHand,
        // Null when the leg was skipped — the preserveWhenNull policy keeps
        // the stored values.
        outsideSupportTotal: outsideSpending ? outsideSpending.supportTotal : null,
        outsideOpposeTotal: outsideSpending ? outsideSpending.opposeTotal : null,
        sourceUrl,
      },
      directBreakdowns: directFinance.directBreakdowns,
      // Undefined when the leg was skipped — stored groups stay untouched.
      outsideGroups: outsideSpending ? outsideSpending.outsideGroups : undefined,
    });
  }

  const { directBreakdowns, ...aggregation } = directFinance;
  const outsideSpendingDiagnostics = outsideSpending
    ? (({ outsideGroups: _outsideGroups, ...diagnostics }) => diagnostics)(outsideSpending)
    : null;
  return {
    candidateId,
    electionId,
    electionYear,
    dryRun,
    committeeId,
    linkWritten: !dryRun,
    summaryWritten: !dryRun,
    directBreakdownsWritten: dryRun ? 0 : directBreakdowns.length,
    outsideGroupsWritten: dryRun || !outsideSpending ? 0 : outsideSpending.outsideGroups.length,
    outsideSupportTotal: outsideSpending ? outsideSpending.supportTotal : null,
    outsideOpposeTotal: outsideSpending ? outsideSpending.opposeTotal : null,
    totalReceipts: indexTotalContributions,
    totalDisbursements: indexRow.totalExpenditures,
    cashOnHand: indexRow.cashOnHand,
    syncedRowSum: directFinance.syncedRowSum,
    reconciliationDifference: difference,
    reconciliationTolerance: tolerance,
    archiveRegistrationGuids,
    archiveRegistrationSource,
    reportInventorySize: inventory.length,
    peachfile: peachfileDiagnostics,
    archive: archiveDiagnostics,
    aggregation,
    outsideSpending: outsideSpendingDiagnostics,
    outsideSpendingSkippedReason,
  };
}
