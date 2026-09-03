// Idaho finance batch sync (docs/plans/idaho-finance.md, Phase 3): optional
// auto-link pass, then the due list, then per-link sync. The candidate grid
// (one 5,000-row page) and the all-time IE list (one 10,000-row page) are
// pulled ONCE per batch and shared: the auto-link resolves against the grid,
// and every link's outside money is selected from the same IE list. A failed
// IE pull never blocks the direct leg — links sync with expenditureRows:
// null, which preserves any prior outside snapshot. Per-link failures are
// recorded and the batch continues (fail-visible, never fail-silent).
//
// Live API only: Idaho's raw-refresh flag gates nothing here (there is no
// artifact-first path — see idahoCandidateFinanceSync.ts); the sync flag is
// checked by the CLI before this runs.

import type { Pool, PoolClient } from "pg";

import {
  autoLinkMissingIdahoCandidateFinanceLinks,
  IDAHO_CFS_GRID_PAGE_SIZE,
  type IdahoFinanceAutoLinkResult,
} from "./idahoCandidateFinanceAutoLink.js";
import {
  listDueIdahoCandidateFinanceSyncRows,
  type IdahoCandidateFinanceDueRow,
} from "./idahoCandidateFinanceDueList.js";
import {
  IDAHO_CFS_INDEPENDENT_EXPENDITURE_PAGE_SIZE,
  mergeIdahoCfsDataClient,
  syncIdahoCandidateFinance,
  type IdahoCandidateFinanceSyncResult,
  type IdahoCfsDataClient,
} from "./idahoCandidateFinanceSync.js";
import type {
  IdahoCandidateRegistrationRow,
  IdahoCfsClientOptions,
  IdahoIndependentExpenditureRow,
} from "./idahoCfsClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

// One HTTP call per link (the contribution page), so a batch is cheap; 25 is
// the fleet default (finance-sync runbook).
const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
// Idaho's last post-general filing is the annual report due January 31
// (67-6607), 89 days after a November 3 general; one stale interval beyond
// that keeps the just-run election in scope until it has been picked up.
// With the default staleness this is 98 days — the auto-link CLI's default.
const IDAHO_POST_ELECTION_REPORT_DUE_DAYS = 90;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

export type IdahoCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  autoLinkMissingLinks?: boolean;
  cacheDir?: string;
  cfsClient?: Partial<IdahoCfsDataClient>;
  cfsClientOptions?: IdahoCfsClientOptions;
  log?: (message: string) => void;
  autoLinkFn?: typeof autoLinkMissingIdahoCandidateFinanceLinks;
  listDueRowsFn?: typeof listDueIdahoCandidateFinanceSyncRows;
  syncCandidateFn?: typeof syncIdahoCandidateFinance;
};

export type IdahoCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  autoLinkResults: IdahoFinanceAutoLinkResult[];
  totalDueRows: number;
  attempted: number;
  succeeded: number;
  failed: number;
  /** Grid rows pulled this batch; null when nothing needed the grid. */
  registrationCount: number | null;
  /** IE rows pulled this batch; null when the pull failed or was not needed. */
  independentExpenditureRowCount: number | null;
  independentExpenditureError: string | null;
  candidates: {
    row: IdahoCandidateFinanceDueRow;
    ok: boolean;
    result?: IdahoCandidateFinanceSyncResult;
    error?: string;
  }[];
};

function positiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isSafeInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Idaho finance batch ${label}: ${value}`);
  }
  return normalized;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export async function syncDueIdahoCandidateFinance(
  input: IdahoCandidateFinanceBatchSyncInput
): Promise<IdahoCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new Error("Invalid Idaho finance batch timestamp");
  }
  const log = input.log ?? ((message: string) => console.log(message));
  const maxCandidates = positiveInteger(input.maxCandidates, DEFAULT_MAX_CANDIDATES, "maxCandidates");
  const staleAfterDays = positiveInteger(input.staleAfterDays, DEFAULT_STALE_AFTER_DAYS, "staleAfterDays");
  const electionLookbackDays = positiveInteger(
    input.electionLookbackDays,
    IDAHO_POST_ELECTION_REPORT_DUE_DAYS + staleAfterDays + 1,
    "electionLookbackDays"
  );
  const electionLookaheadDays = positiveInteger(
    input.electionLookaheadDays,
    DEFAULT_ELECTION_LOOKAHEAD_DAYS,
    "electionLookaheadDays"
  );
  const dryRun = input.dryRun === true;
  const cfsClient = mergeIdahoCfsDataClient(input.cfsClient);

  // The grid is one request either way; pulled at most once per batch.
  let registrations: IdahoCandidateRegistrationRow[] | null = null;
  let registrationCount: number | null = null;
  const loadRegistrations = async (): Promise<IdahoCandidateRegistrationRow[]> => {
    registrations ??= await cfsClient.getRegistrations({ pageSize: IDAHO_CFS_GRID_PAGE_SIZE }, input.cfsClientOptions);
    registrationCount = registrations.length;
    return registrations;
  };

  let autoLinkResults: IdahoFinanceAutoLinkResult[] = [];
  if (!dryRun && input.autoLinkMissingLinks !== false) {
    const autoLink = input.autoLinkFn ?? autoLinkMissingIdahoCandidateFinanceLinks;
    try {
      autoLinkResults = await autoLink({
        db: input.db,
        now,
        maxCandidates: null,
        electionLookbackDays,
        electionLookaheadDays,
        registrations: await loadRegistrations(),
        clientOptions: input.cfsClientOptions,
      });
    } catch (error) {
      log(`Idaho auto-link pass failed (continuing with existing links): ${errorMessage(error)}`);
    }
  }

  const listDueRows = input.listDueRowsFn ?? listDueIdahoCandidateFinanceSyncRows;
  const due = await listDueRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  // One IE pull per batch. A failure skips the outside leg for every link
  // (expenditureRows: null) rather than failing the batch.
  let expenditureRowCount: number | null = null;
  let expenditureError: string | null = null;
  let expenditureRows: readonly IdahoIndependentExpenditureRow[] | null = null;
  if (due.rows.length > 0) {
    try {
      expenditureRows = await cfsClient.getIndependentExpenditures(
        { pageSize: IDAHO_CFS_INDEPENDENT_EXPENDITURE_PAGE_SIZE },
        input.cfsClientOptions
      );
      expenditureRowCount = expenditureRows.length;
    } catch (error) {
      expenditureError = errorMessage(error);
      log(`Idaho IE list pull failed (direct sync continues, outside leg skipped): ${expenditureError}`);
    }
  }

  const syncCandidate = input.syncCandidateFn ?? syncIdahoCandidateFinance;
  const candidates: IdahoCandidateFinanceBatchSyncResult["candidates"] = [];
  let succeeded = 0;
  for (const row of due.rows) {
    try {
      const result = await syncCandidate({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeScope: row.officeScope,
        officeName: row.officeName,
        district: row.district,
        link: {
          registrationGuid: row.registrationGuid,
          filerName: row.filerName,
          linkSource: row.linkSource,
          sourceUrl: row.sourceUrl,
        },
        registrations: await loadRegistrations(),
        expenditureRows,
        cfsClient,
        cfsClientOptions: input.cfsClientOptions,
        cacheDir: input.cacheDir,
        now,
        dryRun,
      });
      succeeded += 1;
      if (result.directCoverageNote !== null) {
        log(`Idaho coverage for ${row.candidateName} (${row.registrationGuid}): ${result.directCoverageNote}`);
      }
      candidates.push({ row, ok: true, result });
    } catch (error) {
      const message = errorMessage(error);
      log(`Idaho finance sync failed for ${row.candidateName} (${row.registrationGuid}): ${message}`);
      candidates.push({ row, ok: false, error: message });
    }
  }

  return {
    dryRun,
    autoLinkResults,
    totalDueRows: due.totalDueRows,
    attempted: due.rows.length,
    succeeded,
    failed: due.rows.length - succeeded,
    registrationCount,
    independentExpenditureRowCount: expenditureRowCount,
    independentExpenditureError: expenditureError,
    candidates,
  };
}
