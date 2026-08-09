import type { Pool, PoolClient } from "pg";

import {
  autoLinkMissingGeorgiaCandidateFinanceLinks,
  listGeorgiaCandidateElectionsMissingFinanceLinks,
  type GeorgiaCandidateCommitteeResolver,
  type GeorgiaFinanceAutoLinkCandidateElection,
} from "./georgiaCandidateFinanceAutoLink.js";
import { listDueGeorgiaCandidateFinanceSyncRows } from "./georgiaCandidateFinanceDueList.js";
import {
  syncGeorgiaCandidateFinance,
  type GeorgiaCandidateFinanceSyncResult,
} from "./georgiaCandidateFinanceSync.js";
import {
  createGeorgiaEthicsTransport,
  fetchGeorgiaIndependentExpenditureRows,
  GeorgiaEthicsClientError,
  type GeorgiaEthicsTransport,
  type GeorgiaIndependentExpenditureRow,
} from "./georgiaEthicsClient.js";

// Batch layer for Georgia candidate finance (georgia_plan.md PR 4/PR 5,
// tennessee shape): auto-link missing links first (fail-open — a broken
// auto-link must not block syncing already-linked candidates), fetch the
// shared PeachFile IE store once, then run the due list stalest-first and
// sync each candidate independently.

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type GeorgiaCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  // Shared transport for every request in the run — the client's politeness
  // rules (single flight, spacing) only hold when all calls share one.
  transport?: GeorgiaEthicsTransport;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  autoLinkMissingLinks?: boolean;
  windowDays?: number;
  maxPasses?: number;
  reconciliationRelativeTolerance?: number;
  reconciliationAbsoluteToleranceFloor?: number;
  maxOutsideGroups?: number;
  syncGeorgiaCandidateFinanceFn?: typeof syncGeorgiaCandidateFinance;
  fetchIndependentExpenditureRowsFn?: typeof fetchGeorgiaIndependentExpenditureRows;
  resolveCandidateCommittee?: GeorgiaCandidateCommitteeResolver;
};

export type GeorgiaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: GeorgiaCandidateFinanceSyncResult;
  error?: string;
};

export type GeorgiaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  autoLinkAttemptedCount: number;
  autoLinkLinkedCount: number;
  // Set when the shared IE store pull failed and every candidate ran
  // direct-only (stored outside data preserved).
  independentExpenditureStoreError: string | null;
  results: GeorgiaCandidateFinanceBatchSyncItemResult[];
};

// Georgia candidates cost hundreds of paced requests each (both hosts,
// windowed passes plus the sweep), so the per-run default stays small.
const DEFAULT_MAX_CANDIDATES = 10;
const DEFAULT_STALE_AFTER_DAYS = 7;
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid Georgia finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Georgia finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

export async function syncDueGeorgiaCandidateFinance(
  input: GeorgiaCandidateFinanceBatchSyncInput
): Promise<GeorgiaCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  assertValidDate(now, "now");
  const maxCandidates = normalizePositiveInteger(input.maxCandidates, DEFAULT_MAX_CANDIDATES, "maxCandidates");
  const staleAfterDays = normalizePositiveInteger(input.staleAfterDays, DEFAULT_STALE_AFTER_DAYS, "staleAfterDays");
  const electionLookbackDays = normalizePositiveInteger(
    input.electionLookbackDays,
    DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS,
    "electionLookbackDays"
  );
  const electionLookaheadDays = normalizePositiveInteger(
    input.electionLookaheadDays,
    DEFAULT_ELECTION_LOOKAHEAD_DAYS,
    "electionLookaheadDays"
  );
  const dryRun = input.dryRun === true;
  const shouldAutoLinkMissingLinks = !dryRun && input.autoLinkMissingLinks !== false;
  const transport = input.transport ?? createGeorgiaEthicsTransport();
  const syncFn = input.syncGeorgiaCandidateFinanceFn ?? syncGeorgiaCandidateFinance;
  let autoLinkAttemptedCount = 0;
  let autoLinkLinkedCount = 0;

  if (shouldAutoLinkMissingLinks) {
    try {
      const missingLinkCandidates: GeorgiaFinanceAutoLinkCandidateElection[] =
        await listGeorgiaCandidateElectionsMissingFinanceLinks(input.db, {
          now,
          maxCandidates,
          electionLookbackDays,
          electionLookaheadDays,
        });
      autoLinkAttemptedCount = missingLinkCandidates.length;
      const autoLinkResults = await autoLinkMissingGeorgiaCandidateFinanceLinks({
        db: input.db,
        transport,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        candidateElections: missingLinkCandidates,
        resolveCandidateCommittee: input.resolveCandidateCommittee,
      });
      autoLinkLinkedCount = autoLinkResults.filter((result) => result.status === "linked").length;
      for (const result of autoLinkResults) {
        if (result.status !== "linked") {
          console.warn("Georgia finance auto-link did not link candidate election:", result);
        }
      }
    } catch (error) {
      console.warn(
        "Georgia finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueGeorgiaCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  // The PeachFile IE store (F5) is candidate-independent, so one paced pull
  // serves every candidate in the run instead of one per candidate. A client
  // failure here (network, WAF, unstable paging, the empty-store guard) must
  // not block the direct-finance refreshes: the run degrades to direct-only
  // with the NULL sentinel — the syncs skip the outside leg and preserve
  // stored outside data instead of each retrying a known-dead fetch.
  // Anything that is not a client error is a bug and still throws.
  let independentExpenditureRows: readonly GeorgiaIndependentExpenditureRow[] | null | undefined;
  let independentExpenditureStoreError: string | null = null;
  if (due.rows.length > 0) {
    const fetchIeFn = input.fetchIndependentExpenditureRowsFn ?? fetchGeorgiaIndependentExpenditureRows;
    try {
      independentExpenditureRows = (await fetchIeFn(transport, "peachfile", { maxPasses: input.maxPasses })).rows;
    } catch (error) {
      if (!(error instanceof GeorgiaEthicsClientError)) {
        throw error;
      }
      independentExpenditureStoreError = error.message;
      independentExpenditureRows = null;
      console.warn(
        "Georgia IE store fetch failed; syncing direct-only and preserving stored outside data:",
        error.message
      );
    }
  }

  const results: GeorgiaCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    try {
      const result = await syncFn({
        db: input.db,
        transport,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeName: row.officeName,
        district: row.district,
        committee: {
          committeeId: row.committeeId,
          committeeName: row.committeeName,
          linkSource: row.linkSource,
          sourceUrl: row.sourceUrl,
        },
        now,
        dryRun,
        windowDays: input.windowDays,
        maxPasses: input.maxPasses,
        reconciliationRelativeTolerance: input.reconciliationRelativeTolerance,
        reconciliationAbsoluteToleranceFloor: input.reconciliationAbsoluteToleranceFloor,
        maxOutsideGroups: input.maxOutsideGroups,
        independentExpenditureRows,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        committeeId: row.committeeId,
        ok: true,
        result,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Georgia finance sync failed for candidate; continuing:", {
        candidateId: row.candidateId,
        electionId: row.electionId,
        committeeId: row.committeeId,
        error: message,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        committeeId: row.committeeId,
        ok: false,
        error: message,
      });
    }
  }

  return {
    dryRun,
    now: now.toISOString(),
    staleAfterDays,
    maxCandidates,
    dueCandidateCount: due.totalDueRows,
    selectedCandidateCount: due.rows.length,
    syncedCandidateCount: results.filter((result) => result.ok).length,
    failedCandidateCount: results.filter((result) => !result.ok).length,
    autoLinkAttemptedCount,
    autoLinkLinkedCount,
    independentExpenditureStoreError,
    results,
  };
}
