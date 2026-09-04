// New Hampshire finance batch sync (Phase 3): optional auto-link pass, then
// the due list, then per-link sync. The per-candidate sync
// (newHampshireCandidateFinanceSync.ts) keeps its contract: it takes the
// candidate's race facts plus a numeric election-cycle ID, re-resolves the
// filer against the live filing-entity registry, and pulls that filer's
// receipts and the cycle's IE list itself. What the batch adds is sharing: the
// cycle list, the filing-entity registry, and the IE list are each pulled ONCE
// per cycle per batch through a memoizing client handed to the auto-link and
// to every sync call. Per-link failures are recorded and the batch continues
// (fail-visible, never fail-silent); a link whose filer no longer resolves is
// a failure too, because nothing was written and it would otherwise stay
// silently due forever.
//
// Live API only: the sync reads the search API, not the bulk CSV artifacts the
// raw-refresh script caches, so nothing here touches that cache. The CLI
// checks the flags before this runs.

import type { Pool, PoolClient } from "pg";

import {
  autoLinkMissingNewHampshireCandidateFinanceLinks,
  DEFAULT_NEW_HAMPSHIRE_CFS_REGISTRY_CLIENT,
  resolveNewHampshireElectionCycleId,
  type NewHampshireCfsRegistryClient,
  type NewHampshireFinanceAutoLinkResult,
} from "./newHampshireCandidateFinanceAutoLink.js";
import {
  listDueNewHampshireCandidateFinanceSyncRows,
  type NewHampshireCandidateFinanceDueRow,
} from "./newHampshireCandidateFinanceDueList.js";
import { resolveNewHampshireCandidateFiler } from "./newHampshireCandidateFilerResolver.js";
import {
  syncNewHampshireCandidateFinance,
  type NewHampshireCandidateFinanceSyncResult,
  type NewHampshireCfsDataClient,
} from "./newHampshireCandidateFinanceSync.js";
import {
  getAllNewHampshireIndependentExpenditures,
  getAllNewHampshireReceipts,
  type NewHampshireCfsClientOptions,
  type NewHampshireElectionCycle,
  type NewHampshireFilingEntityRow,
  type NewHampshireIndependentExpenditureRow,
} from "./newHampshireCfsClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

/** Everything the batch needs from the CFS: the sync's client plus the registry calls. */
export type NewHampshireCfsBatchClient = NewHampshireCfsDataClient & NewHampshireCfsRegistryClient;

const DEFAULT_BATCH_CLIENT: NewHampshireCfsBatchClient = {
  ...DEFAULT_NEW_HAMPSHIRE_CFS_REGISTRY_CLIENT,
  getReceipts: getAllNewHampshireReceipts,
  getIndependentExpenditures: getAllNewHampshireIndependentExpenditures,
};

// A few HTTP calls per link (filer receipts; the registry and IE list are
// shared), so a batch is cheap; 25 is the fleet default (finance-sync runbook).
const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
// New Hampshire's last post-general receipts-and-expenditures report of the
// 2026 cycle is due November 25 (backend/docs/new-hampshire-campaign-finance.md),
// 22 days after the November 3 general; one stale interval beyond that keeps
// the just-run election in scope until it has been picked up. With the
// default staleness this is 30 days — the auto-link CLI's default.
const NEW_HAMPSHIRE_POST_ELECTION_REPORT_DUE_DAYS = 22;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

export type NewHampshireCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  autoLinkMissingLinks?: boolean;
  cfsClient?: Partial<NewHampshireCfsBatchClient>;
  cfsClientOptions?: NewHampshireCfsClientOptions;
  log?: (message: string) => void;
  autoLinkFn?: typeof autoLinkMissingNewHampshireCandidateFinanceLinks;
  listDueRowsFn?: typeof listDueNewHampshireCandidateFinanceSyncRows;
  syncCandidateFn?: typeof syncNewHampshireCandidateFinance;
};

export type NewHampshireCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  autoLinkResults: NewHampshireFinanceAutoLinkResult[];
  /** Message of a thrown auto-link pass (the batch continued with existing links); null when the pass ran or was skipped. */
  autoLinkError: string | null;
  totalDueRows: number;
  attempted: number;
  succeeded: number;
  failed: number;
  /** Election year -> CFS cycle ID resolved this batch (only years that were needed). */
  electionCycleIds: Record<string, number>;
  candidates: {
    row: NewHampshireCandidateFinanceDueRow;
    ok: boolean;
    result?: NewHampshireCandidateFinanceSyncResult;
    error?: string;
  }[];
};

function positiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isSafeInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Hampshire finance batch ${label}: ${value}`);
  }
  return normalized;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function memoizeByCycle<T>(
  cache: Map<number, Promise<T>>,
  electionCycleId: number,
  load: () => Promise<T>
): Promise<T> {
  let pending = cache.get(electionCycleId);
  if (!pending) {
    pending = load();
    cache.set(electionCycleId, pending);
  }
  return pending;
}

/**
 * Wraps a CFS client so the cycle list, each cycle's filing-entity registry,
 * and each cycle's IE list are requested at most once. Promises are memoized,
 * rejection included, so an outage costs one timeout, not one per link.
 * Receipts are per filer and pass straight through.
 */
export function createSharedNewHampshireCfsBatchClient(
  client: Partial<NewHampshireCfsBatchClient> | undefined
): NewHampshireCfsBatchClient {
  const base: NewHampshireCfsBatchClient = { ...DEFAULT_BATCH_CLIENT, ...(client ?? {}) };
  let cyclesPromise: Promise<NewHampshireElectionCycle[]> | null = null;
  const filingEntitiesByCycle = new Map<number, Promise<NewHampshireFilingEntityRow[]>>();
  const expendituresByCycle = new Map<number, Promise<NewHampshireIndependentExpenditureRow[]>>();
  return {
    getElectionCycles: (options) => (cyclesPromise ??= base.getElectionCycles(options)),
    getFilingEntities: (input, options) =>
      memoizeByCycle(filingEntitiesByCycle, input.electionCycleId, () => base.getFilingEntities(input, options)),
    getReceipts: (input, options) => base.getReceipts(input, options),
    getIndependentExpenditures: (input, options) =>
      memoizeByCycle(expendituresByCycle, input.electionCycleId, () => base.getIndependentExpenditures(input, options)),
  };
}

/**
 * Picks the candidate spelling the per-candidate sync must resolve with: the
 * first of the due row's spellings that resolves (any registration status,
 * exactly as the sync resolves) to the linked filing entity. Pure — it reads
 * the registry rows it is given. Throws, with every attempt listed, when no
 * spelling lands on the linked filer, so the batch records the link as failed
 * without calling the sync (and without writing another filer's money).
 */
export function chooseSyncCandidateName(input: {
  row: NewHampshireCandidateFinanceDueRow;
  electionCycleId: number;
  filingEntityRows: readonly NewHampshireFilingEntityRow[];
}): string {
  const attempts: string[] = [];
  for (const candidateName of input.row.candidateNames) {
    const resolution = resolveNewHampshireCandidateFiler({
      candidateName,
      officeScope: input.row.officeScope,
      officeName: input.row.officeName,
      district: input.row.district,
      electionCycleId: input.electionCycleId,
      filingEntityRows: input.filingEntityRows,
      sourceUrl: input.row.sourceUrl,
    });
    if (resolution.status === "matched" && resolution.filingEntityId === input.row.filingEntityId) {
      return candidateName;
    }
    attempts.push(
      `${JSON.stringify(candidateName)} -> ${
        resolution.status === "matched" ? `filer ${resolution.filingEntityId}` : `${resolution.status}: ${resolution.reason}`
      }`
    );
  }
  throw new Error(`no candidate spelling resolves to linked filer ${input.row.filingEntityId} (${attempts.join("; ")})`);
}

export async function syncDueNewHampshireCandidateFinance(
  input: NewHampshireCandidateFinanceBatchSyncInput
): Promise<NewHampshireCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new Error("Invalid New Hampshire finance batch timestamp");
  }
  const log = input.log ?? ((message: string) => console.log(message));
  const maxCandidates = positiveInteger(input.maxCandidates, DEFAULT_MAX_CANDIDATES, "maxCandidates");
  const staleAfterDays = positiveInteger(input.staleAfterDays, DEFAULT_STALE_AFTER_DAYS, "staleAfterDays");
  const electionLookbackDays = positiveInteger(
    input.electionLookbackDays,
    NEW_HAMPSHIRE_POST_ELECTION_REPORT_DUE_DAYS + staleAfterDays + 1,
    "electionLookbackDays"
  );
  const electionLookaheadDays = positiveInteger(
    input.electionLookaheadDays,
    DEFAULT_ELECTION_LOOKAHEAD_DAYS,
    "electionLookaheadDays"
  );
  const dryRun = input.dryRun === true;
  const cfsClient = createSharedNewHampshireCfsBatchClient(input.cfsClient);

  let autoLinkResults: NewHampshireFinanceAutoLinkResult[] = [];
  let autoLinkError: string | null = null;
  if (!dryRun && input.autoLinkMissingLinks !== false) {
    const autoLink = input.autoLinkFn ?? autoLinkMissingNewHampshireCandidateFinanceLinks;
    try {
      autoLinkResults = await autoLink({
        db: input.db,
        now,
        maxCandidates: null,
        electionLookbackDays,
        electionLookaheadDays,
        cfsClient,
        cfsClientOptions: input.cfsClientOptions,
      });
    } catch (error) {
      autoLinkError = errorMessage(error);
      log(`New Hampshire auto-link pass failed (continuing with existing links): ${autoLinkError}`);
    }
  }

  const listDueRows = input.listDueRowsFn ?? listDueNewHampshireCandidateFinanceSyncRows;
  const due = await listDueRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });
  const electionCycleIds: Record<string, number> = {};
  if (due.rows.length === 0) {
    return {
      dryRun,
      autoLinkResults,
      autoLinkError,
      totalDueRows: due.totalDueRows,
      attempted: 0,
      succeeded: 0,
      failed: 0,
      electionCycleIds,
      candidates: [],
    };
  }

  const syncCandidate = input.syncCandidateFn ?? syncNewHampshireCandidateFinance;
  const candidates: NewHampshireCandidateFinanceBatchSyncResult["candidates"] = [];
  let succeeded = 0;
  for (const row of due.rows) {
    const label = `${row.candidateName} (${row.filingEntityId})`;
    try {
      const electionCycleId = resolveNewHampshireElectionCycleId({
        cycles: await cfsClient.getElectionCycles(input.cfsClientOptions),
        electionYear: row.electionYear,
      });
      electionCycleIds[String(row.electionYear)] = electionCycleId;
      // The sync resolves one spelling against the live registry and writes
      // whatever filer that spelling resolves to. Choose the spelling here,
      // against the same memoized registry the sync is about to read, and
      // require it to land on the linked filer — so a link the auto-link made
      // from the structured name still syncs, and a spelling that now resolves
      // to a different filer never writes that filer's money under this link.
      const candidateName = chooseSyncCandidateName({
        row,
        electionCycleId,
        filingEntityRows: await cfsClient.getFilingEntities({ electionCycleId }, input.cfsClientOptions),
      });
      const result = await syncCandidate({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName,
        electionYear: row.electionYear,
        electionCycleId,
        officeScope: row.officeScope,
        officeName: row.officeName,
        district: row.district,
        sourceUrl: row.sourceUrl,
        cfsClient,
        cfsClientOptions: input.cfsClientOptions,
        now,
        dryRun,
      });
      if (result.resolution.status !== "matched") {
        throw new Error(`filer resolution ${result.resolution.status}: ${result.resolution.reason}`);
      }
      if (result.resolution.filingEntityId !== row.filingEntityId) {
        throw new Error(`filer resolution landed on ${result.resolution.filingEntityId}, link holds ${row.filingEntityId}`);
      }
      // The sync tolerates one failed section (the writer preserves the other),
      // but when both fail it writes nothing and the link stays due; that must
      // count as a failure or the run reads green.
      if (result.directAggregation === null && result.outsideAggregation === null) {
        throw new Error(
          `nothing written: direct ${result.directSkippedReason ?? "unavailable"}; outside ${result.outsideSkippedReason ?? "unavailable"}`
        );
      }
      succeeded += 1;
      if (result.directSkippedReason !== null) {
        log(`New Hampshire direct money skipped for ${label}: ${result.directSkippedReason}`);
      }
      if (result.outsideSkippedReason !== null) {
        log(`New Hampshire outside money skipped for ${label}: ${result.outsideSkippedReason}`);
      }
      candidates.push({ row, ok: true, result });
    } catch (error) {
      const message = errorMessage(error);
      log(`New Hampshire finance sync failed for ${label}: ${message}`);
      candidates.push({ row, ok: false, error: message });
    }
  }

  return {
    dryRun,
    autoLinkResults,
    autoLinkError,
    totalDueRows: due.totalDueRows,
    attempted: due.rows.length,
    succeeded,
    failed: due.rows.length - succeeded,
    electionCycleIds,
    candidates,
  };
}
