// West Virginia finance batch sync: optional auto-link pass, then the due
// list, then a per-candidate cache-only sync. One registry fetch and one
// parse per cached artifact are shared across the whole batch (the
// memoizing loaders). Per-candidate failures are recorded and the batch
// continues (fail-visible, never fail-silent). Every step is injectable.

import type { Pool, PoolClient } from "pg";

import {
  autoLinkMissingWestVirginiaCandidateFinanceLinks,
  createWestVirginiaCommitteeRegistryLoader,
  type WestVirginiaFinanceAutoLinkResult,
} from "./westVirginiaCandidateFinanceAutoLink.js";
import {
  listDueWestVirginiaCandidateFinanceSyncRows,
  type WestVirginiaCandidateFinanceDueRow,
} from "./westVirginiaCandidateFinanceDueList.js";
import {
  createWestVirginiaFinanceArtifactLoader,
  syncWestVirginiaCandidateFinance,
  type WestVirginiaCandidateFinanceSyncResult,
} from "./westVirginiaCandidateFinanceSync.js";
import type { WestVirginiaCfrsClientOptions } from "./westVirginiaCfrsClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

const DEFAULT_MAX_CANDIDATES = 10;
const DEFAULT_STALE_AFTER_DAYS = 7;
// The cycle's last period (Q4) is due January 7 of the year after the
// election, and the nightly file for it lands a day later; ~70 days after a
// November general keeps a just-finished election in scope until then.
const WEST_VIRGINIA_POST_ELECTION_REPORT_DUE_DAYS = 70;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

export type WestVirginiaCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  dryRun?: boolean;
  autoLinkMissingLinks?: boolean;
  cacheDir?: string;
  clientOptions?: WestVirginiaCfrsClientOptions;
  log?: (message: string) => void;
  autoLinkFn?: typeof autoLinkMissingWestVirginiaCandidateFinanceLinks;
  listDueRowsFn?: typeof listDueWestVirginiaCandidateFinanceSyncRows;
  syncCandidateFn?: typeof syncWestVirginiaCandidateFinance;
};

export type WestVirginiaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  autoLinkResults: WestVirginiaFinanceAutoLinkResult[];
  totalDueRows: number;
  attempted: number;
  succeeded: number;
  failed: number;
  candidates: {
    row: WestVirginiaCandidateFinanceDueRow;
    ok: boolean;
    result?: WestVirginiaCandidateFinanceSyncResult;
    error?: string;
  }[];
};

export async function syncDueWestVirginiaCandidateFinance(
  input: WestVirginiaCandidateFinanceBatchSyncInput
): Promise<WestVirginiaCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  const log = input.log ?? ((message: string) => console.log(message));
  const maxCandidates = input.maxCandidates ?? DEFAULT_MAX_CANDIDATES;
  const staleAfterDays = input.staleAfterDays ?? DEFAULT_STALE_AFTER_DAYS;
  const electionLookbackDays =
    input.electionLookbackDays ?? WEST_VIRGINIA_POST_ELECTION_REPORT_DUE_DAYS + staleAfterDays + 1;
  const electionLookaheadDays = input.electionLookaheadDays ?? DEFAULT_ELECTION_LOOKAHEAD_DAYS;
  const dryRun = input.dryRun === true;

  let autoLinkResults: WestVirginiaFinanceAutoLinkResult[] = [];
  if (!dryRun && input.autoLinkMissingLinks !== false) {
    const autoLink = input.autoLinkFn ?? autoLinkMissingWestVirginiaCandidateFinanceLinks;
    try {
      autoLinkResults = await autoLink({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        loadRegistry: createWestVirginiaCommitteeRegistryLoader({ clientOptions: input.clientOptions }),
        clientOptions: input.clientOptions,
      });
    } catch (error) {
      log(
        `West Virginia auto-link pass failed (continuing with existing links): ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  const listDueRows = input.listDueRowsFn ?? listDueWestVirginiaCandidateFinanceSyncRows;
  const due = await listDueRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  // One parse per cached artifact for the whole batch.
  const loadArtifacts = createWestVirginiaFinanceArtifactLoader(input.cacheDir);
  const syncCandidate = input.syncCandidateFn ?? syncWestVirginiaCandidateFinance;
  const candidates: WestVirginiaCandidateFinanceBatchSyncResult["candidates"] = [];
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
          entityId: row.entityId,
          committeeName: row.committeeName,
          linkSource: row.linkSource,
          sourceUrl: row.sourceUrl,
        },
        now,
        dryRun,
        loadArtifacts,
      });
      succeeded += 1;
      candidates.push({ row, ok: true, result });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log(`West Virginia finance sync failed for ${row.candidateName} (entityId ${row.entityId}): ${message}`);
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
    candidates,
  };
}
