// Phase 3 batch sync: auto-link leg, shared due-list selection, and the
// per-candidate sync loop with error isolation (one bad candidate never
// aborts the batch).
//
// The due list is the shared standardStateFinanceDueListQuery — it roots in
// denver_candidate_finance_links, so only Denver-linked candidates can ever
// appear (the query's state filter is CO-wide, but no other Colorado place
// writes rows into the denver_ tables). Link identity is filer_id, so the
// config passes linkColumns + mapRow.
//
// Cycle binding (the Phase 2 rule, applied to sync): SearchLight cycles are
// bound to election dates via an explicit allowlist — v1 maps only
// DENVER_2026_VACANCY_ELECTION_DATE to cycle 36. A due candidate on any other
// date is SKIPPED (another cycle's work — extending the map is that phase's
// change), never guessed into cycle 36.

import type { Pool, PoolClient } from "pg";
import { createStandardStateFinanceDueListQuery } from "../finance/standardStateFinanceDueListQuery.js";
import {
  DENVER_2026_VACANCY_ELECTION_CYCLE_ID,
  DENVER_2026_VACANCY_ELECTION_DATE,
  DENVER_FINANCE_ELIGIBLE_OFFICE_NAMES,
} from "./denverFinanceEligibleOffices.js";
import {
  autoLinkMissingDenverCandidateFinanceLinks,
  listDenverCandidateElectionsMissingFinanceLinks,
  loadDenverRegistrantRecords,
  type DenverFinanceAutoLinkResult,
} from "./denverCandidateFinanceAutoLink.js";
import {
  syncDenverCandidateFinance,
  type DenverCandidateFinanceSyncResult,
} from "./denverCandidateFinanceSync.js";
import {
  getDenverCandidatesByElectionCycle,
  type DenverSearchlightClientOptions,
} from "./denverSearchlightClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type PoolLike = Queryable & { connect: () => Promise<PoolClient> };

/** "scope::canonical_name" keys for the shared due-list office filter. */
export const DENVER_FINANCE_ELIGIBLE_OFFICE_KEYS =
  DENVER_FINANCE_ELIGIBLE_OFFICE_NAMES.map((name) => `place::${name}`);

/** ISO election date -> SearchLight cycle id. v1 covers the 2026 vacancy only. */
export const DENVER_ELECTION_DATE_TO_CYCLE_ID: Readonly<Record<string, number>> =
  {
    [DENVER_2026_VACANCY_ELECTION_DATE]: DENVER_2026_VACANCY_ELECTION_CYCLE_ID,
  };

export type DenverCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district: string | null;
  filerId: number;
  committeeName: string;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueDenverCandidateFinanceSyncRows =
  createStandardStateFinanceDueListQuery({
    state: "CO",
    tables: {
      links: "denver_candidate_finance_links",
      summaries: "denver_candidate_finance_summaries",
    },
    eligibleOfficeKeys: DENVER_FINANCE_ELIGIBLE_OFFICE_KEYS,
    linkColumns: ["filer_id", "committee_name"],
    mapRow: (row): DenverCandidateFinanceDueRow => {
      // filer_id is digits-text by schema CHECK; a non-integer here is DB
      // corruption and must abort the batch rather than sync filer NaN.
      const filerId = Number(row.filer_id);
      if (!Number.isSafeInteger(filerId) || filerId <= 0)
        throw new Error(`Invalid Denver due-list filer id: ${String(row.filer_id)}`);
      return {
        candidateId: row.candidate_id,
        electionId: row.election_id,
        candidateName: row.candidate_name,
        electionYear: row.election_year,
        officeName: row.office_name,
        district: row.district,
        filerId,
        committeeName: String(row.committee_name),
        sourceUrl: row.source_url,
        lastSyncedAt: row.last_synced_at,
      };
    },
  });

export type DenverCandidateFinanceBatchItemResult = {
  candidateId: string;
  electionId: string;
  status: "synced" | "failed" | "skipped";
  reason?: string;
  result?: DenverCandidateFinanceSyncResult;
};

export type DenverCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  /** Null when the leg was skipped (dry run / autoLink=false / none missing). */
  autoLinkResults: DenverFinanceAutoLinkResult[] | null;
  autoLinkError: string | null;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  skippedCandidateCount: number;
  results: DenverCandidateFinanceBatchItemResult[];
};

export async function syncDueDenverCandidateFinance(input: {
  db: PoolLike;
  now?: Date;
  dryRun?: boolean;
  autoLink?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  bypassAnomalyCheck?: boolean;
  clientOptions?: DenverSearchlightClientOptions;
  syncFn?: typeof syncDenverCandidateFinance;
}): Promise<DenverCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime()))
    throw new Error("Invalid Denver finance batch sync timestamp");
  const dryRun = input.dryRun === true;
  const maxCandidates = input.maxCandidates ?? 25;
  const staleAfterDays = input.staleAfterDays ?? 1;
  // Post-election filings keep arriving (Denver's final reports land weeks
  // after election day), so the window looks back further than Houston's.
  const electionLookbackDays = input.electionLookbackDays ?? 45;
  const electionLookaheadDays = input.electionLookaheadDays ?? 400;
  const options = input.clientOptions ?? {};

  // --- Auto-link leg (skipped in dry runs: it writes link rows). A failure
  // here must not stop existing links from syncing.
  let autoLinkResults: DenverFinanceAutoLinkResult[] | null = null;
  let autoLinkError: string | null = null;
  if (!dryRun && input.autoLink !== false) {
    try {
      const missing = await listDenverCandidateElectionsMissingFinanceLinks(
        input.db,
        { now, maxCandidates, electionLookbackDays, electionLookaheadDays },
      );
      const inCycle = missing.filter(
        (candidate) =>
          candidate.electionDate === DENVER_2026_VACANCY_ELECTION_DATE,
      );
      if (inCycle.length > 0) {
        const registrants = await loadDenverRegistrantRecords(
          DENVER_2026_VACANCY_ELECTION_CYCLE_ID,
          options,
        );
        autoLinkResults = await autoLinkMissingDenverCandidateFinanceLinks({
          db: input.db,
          now,
          electionCycleId: DENVER_2026_VACANCY_ELECTION_CYCLE_ID,
          electionDate: DENVER_2026_VACANCY_ELECTION_DATE,
          candidates: inCycle,
          registrants,
        });
      }
    } catch (error) {
      autoLinkError = error instanceof Error ? error.message : String(error);
    }
  }

  // --- Due selection (shared query) + election dates for cycle binding. ---
  const due = await listDueDenverCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });
  const electionDates = new Map<string, string>();
  if (due.rows.length > 0) {
    const dateResult = await input.db.query<{
      id: string;
      election_date: string;
    }>(
      `SELECT id::text,election_date::text election_date FROM public.elections WHERE id=ANY($1::uuid[])`,
      [[...new Set(due.rows.map((row) => row.electionId))]],
    );
    for (const row of dateResult.rows)
      electionDates.set(row.id, row.election_date.slice(0, 10));
  }

  const results: DenverCandidateFinanceBatchItemResult[] = [];
  let cycleRegistrants:
    | Awaited<ReturnType<typeof getDenverCandidatesByElectionCycle>>
    | undefined;
  for (const row of due.rows) {
    const electionDate = electionDates.get(row.electionId);
    const electionCycleId =
      electionDate === undefined
        ? undefined
        : DENVER_ELECTION_DATE_TO_CYCLE_ID[electionDate];
    if (electionCycleId === undefined) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        status: "skipped",
        reason: `no SearchLight cycle mapping for election date ${electionDate ?? "unknown"}`,
      });
      continue;
    }
    try {
      // One registration-list fetch per run (v1 has a single cycle, so the
      // first synced candidate's list serves the rest).
      cycleRegistrants ??= await getDenverCandidatesByElectionCycle(
        electionCycleId,
        options,
      );
      const result = await (input.syncFn ?? syncDenverCandidateFinance)({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        candidateDisplayName: row.candidateName,
        officeName: row.officeName,
        district: row.district,
        filerId: row.filerId,
        committeeName: row.committeeName,
        electionCycleId,
        cycleRegistrants,
        bypassAnomalyCheck: input.bypassAnomalyCheck,
        dryRun,
        now,
        clientOptions: options,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        status: "synced",
        result,
      });
    } catch (error) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        status: "failed",
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return {
    dryRun,
    now: now.toISOString(),
    staleAfterDays,
    maxCandidates,
    autoLinkResults,
    autoLinkError,
    dueCandidateCount: due.totalDueRows,
    selectedCandidateCount: due.rows.length,
    syncedCandidateCount: results.filter((item) => item.status === "synced")
      .length,
    failedCandidateCount: results.filter((item) => item.status === "failed")
      .length,
    skippedCandidateCount: results.filter((item) => item.status === "skipped")
      .length,
    results,
  };
}
