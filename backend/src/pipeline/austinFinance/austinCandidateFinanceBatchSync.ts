// Phase 3 batch sync: auto-link leg, shared due-list selection, and the
// per-candidate sync loop with error isolation (one bad candidate never
// aborts the batch). Denver copy.
//
// The due list is the shared standardStateFinanceDueListQuery — it roots in
// atx_candidate_finance_links, so only Austin-linked candidates can ever
// appear (the query's state filter is TX-wide and the office keys are the
// same names Houston uses, but no other Texas place writes rows into the
// atx_ tables). Link identity is filer_key/filer_name, so the config passes
// linkColumns + mapRow.
//
// Election binding (the Phase 2 rule, applied to sync): a due candidate is
// synced only when its election date is on the v1 allowlist and its
// election yields an office code (canonical office + ballot title). Anything
// else is SKIPPED with a reason — another cycle's work, never guessed.

import type { Pool, PoolClient } from "pg";
import { createStandardStateFinanceDueListQuery } from "../finance/standardStateFinanceDueListQuery.js";
import {
  autoLinkMissingAustinCandidateFinanceLinks,
  listAustinCandidateElectionsMissingFinanceLinks,
  loadAustinReportFilers,
  type AustinFinanceAutoLinkResult,
} from "./austinCandidateFinanceAutoLink.js";
import {
  loadAustinOutsideDatasets,
  syncAustinCandidateFinance,
  type AustinCandidateFinanceSyncResult,
  type AustinOutsideDatasets,
} from "./austinCandidateFinanceSync.js";
import {
  AUSTIN_FINANCE_ELECTION_DATES,
  AUSTIN_FINANCE_ELIGIBLE_OFFICE_NAMES,
  austinOfficeCodeForElection,
  isAustinFinanceSupportedElectionDate,
  type AustinOfficeCode,
} from "./austinFinanceEligibleOffices.js";
import {
  defaultAustinSocrataClientOptions,
  type AustinSocrataClientOptions,
} from "./austinSocrataClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type PoolLike = Queryable & { connect: () => Promise<PoolClient> };

/** "scope::canonical_name" keys for the shared due-list office filter. */
export const AUSTIN_FINANCE_ELIGIBLE_OFFICE_KEYS =
  AUSTIN_FINANCE_ELIGIBLE_OFFICE_NAMES.map((name) => `place::${name}`);

export type AustinCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district: string | null;
  filerKey: string;
  filerName: string;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export const listDueAustinCandidateFinanceSyncRows =
  createStandardStateFinanceDueListQuery({
    state: "TX",
    tables: {
      links: "atx_candidate_finance_links",
      summaries: "atx_candidate_finance_summaries",
    },
    eligibleOfficeKeys: AUSTIN_FINANCE_ELIGIBLE_OFFICE_KEYS,
    linkColumns: ["filer_key", "filer_name"],
    mapRow: (row): AustinCandidateFinanceDueRow => {
      // Both are NOT NULL + non-blank by schema CHECK; a blank here is DB
      // corruption and must abort the batch rather than sync filer "".
      const filerKey = String(row.filer_key ?? "").trim();
      const filerName = String(row.filer_name ?? "").trim();
      if (!filerKey || !filerName)
        throw new Error(
          `Invalid Austin due-list filer identity: key=${JSON.stringify(row.filer_key)} name=${JSON.stringify(row.filer_name)}`,
        );
      return {
        candidateId: row.candidate_id,
        electionId: row.election_id,
        candidateName: row.candidate_name,
        electionYear: row.election_year,
        officeName: row.office_name,
        district: row.district,
        filerKey,
        filerName,
        sourceUrl: row.source_url,
        lastSyncedAt: row.last_synced_at,
      };
    },
  });

export type AustinCandidateFinanceBatchItemResult = {
  candidateId: string;
  electionId: string;
  status: "synced" | "failed" | "skipped";
  reason?: string;
  result?: AustinCandidateFinanceSyncResult;
};

export type AustinCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  /** Null when the leg was skipped (dry run / autoLink=false / none missing). */
  autoLinkResults: AustinFinanceAutoLinkResult[] | null;
  autoLinkError: string | null;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  skippedCandidateCount: number;
  results: AustinCandidateFinanceBatchItemResult[];
};

/** Election facts the sync needs beyond the link row. */
type ElectionFacts = { electionDate: string; officeCode: AustinOfficeCode | null };

async function loadElectionFacts(
  db: Queryable,
  electionIds: readonly string[],
): Promise<Map<string, ElectionFacts>> {
  const facts = new Map<string, ElectionFacts>();
  if (electionIds.length === 0) return facts;
  const result = await db.query<{
    id: string;
    election_date: string;
    office_name: string | null;
    official_ballot_title: string | null;
  }>(
    `SELECT election.id::text,election.election_date::text election_date,office.canonical_name office_name,election.official_ballot_title FROM public.elections election LEFT JOIN public.offices office ON office.id=election.office_id WHERE election.id=ANY($1::uuid[])`,
    [[...new Set(electionIds)]],
  );
  for (const row of result.rows)
    facts.set(row.id, {
      electionDate: row.election_date.slice(0, 10),
      officeCode: austinOfficeCodeForElection({
        officeCanonicalName: row.office_name,
        officialBallotTitle: row.official_ballot_title,
      }),
    });
  return facts;
}

export async function syncDueAustinCandidateFinance(input: {
  db: PoolLike;
  now?: Date;
  dryRun?: boolean;
  autoLink?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  bypassAnomalyCheck?: boolean;
  clientOptions?: AustinSocrataClientOptions;
  syncFn?: typeof syncAustinCandidateFinance;
  /** Test seam for the city-wide DCE + purpose fetch. */
  loadOutsideDatasetsFn?: typeof loadAustinOutsideDatasets;
}): Promise<AustinCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime()))
    throw new Error("Invalid Austin finance batch sync timestamp");
  const dryRun = input.dryRun === true;
  const maxCandidates = input.maxCandidates ?? 25;
  const staleAfterDays = input.staleAfterDays ?? 1;
  // Post-election filings keep arriving (the January semiannual lands ~10
  // weeks after a November election), so the window looks back that far.
  const electionLookbackDays = input.electionLookbackDays ?? 90;
  const electionLookaheadDays = input.electionLookaheadDays ?? 400;
  const options = input.clientOptions ?? defaultAustinSocrataClientOptions();

  // --- Auto-link leg (skipped in dry runs: it writes link rows). A failure
  // here must not stop existing links from syncing.
  let autoLinkResults: AustinFinanceAutoLinkResult[] | null = null;
  let autoLinkError: string | null = null;
  if (!dryRun && input.autoLink !== false) {
    try {
      const missing = await listAustinCandidateElectionsMissingFinanceLinks(
        input.db,
        { electionDates: AUSTIN_FINANCE_ELECTION_DATES, maxCandidates },
      );
      if (missing.length > 0) {
        autoLinkResults = [];
        // One filer fetch per election date present (the resolver's picture
        // is per Report Detail election tag).
        for (const electionDate of new Set(missing.map((row) => row.electionDate))) {
          const filers = await loadAustinReportFilers(electionDate, options);
          autoLinkResults.push(
            ...(await autoLinkMissingAustinCandidateFinanceLinks({
              db: input.db,
              now,
              electionDate,
              candidates: missing.filter((row) => row.electionDate === electionDate),
              filers,
            })),
          );
        }
      }
    } catch (error) {
      autoLinkError = error instanceof Error ? error.message : String(error);
    }
  }

  // --- Due selection (shared query) + election facts for cycle binding. ---
  const due = await listDueAustinCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });
  const facts = await loadElectionFacts(
    input.db,
    due.rows.map((row) => row.electionId),
  );

  const results: AustinCandidateFinanceBatchItemResult[] = [];
  // The city-wide DCE + purpose datasets are fetched once per run, only once
  // a candidate actually needs them; a fetch failure fails every remaining
  // candidate with the same reason instead of re-fetching per candidate.
  let outsideDatasets: AustinOutsideDatasets | undefined;
  let outsideDatasetsError: string | null = null;
  const getOutsideDatasets = async (): Promise<AustinOutsideDatasets> => {
    if (outsideDatasetsError !== null) throw new Error(outsideDatasetsError);
    if (outsideDatasets === undefined) {
      try {
        outsideDatasets = await (input.loadOutsideDatasetsFn ?? loadAustinOutsideDatasets)(options);
      } catch (error) {
        outsideDatasetsError = error instanceof Error ? error.message : String(error);
        throw error;
      }
    }
    return outsideDatasets;
  };
  for (const row of due.rows) {
    const election = facts.get(row.electionId);
    if (!election || !isAustinFinanceSupportedElectionDate(election.electionDate)) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        status: "skipped",
        reason: `election date ${election?.electionDate ?? "unknown"} is not in the Austin finance allowlist`,
      });
      continue;
    }
    if (election.officeCode === null) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        status: "skipped",
        reason: "election has no Austin office code (council title without a single district number)",
      });
      continue;
    }
    try {
      const outsideDatasets = await getOutsideDatasets();
      const result = await (input.syncFn ?? syncAustinCandidateFinance)({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        candidateDisplayName: row.candidateName,
        officeName: row.officeName,
        district: row.district,
        filerName: row.filerName,
        electionDate: election.electionDate,
        officeCode: election.officeCode,
        outsideDatasets,
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
