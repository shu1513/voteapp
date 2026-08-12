// Phase 5 batch sync, the SF due-query pattern reshaped for a bulk-file
// source: the cycle workbooks are loaded ONCE per election year (refreshed
// from the portal only when the raw-data flag allows) and threaded through
// both legs —
//
//   1. Auto-link candidates missing active links (warn-and-continue — a
//      linking failure must not stop existing links from syncing).
//   2. The stalest-first candidate loop over active links.
//
// There is no SF-style stale-election wholesale-refresh leg on purpose: San
// José has no outside-relations table to go stale, every snapshot rewrites
// its outside groups, and each successful sync's link upsert advances
// last_verified_at.

import type { Pool, PoolClient } from "pg";
import {
  autoLinkMissingSanJoseCandidateFinanceLinks,
  listSanJoseCandidateElectionsMissingFinanceLinks,
} from "./sanJoseCandidateFinanceAutoLink.js";
import {
  loadSanJoseCycleWorkbookData,
  syncSanJoseCandidateFinance,
  type SanJoseCycleWorkbookData,
} from "./sanJoseCandidateFinanceSync.js";
import { parseSanJoseCityCouncilSeatNumber } from "./sanJoseFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type PoolLike = Queryable & { connect: () => Promise<PoolClient> };

const SAN_JOSE_DISTRICT_PREDICATE = `district.state='CA' AND district.district_type='place' AND district.geoid_compact='0668000'`;

type DueRow = {
  candidate_id: string;
  election_id: string;
  election_year: number;
  candidate_name: string;
  office_name: string;
  official_ballot_title: string | null;
  fppc_id: string;
  last_synced_at: string | null;
  total_due_rows: string | number;
};

export type SanJoseCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  autoLinkAttemptedCount: number;
  autoLinkLinkedCount: number;
  /** Per-year workbook acquisition outcomes (downloaded/unchanged/cached). */
  workbookSources: SanJoseCycleWorkbookData["sources"];
  results: Array<{
    candidateId: string;
    electionId: string;
    ok: boolean;
    error?: string;
  }>;
};

const integer = (
  value: number | undefined,
  fallback: number,
  label: string,
): number => {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result <= 0)
    throw new Error(`Invalid San José finance ${label}: ${value}`);
  return result;
};

// Validated up front so a mistyped backfill target fails loudly here instead
// of as a Postgres cast error after the auto-link leg already ran.
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export async function syncDueSanJoseCandidateFinance(input: {
  db: PoolLike;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  /**
   * Historical-backfill targeting: sync ONLY this election's active links.
   * Replaces the election-date window, drops the withdrawn/lost exclusion
   * (in a decided election the losers are the point of the backfill), drops
   * the staleness filter (an explicitly targeted rerun must select the
   * election even right after a previous run), and skips the auto-link leg.
   */
  electionId?: string;
  autoLinkMissingLinks?: boolean;
  /** Portal refresh permission — the script derives this from the raw-data flag. */
  refreshRawData?: boolean;
  cacheDir?: string;
  fetchImpl?: typeof fetch;
  bypassAnomalyCheck?: boolean;
  /** Test seams; default to the real implementations. */
  loadWorkbookData?: typeof loadSanJoseCycleWorkbookData;
  syncCandidateFn?: typeof syncSanJoseCandidateFinance;
}): Promise<SanJoseCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime()))
    throw new Error("Invalid San José finance batch timestamp");
  const max = integer(input.maxCandidates, 25, "maxCandidates"),
    stale = integer(input.staleAfterDays, 1, "staleAfterDays"),
    lookback = integer(input.electionLookbackDays, 45, "electionLookbackDays"),
    lookahead = integer(
      input.electionLookaheadDays,
      730,
      "electionLookaheadDays",
    );
  const electionId = input.electionId?.trim() || undefined;
  if (electionId !== undefined && !UUID_PATTERN.test(electionId))
    throw new Error(`Invalid San José finance electionId: ${electionId}`);
  const loadWorkbookData =
    input.loadWorkbookData ?? loadSanJoseCycleWorkbookData;
  const syncCandidateFn = input.syncCandidateFn ?? syncSanJoseCandidateFinance;

  // One workbook pair per election year for the whole run. The first load
  // for a year performs the (flag-gated) refresh; a load failure is
  // remembered so the portal is not hammered once per candidate.
  const workbookCache = new Map<
    number,
    { data: SanJoseCycleWorkbookData } | { error: string }
  >();
  const workbookSources: SanJoseCycleWorkbookData["sources"] = [];
  const getWorkbookData = async (
    electionYear: number,
  ): Promise<{ data: SanJoseCycleWorkbookData } | { error: string }> => {
    let entry = workbookCache.get(electionYear);
    if (!entry) {
      try {
        const data = await loadWorkbookData({
          electionYear,
          refreshRawData: Boolean(input.refreshRawData),
          cacheDir: input.cacheDir,
          fetchImpl: input.fetchImpl,
          now,
        });
        workbookSources.push(...data.sources);
        entry = { data };
      } catch (error) {
        entry = {
          error: error instanceof Error ? error.message : String(error),
        };
      }
      workbookCache.set(electionYear, entry);
    }
    return entry;
  };

  // --- Leg 1: link candidates that have no active link yet. ---
  let attempted = 0,
    linked = 0;
  if (!input.dryRun && electionId === undefined && input.autoLinkMissingLinks !== false) {
    try {
      const candidates =
        await listSanJoseCandidateElectionsMissingFinanceLinks(input.db, {
          now,
          maxCandidates: max,
          electionLookbackDays: lookback,
          electionLookaheadDays: lookahead,
        });
      attempted = candidates.length;
      const byYear = new Map<number, typeof candidates>();
      for (const candidate of candidates) {
        const group = byYear.get(candidate.electionYear) ?? [];
        group.push(candidate);
        byYear.set(candidate.electionYear, group);
      }
      for (const [electionYear, group] of byYear) {
        const workbook = await getWorkbookData(electionYear);
        if ("error" in workbook) {
          console.warn(
            "San José finance auto-link skipped an election year; workbook unavailable:",
            { electionYear, reason: workbook.error },
          );
          continue;
        }
        const linkResults = await autoLinkMissingSanJoseCandidateFinanceLinks({
          db: input.db,
          now,
          candidates: group,
          workbook: workbook.data.workbook,
        });
        linked += linkResults.filter((row) => row.status === "linked").length;
        for (const row of linkResults)
          if (row.status !== "linked" && row.status !== "no_committee")
            console.warn(
              "San José finance auto-link did not link candidate election:",
              row,
            );
      }
    } catch (error) {
      console.warn(
        "San José finance auto-link skipped; continuing existing links",
        error instanceof Error ? error.message : error,
      );
    }
  }

  // --- Candidate loop: stalest first (never-synced links lead via NULLS
  // FIRST, so reruns resume partial failures before touching synced rows).
  // Ordinary runs are ALWAYS bounded by the election-date window and the
  // staleness filter — history is reachable only through explicit electionId
  // targeting (see the input doc).
  const dueScope =
    electionId === undefined
      ? {
          where: `election.election_date>=(($1::timestamptz AT TIME ZONE 'UTC')::date-make_interval(days=>$4::int)) AND election.election_date<=(($1::timestamptz AT TIME ZONE 'UTC')::date+make_interval(days=>$5::int)) AND ce.status NOT IN ('withdrawn','lost') AND (summary.last_synced_at IS NULL OR summary.last_synced_at<($1::timestamptz-make_interval(days=>$2::int)))`,
          limit: "$3::int",
          params: [now.toISOString(), stale, max, lookback, lookahead],
        }
      : {
          where: `election.id=$1::uuid`,
          limit: "$2::int",
          params: [electionId, max],
        };
  const due = await input.db.query<DueRow>(
    `WITH due AS (SELECT link.candidate_id::text candidate_id,link.election_id::text election_id,link.election_year,COALESCE(NULLIF(trim(candidate.display_name),''),link.candidate_name_normalized) candidate_name,office.canonical_name office_name,election.official_ballot_title,link.fppc_id,summary.last_synced_at::text last_synced_at,count(*) OVER() total_due_rows FROM public.sjc_candidate_finance_links link JOIN public.candidates candidate ON candidate.id=link.candidate_id JOIN public.candidate_elections ce ON ce.candidate_id=link.candidate_id AND ce.election_id=link.election_id JOIN public.elections election ON election.id=link.election_id JOIN public.districts district ON district.id=election.district_id JOIN public.offices office ON office.id=election.office_id LEFT JOIN public.sjc_candidate_finance_summaries summary ON summary.link_id=link.id AND summary.election_year=link.election_year WHERE link.link_status='active' AND candidate.deleted_at IS NULL AND ${SAN_JOSE_DISTRICT_PREDICATE} AND ${dueScope.where} ORDER BY summary.last_synced_at NULLS FIRST,election.election_date,link.candidate_name_normalized LIMIT ${dueScope.limit}) SELECT * FROM due`,
    dueScope.params,
  );
  const results: SanJoseCandidateFinanceBatchSyncResult["results"] = [];
  for (const row of due.rows) {
    try {
      const officeName = row.office_name.trim();
      if (officeName !== "Mayor" && officeName !== "City Council Member")
        throw new Error(
          `Linked election office "${officeName}" is not a San José finance office`,
        );
      const seatNumber =
        officeName === "Mayor"
          ? null
          : parseSanJoseCityCouncilSeatNumber(row.official_ballot_title);
      if (officeName === "City Council Member" && seatNumber === null)
        throw new Error(
          `Cannot parse a council district from ballot title "${row.official_ballot_title}"`,
        );
      const workbook = await getWorkbookData(row.election_year);
      if ("error" in workbook) throw new Error(workbook.error);
      await syncCandidateFn({
        db: input.db,
        candidateId: row.candidate_id,
        electionId: row.election_id,
        electionYear: row.election_year,
        candidateDisplayName: row.candidate_name,
        officeName,
        seatNumber,
        fppcId: row.fppc_id,
        workbook: workbook.data.workbook,
        bypassAnomalyCheck: input.bypassAnomalyCheck,
        dryRun: input.dryRun,
        now,
      });
      results.push({
        candidateId: row.candidate_id,
        electionId: row.election_id,
        ok: true,
      });
    } catch (error) {
      results.push({
        candidateId: row.candidate_id,
        electionId: row.election_id,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  const synced = results.filter((row) => row.ok).length;
  return {
    dryRun: Boolean(input.dryRun),
    dueCandidateCount: due.rows.length
      ? Number(due.rows[0]!.total_due_rows)
      : 0,
    selectedCandidateCount: due.rows.length,
    syncedCandidateCount: synced,
    failedCandidateCount: results.length - synced,
    autoLinkAttemptedCount: attempted,
    autoLinkLinkedCount: linked,
    workbookSources,
    results,
  };
}
