import type { Pool, PoolClient } from "pg";
import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  autoLinkMissingLosAngelesCandidateFinanceLinks,
  listLosAngelesCandidateElectionsMissingFinanceLinks,
} from "./losAngelesCandidateFinanceAutoLink.js";
import { syncLosAngelesCandidateFinance } from "./losAngelesCandidateFinanceSync.js";
import {
  getLosAngelesEthicsCandidateTotals,
  type LosAngelesCityEthicsClientOptions,
} from "./losAngelesCityEthicsClient.js";
import type { LosAngelesOpenDataClientOptions } from "./losAngelesOpenDataClient.js";
import { toLosAngelesEthicsOfficeName } from "./losAngelesCityFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type PoolLike = Queryable & Pick<Pool, "connect">;
type DueRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  election_date: string;
  office_name: string;
  seat_number: number | null;
  ethics_election_id: string;
  ethics_candidate_person_id: string;
  last_synced_at: string | null;
  total_due_rows: string | number;
};
export type LosAngelesCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  autoLinkAttemptedCount: number;
  autoLinkLinkedCount: number;
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
  if (!Number.isInteger(result) || result <= 0)
    throw new Error(`Invalid Los Angeles finance ${label}: ${value}`);
  return result;
};

export async function syncDueLosAngelesCandidateFinance(input: {
  db: PoolLike;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  autoLinkMissingLinks?: boolean;
  ethicsClientOptions?: LosAngelesCityEthicsClientOptions;
  openDataClientOptions?: LosAngelesOpenDataClientOptions;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
}): Promise<LosAngelesCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime()))
    throw new Error("Invalid Los Angeles finance batch timestamp");
  const max = integer(input.maxCandidates, 25, "maxCandidates"),
    stale = integer(input.staleAfterDays, 1, "staleAfterDays"),
    lookback = integer(input.electionLookbackDays, 45, "electionLookbackDays"),
    lookahead = integer(
      input.electionLookaheadDays,
      730,
      "electionLookaheadDays",
    );
  let attempted = 0,
    linked = 0;
  if (!input.dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const candidates =
        await listLosAngelesCandidateElectionsMissingFinanceLinks(input.db, {
          now,
          maxCandidates: max,
          electionLookbackDays: lookback,
          electionLookaheadDays: lookahead,
        });
      attempted = candidates.length;
      const linkResults = await autoLinkMissingLosAngelesCandidateFinanceLinks({
        db: input.db,
        now,
        candidates,
        ethicsClientOptions: input.ethicsClientOptions,
      });
      linked = linkResults.filter((row) => row.status === "linked").length;
      for (const row of linkResults)
        if (row.status === "error")
          console.warn("Los Angeles finance auto-link failed; continuing", row);
    } catch (error) {
      console.warn(
        "Los Angeles finance auto-link skipped; continuing existing links",
        error instanceof Error ? error.message : error,
      );
    }
  }
  const due = await input.db.query<DueRow>(
    `WITH due AS (SELECT link.candidate_id::text candidate_id,link.election_id::text election_id,COALESCE(NULLIF(trim(candidate.display_name),''),link.candidate_name_normalized) candidate_name,link.election_year,election.election_date::text election_date,link.office_name,link.seat_number,link.ethics_election_id,link.ethics_candidate_person_id,summary.last_synced_at::text last_synced_at,count(*) OVER() total_due_rows FROM public.lacity_candidate_finance_links link JOIN public.candidates candidate ON candidate.id=link.candidate_id JOIN public.candidate_elections ce ON ce.candidate_id=link.candidate_id AND ce.election_id=link.election_id JOIN public.elections election ON election.id=link.election_id JOIN public.districts district ON district.id=election.district_id LEFT JOIN public.lacity_candidate_finance_summaries summary ON summary.link_id=link.id AND summary.election_year=link.election_year WHERE link.link_status='active' AND candidate.deleted_at IS NULL AND district.state='CA' AND ((district.district_type='place' AND district.geoid_compact='0644000' AND link.office_name<>'School Board Member') OR (district.district_type='school_unified' AND district.geoid_compact='0622710' AND link.office_name='School Board Member')) AND election.election_date>=($1::date-make_interval(days=>$4::int)) AND election.election_date<=($1::date+make_interval(days=>$5::int)) AND ce.status NOT IN ('withdrawn','lost') AND (summary.last_synced_at IS NULL OR summary.last_synced_at<($1::timestamptz-make_interval(days=>$2::int))) ORDER BY summary.last_synced_at NULLS FIRST,election.election_date,link.candidate_name_normalized LIMIT $3::int) SELECT * FROM due`,
    [now.toISOString(), stale, max, lookback, lookahead],
  );
  const totalsCache = new Map<
    string,
    Awaited<ReturnType<typeof getLosAngelesEthicsCandidateTotals>>
  >();
  const results: LosAngelesCandidateFinanceBatchSyncResult["results"] = [];
  for (const row of due.rows) {
    try {
      const ethicsOfficeName = toLosAngelesEthicsOfficeName({
        officeScope:
          row.office_name === "School Board Member"
            ? "school_unified"
            : "place",
        officeCanonicalName: row.office_name,
        seatNumber: row.seat_number,
      });
      if (!ethicsOfficeName)
        throw new Error(
          `Linked Los Angeles finance office is not eligible: ${row.office_name}`,
        );
      const totalsCacheKey = `${row.ethics_election_id}:${ethicsOfficeName}`;
      let totals = totalsCache.get(totalsCacheKey);
      if (!totals) {
        totals = await getLosAngelesEthicsCandidateTotals(
          { electionId: row.ethics_election_id, officeName: ethicsOfficeName },
          input.ethicsClientOptions,
        );
        totalsCache.set(totalsCacheKey, totals);
      }
      const total = totals.find(
        (item) => item.candidatePersonId === row.ethics_candidate_person_id,
      );
      if (!total)
        throw new Error(
          "Linked candidate missing from current Los Angeles Ethics totals",
        );
      await syncLosAngelesCandidateFinance({
        db: input.db,
        candidateId: row.candidate_id,
        electionId: row.election_id,
        electionYear: row.election_year,
        candidateName: row.candidate_name,
        officeName: row.office_name,
        seatNumber: row.seat_number,
        total,
        ethicsClientOptions: input.ethicsClientOptions,
        openDataClientOptions: input.openDataClientOptions,
        financeIndustryClassifier: input.financeIndustryClassifier,
        aiClassificationMinAmount: input.aiClassificationMinAmount,
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
    results,
  };
}
