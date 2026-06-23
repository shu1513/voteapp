import type { Pool, PoolClient } from "pg";

import {
  fetchVirginiaCampaignFinanceReport,
  fetchVirginiaCommitteeReportList,
  type VirginiaCampaignFinanceClientOptions,
  type VirginiaScheduleAContribution,
} from "./virginiaCampaignFinanceClient.js";
import {
  autoLinkMissingVirginiaCandidateFinanceLinks,
  listVirginiaCandidateElectionsMissingFinanceLinks,
  type VirginiaCandidateCommitteeResolver,
  type VirginiaFinanceAutoLinkCandidateElection,
} from "./virginiaCandidateFinanceAutoLink.js";
import {
  syncVirginiaCandidateFinance,
  type VirginiaCandidateFinanceSyncResult,
} from "./virginiaCandidateFinanceSync.js";
import { VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./virginiaFinanceEligibleOffices.js";
import type { VirginiaFinanceLinkSource } from "./virginiaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type VirginiaCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  committeeId: string;
  committeeCode: string | null;
  committeeName: string;
  linkSource: VirginiaFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export type VirginiaCandidateFinanceReportData = {
  committeeId: string;
  sourceUrl: string | null;
  contributions: VirginiaScheduleAContribution[];
  scheduledReportCount: number;
};

export type VirginiaCandidateFinanceBatchSyncInput = {
  db: Queryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  autoLinkMissingLinks?: boolean;
  clientOptions?: VirginiaCampaignFinanceClientOptions;
  syncVirginiaCandidateFinanceFn?: typeof syncVirginiaCandidateFinance;
  loadReportDataForCommittee?: typeof loadVirginiaReportDataForCommittee;
  resolveCandidateCommittee?: VirginiaCandidateCommitteeResolver;
};

export type VirginiaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: VirginiaCandidateFinanceSyncResult;
  error?: string;
};

export type VirginiaCandidateFinanceBatchSyncResult = {
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
  results: VirginiaCandidateFinanceBatchSyncItemResult[];
};

type VirginiaCandidateFinanceDueQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  committee_id: string;
  committee_code: string | null;
  committee_name: string;
  link_source: VirginiaFinanceLinkSource;
  source_url: string | null;
  last_synced_at: string | null;
  total_due_rows: string | number;
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
// Keep one extra calendar day so UTC scheduler timing cannot skip election-night finance syncs.
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid Virginia finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Virginia finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function normalizeDistrict(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function mapDueRow(row: VirginiaCandidateFinanceDueQueryRow): VirginiaCandidateFinanceDueRow {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: normalizeDistrict(row.district),
    committeeId: row.committee_id,
    committeeCode: row.committee_code,
    committeeName: row.committee_name,
    linkSource: row.link_source,
    sourceUrl: row.source_url,
    lastSyncedAt: row.last_synced_at,
  };
}

export async function loadVirginiaReportDataForCommittee(input: {
  committeeId: string;
  clientOptions?: VirginiaCampaignFinanceClientOptions;
}): Promise<VirginiaCandidateFinanceReportData> {
  const reportList = await fetchVirginiaCommitteeReportList(input.committeeId, input.clientOptions);
  const contributions: VirginiaScheduleAContribution[] = [];
  for (const reportId of reportList.scheduledReportIds) {
    const report = await fetchVirginiaCampaignFinanceReport(reportId, input.clientOptions);
    contributions.push(...report.scheduleA);
  }

  return {
    committeeId: input.committeeId,
    sourceUrl: reportList.sourceUrl,
    contributions,
    scheduledReportCount: reportList.scheduledReportIds.length,
  };
}

export async function listDueVirginiaCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: VirginiaCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<VirginiaCandidateFinanceDueQueryRow>(
    `
      WITH due AS (
        SELECT
          link.candidate_id::text AS candidate_id,
          link.election_id::text AS election_id,
          COALESCE(
            NULLIF(trim(candidate.display_name), ''),
            NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), ''),
            link.candidate_name_normalized
          ) AS candidate_name,
          link.election_year,
          office.scope AS office_scope,
          link.office_name,
          link.district,
          link.committee_id,
          link.committee_code,
          link.committee_name,
          link.link_source,
          link.source_url,
          summary.last_synced_at::text AS last_synced_at,
          COUNT(*) OVER () AS total_due_rows
        FROM public.va_candidate_finance_links AS link
        JOIN public.candidates AS candidate
          ON candidate.id = link.candidate_id
        JOIN public.candidate_elections AS candidate_election
          ON candidate_election.candidate_id = link.candidate_id
         AND candidate_election.election_id = link.election_id
        JOIN public.elections AS election
          ON election.id = link.election_id
        JOIN public.districts AS district
          ON district.id = election.district_id
        LEFT JOIN public.offices AS office
          ON office.id = election.office_id
        LEFT JOIN public.va_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'VA'
          AND election.race_type = 'office'
          AND election.election_date >= ($1::date - make_interval(days => $4::int))
          AND election.election_date <= ($1::date + make_interval(days => $5::int))
          AND candidate_election.status NOT IN ('withdrawn', 'lost')
          AND (office.scope || '::' || office.canonical_name) = ANY($6::text[])
          AND (
            summary.last_synced_at IS NULL
            OR summary.last_synced_at < ($1::timestamptz - make_interval(days => $2::int))
          )
        ORDER BY summary.last_synced_at ASC NULLS FIRST,
                 election.election_date ASC,
                 link.candidate_name_normalized ASC,
                 link.id ASC
        LIMIT $3::int
      )
      SELECT
        candidate_id,
        election_id,
        candidate_name,
        election_year,
        office_scope,
        office_name,
        district,
        committee_id,
        committee_code,
        committee_name,
        link_source,
        source_url,
        last_synced_at,
        total_due_rows
      FROM due
    `,
    [
      input.now.toISOString(),
      input.staleAfterDays,
      input.maxCandidates,
      input.electionLookbackDays,
      input.electionLookaheadDays,
      [...VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueVirginiaCandidateFinance(
  input: VirginiaCandidateFinanceBatchSyncInput
): Promise<VirginiaCandidateFinanceBatchSyncResult> {
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
  // Daily syncs intentionally auto-link eligible Virginia candidates unless explicitly disabled.
  const shouldAutoLinkMissingLinks = !dryRun && input.autoLinkMissingLinks !== false;
  const syncFn = input.syncVirginiaCandidateFinanceFn ?? syncVirginiaCandidateFinance;
  const loadReportData = input.loadReportDataForCommittee ?? loadVirginiaReportDataForCommittee;
  let autoLinkAttemptedCount = 0;
  let autoLinkLinkedCount = 0;

  if (shouldAutoLinkMissingLinks) {
    try {
      const missingLinkCandidates: VirginiaFinanceAutoLinkCandidateElection[] =
        await listVirginiaCandidateElectionsMissingFinanceLinks(input.db, {
          now,
          maxCandidates,
          electionLookbackDays,
          electionLookaheadDays,
        });
      autoLinkAttemptedCount = missingLinkCandidates.length;
      const autoLinkResults = await autoLinkMissingVirginiaCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        candidateElections: missingLinkCandidates,
        resolveCandidateCommittee: input.resolveCandidateCommittee,
        clientOptions: input.clientOptions,
      });
      autoLinkLinkedCount = autoLinkResults.filter((result) => result.status === "linked").length;
      for (const result of autoLinkResults) {
        if (result.status !== "linked") {
          console.warn("Virginia finance auto-link did not link candidate election:", result);
        }
      }
    } catch (error) {
      console.warn(
        "Virginia finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueVirginiaCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  const results: VirginiaCandidateFinanceBatchSyncItemResult[] = [];
  const reportDataByCommitteeId = new Map<string, Promise<VirginiaCandidateFinanceReportData>>();
  for (const row of due.rows) {
    try {
      let reportDataPromise = reportDataByCommitteeId.get(row.committeeId);
      if (!reportDataPromise) {
        reportDataPromise = loadReportData({
          committeeId: row.committeeId,
          clientOptions: input.clientOptions,
        }).catch((error) => {
          reportDataByCommitteeId.delete(row.committeeId);
          throw error;
        });
        reportDataByCommitteeId.set(row.committeeId, reportDataPromise);
      }
      const reportData = await reportDataPromise;
      const result = await syncFn({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeName: row.officeName,
        district: row.district,
        committeeId: row.committeeId,
        committeeCode: row.committeeCode,
        committeeName: row.committeeName,
        linkSource: row.linkSource,
        sourceUrl: row.sourceUrl,
        contributions: reportData.contributions,
        contributionSourceUrl: reportData.sourceUrl,
        dryRun,
        now,
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
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        committeeId: row.committeeId,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const syncedCandidateCount = results.filter((result) => result.ok).length;
  return {
    dryRun,
    now: now.toISOString(),
    staleAfterDays,
    maxCandidates,
    dueCandidateCount: due.totalDueRows,
    selectedCandidateCount: due.rows.length,
    syncedCandidateCount,
    failedCandidateCount: results.length - syncedCandidateCount,
    autoLinkAttemptedCount,
    autoLinkLinkedCount,
    results,
  };
}
