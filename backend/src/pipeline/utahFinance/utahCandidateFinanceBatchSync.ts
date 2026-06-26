import type { Pool, PoolClient } from "pg";

import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  buildUtahGenerateReportUrl,
  UTAH_DISCLOSURES_BASE_URL,
  type UtahDisclosuresClientOptions,
  type UtahDisclosuresGenerateReportInput,
  type UtahDisclosuresTransactionRow,
} from "./utahDisclosuresClient.js";
import {
  downloadUtahGeneratedReportRowsWithCache,
  type UtahDisclosuresCachedRows,
} from "./utahDisclosuresCsvCache.js";
import { UTAH_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./utahFinanceEligibleOffices.js";
import {
  syncUtahCandidateFinance,
  type UtahCandidateFinanceSyncResult,
} from "./utahCandidateFinanceSync.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type UtahCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  folderId: string;
  committeeName: string;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export type UtahGeneratedReportRowsLoader = (
  input: UtahDisclosuresGenerateReportInput
) => Promise<UtahDisclosuresCachedRows>;

export type UtahCandidateFinanceBatchSyncInput = {
  db: Queryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
  refreshCache?: boolean;
  disclosuresClientOptions?: UtahDisclosuresClientOptions;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  classifySupportingCommitteeIndustriesWithAi?: boolean;
  supportingCommitteeIndustryMinAmount?: number;
  loadGeneratedReportRowsFn?: UtahGeneratedReportRowsLoader;
  syncUtahCandidateFinanceFn?: typeof syncUtahCandidateFinance;
};

export type UtahCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  folderId: string;
  ok: boolean;
  result?: UtahCandidateFinanceSyncResult;
  supportingCommitteeLoadError?: string;
  error?: string;
};

export type UtahCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  results: UtahCandidateFinanceBatchSyncItemResult[];
};

type UtahCandidateFinanceDueQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  folder_id: string;
  committee_name: string;
  source_url: string | null;
  last_synced_at: string | null;
  total_due_rows: string | number;
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid Utah finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Utah finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function mapDueRow(row: UtahCandidateFinanceDueQueryRow): UtahCandidateFinanceDueRow {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
    folderId: row.folder_id,
    committeeName: row.committee_name,
    sourceUrl: row.source_url,
    lastSyncedAt: row.last_synced_at,
  };
}

function buildFolderDetailsUrl(folderId: string): string {
  return new URL(`/Search/AdvancedSearch/FolderDetails/${folderId}`, UTAH_DISCLOSURES_BASE_URL).toString();
}

function buildDefaultRowsLoader(
  input: Pick<
    UtahCandidateFinanceBatchSyncInput,
    "rawDataCacheDir" | "refreshCache" | "disclosuresClientOptions"
  >
): UtahGeneratedReportRowsLoader {
  return async (reportInput) =>
    downloadUtahGeneratedReportRowsWithCache(
      {
        ...reportInput,
        cacheDir: input.rawDataCacheDir,
      },
      {
        ...input.disclosuresClientOptions,
        refreshCache: input.refreshCache,
      }
    );
}

async function loadPacRowsForYear(input: {
  year: number;
  loadGeneratedReportRows: UtahGeneratedReportRowsLoader;
  cache: Map<number, Promise<UtahDisclosuresCachedRows>>;
}): Promise<UtahDisclosuresCachedRows> {
  const existing = input.cache.get(input.year);
  if (existing) {
    return existing;
  }
  const load = input.loadGeneratedReportRows({ reportYear: input.year, entityType: "PAC" });
  input.cache.set(input.year, load);
  return load;
}

export async function listDueUtahCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: UtahCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<UtahCandidateFinanceDueQueryRow>(
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
          link.folder_id,
          link.committee_name,
          link.source_url,
          summary.last_synced_at::text AS last_synced_at,
          COUNT(*) OVER () AS total_due_rows
        FROM public.ut_candidate_finance_links AS link
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
        LEFT JOIN public.ut_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'UT'
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
        folder_id,
        committee_name,
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
      [...UTAH_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueUtahCandidateFinance(
  input: UtahCandidateFinanceBatchSyncInput
): Promise<UtahCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncUtahCandidateFinanceFn ?? syncUtahCandidateFinance;
  const loadGeneratedReportRows = input.loadGeneratedReportRowsFn ?? buildDefaultRowsLoader(input);
  const pacRowsByYear = new Map<number, Promise<UtahDisclosuresCachedRows>>();

  const due = await listDueUtahCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  const results: UtahCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    try {
      const candidateReport = await loadGeneratedReportRows({
        reportYear: row.electionYear,
        folderId: row.folderId,
      });
      let supportingCommitteeTransactions: readonly UtahDisclosuresTransactionRow[] | undefined;
      let supportingCommitteeSourceUrl: string | undefined;
      let supportingCommitteeLoadError: string | undefined;

      try {
        const pacReport = await loadPacRowsForYear({
          year: row.electionYear,
          loadGeneratedReportRows,
          cache: pacRowsByYear,
        });
        supportingCommitteeTransactions = pacReport.rows;
        supportingCommitteeSourceUrl = pacReport.sourceUrl;
      } catch (error) {
        supportingCommitteeLoadError = error instanceof Error ? error.message : String(error);
      }

      const folderSourceUrl = row.sourceUrl ?? buildFolderDetailsUrl(row.folderId);
      const result = await syncFn({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeScope: row.officeScope,
        officeName: row.officeName,
        district: row.district,
        sourceUrl: folderSourceUrl,
        transactionSourceUrl:
          candidateReport.sourceUrl ??
          buildUtahGenerateReportUrl({ reportYear: row.electionYear, folderId: row.folderId }),
        supportingCommitteeSourceUrl,
        entityRows: [
          {
            folderId: row.folderId,
            entityName: row.committeeName,
            reportYears: [row.electionYear],
            sourceUrl: folderSourceUrl,
          },
        ],
        trustedCommittee: {
          folderId: row.folderId,
          committeeName: row.committeeName,
          reportYears: [row.electionYear],
          sourceUrl: folderSourceUrl,
        },
        transactions: candidateReport.rows,
        supportingCommitteeTransactions,
        financeIndustryClassifier: input.financeIndustryClassifier,
        classifySupportingCommitteeIndustriesWithAi: input.classifySupportingCommitteeIndustriesWithAi,
        supportingCommitteeIndustryMinAmount: input.supportingCommitteeIndustryMinAmount,
        dryRun,
        now,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        folderId: row.folderId,
        ok: true,
        result,
        ...(supportingCommitteeLoadError ? { supportingCommitteeLoadError } : {}),
      });
    } catch (error) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        folderId: row.folderId,
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
    results,
  };
}
