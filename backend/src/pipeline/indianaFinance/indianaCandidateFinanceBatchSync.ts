import { stat } from "node:fs/promises";
import type { Pool, PoolClient } from "pg";

import {
  autoLinkMissingIndianaCandidateFinanceLinks,
  buildIndianaCandidateNamePredicate,
  listIndianaCandidateElectionsMissingFinanceLinks,
  type IndianaFinanceAutoLinkCandidateElection,
} from "./indianaCandidateFinanceAutoLink.js";
import {
  syncIndianaCandidateFinance,
  type IndianaCandidateFinanceSyncResult,
} from "./indianaCandidateFinanceSync.js";
import { INDIANA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./indianaFinanceEligibleOffices.js";
import {
  DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR,
  buildIndianaCampaignFinanceArtifactUrl,
  getIndianaCampaignFinanceArtifactCachePaths,
  readIndianaCampaignFinanceArtifactCacheMetadata,
} from "./indianaCampaignFinanceArtifactCache.js";
import {
  readIndianaCampaignFinanceContributionRows,
  type IndianaCampaignFinanceContributionRow,
} from "./indianaCampaignFinanceReader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type IndianaCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  committeeId: string;
  committeeName: string;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export type IndianaContributionDataForYear = {
  year: number;
  zipPath: string;
  sourceUrl: string;
  rowsByCommitteeId: Map<string, IndianaCampaignFinanceContributionRow[]>;
};

export type IndianaCandidateFinanceBatchSyncInput = {
  db: Queryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
  autoLinkMissingLinks?: boolean;
  contributionDataByYear?: ReadonlyMap<number, IndianaContributionDataForYear>;
  syncIndianaCandidateFinanceFn?: typeof syncIndianaCandidateFinance;
};

export type IndianaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: IndianaCandidateFinanceSyncResult;
  error?: string;
};

export type IndianaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  results: IndianaCandidateFinanceBatchSyncItemResult[];
};

type IndianaCandidateFinanceDueQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  committee_id: string;
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
    throw new Error(`Invalid Indiana finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Indiana finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function normalizeCommitteeId(value: string): string {
  return value.trim().toUpperCase();
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function mapDueRow(row: IndianaCandidateFinanceDueQueryRow): IndianaCandidateFinanceDueRow {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
    committeeId: row.committee_id,
    committeeName: row.committee_name,
    sourceUrl: row.source_url,
    lastSyncedAt: row.last_synced_at,
  };
}

async function fileExists(path: string): Promise<boolean> {
  try {
    const fileStat = await stat(path);
    return fileStat.isFile();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

function groupDueRowsByYear(rows: readonly IndianaCandidateFinanceDueRow[]): Map<number, IndianaCandidateFinanceDueRow[]> {
  const byYear = new Map<number, IndianaCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function groupAutoLinkCandidatesByYear(
  rows: readonly IndianaFinanceAutoLinkCandidateElection[]
): Map<number, IndianaFinanceAutoLinkCandidateElection[]> {
  const byYear = new Map<number, IndianaFinanceAutoLinkCandidateElection[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function groupContributionRowsByCommittee(
  rows: readonly IndianaCampaignFinanceContributionRow[]
): Map<string, IndianaCampaignFinanceContributionRow[]> {
  const byCommittee = new Map<string, IndianaCampaignFinanceContributionRow[]>();
  for (const row of rows) {
    const committeeId = normalizeCommitteeId(row.FileNumber);
    if (!committeeId) {
      continue;
    }
    const existing = byCommittee.get(committeeId) ?? [];
    existing.push(row);
    byCommittee.set(committeeId, existing);
  }
  return byCommittee;
}

async function loadContributionDataForYear(input: {
  year: number;
  committeeIds: readonly string[];
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
}): Promise<IndianaContributionDataForYear> {
  const normalizedCommitteeIds = new Set(input.committeeIds.map(normalizeCommitteeId).filter(Boolean));
  const paths = getIndianaCampaignFinanceArtifactCachePaths({
    cacheDir:
      input.rawDataCacheDir ??
      process.env.INDIANA_CAMPAIGN_FINANCE_CACHE_DIR?.trim() ??
      DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR,
    year: input.year,
    artifactKind: "contribution",
  });
  const zipPath = input.rawDataZipPath ?? paths.zipPath;
  if (!(await fileExists(zipPath))) {
    throw new Error(`Indiana campaign finance contribution ZIP not found for ${input.year}: ${zipPath}`);
  }

  const metadata = input.rawDataZipPath
    ? null
    : await readIndianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  const rows = await readIndianaCampaignFinanceContributionRows({
    zipPath,
    year: input.year,
    predicate: (row) => normalizedCommitteeIds.has(normalizeCommitteeId(row.FileNumber)),
  });

  return {
    year: input.year,
    zipPath,
    sourceUrl:
      metadata?.remote.url ??
      buildIndianaCampaignFinanceArtifactUrl({ year: input.year, artifactKind: "contribution" }),
    rowsByCommitteeId: groupContributionRowsByCommittee(rows),
  };
}

async function loadAutoLinkContributionRowsForYear(input: {
  year: number;
  candidates: readonly IndianaFinanceAutoLinkCandidateElection[];
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
  contributionDataByYear?: ReadonlyMap<number, IndianaContributionDataForYear>;
}): Promise<{ rows: IndianaCampaignFinanceContributionRow[]; sourceUrl: string }> {
  const injected = input.contributionDataByYear?.get(input.year);
  if (injected) {
    return {
      rows: [...injected.rowsByCommitteeId.values()].flat(),
      sourceUrl: injected.sourceUrl,
    };
  }

  const paths = getIndianaCampaignFinanceArtifactCachePaths({
    cacheDir:
      input.rawDataCacheDir ??
      process.env.INDIANA_CAMPAIGN_FINANCE_CACHE_DIR?.trim() ??
      DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR,
    year: input.year,
    artifactKind: "contribution",
  });
  const zipPath = input.rawDataZipPath ?? paths.zipPath;
  if (!(await fileExists(zipPath))) {
    throw new Error(`Indiana campaign finance contribution ZIP not found for ${input.year}: ${zipPath}`);
  }

  const metadata = input.rawDataZipPath
    ? null
    : await readIndianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  return {
    rows: await readIndianaCampaignFinanceContributionRows({
      zipPath,
      year: input.year,
      predicate: buildIndianaCandidateNamePredicate(input.candidates),
    }),
    sourceUrl:
      metadata?.remote.url ??
      buildIndianaCampaignFinanceArtifactUrl({ year: input.year, artifactKind: "contribution" }),
  };
}

export async function listDueIndianaCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: IndianaCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<IndianaCandidateFinanceDueQueryRow>(
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
          link.committee_name,
          link.source_url,
          summary.last_synced_at::text AS last_synced_at,
          COUNT(*) OVER () AS total_due_rows
        FROM public.in_candidate_finance_links AS link
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
        LEFT JOIN public.in_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'IN'
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
      [...INDIANA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueIndianaCandidateFinance(
  input: IndianaCandidateFinanceBatchSyncInput
): Promise<IndianaCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncIndianaCandidateFinanceFn ?? syncIndianaCandidateFinance;

  if (!dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const missingLinkCandidates = await listIndianaCandidateElectionsMissingFinanceLinks(input.db, {
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
      });
      const contributionRowsByYear = new Map<number, readonly IndianaCampaignFinanceContributionRow[]>();
      const sourceUrlByYear = new Map<number, string>();
      for (const [year, candidates] of groupAutoLinkCandidatesByYear(missingLinkCandidates).entries()) {
        const data = await loadAutoLinkContributionRowsForYear({
          year,
          candidates,
          rawDataZipPath: input.rawDataZipPath,
          rawDataCacheDir: input.rawDataCacheDir,
          contributionDataByYear: input.contributionDataByYear,
        });
        contributionRowsByYear.set(year, data.rows);
        sourceUrlByYear.set(year, data.sourceUrl);
      }
      await autoLinkMissingIndianaCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        contributionRowsByYear,
        sourceUrlByYear,
        candidateElections: missingLinkCandidates,
      });
    } catch (error) {
      console.warn(
        "Indiana finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueIndianaCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });
  const contributionDataByYear = new Map<number, IndianaContributionDataForYear>(
    input.contributionDataByYear ? [...input.contributionDataByYear.entries()] : []
  );
  const contributionDataLoadErrorsByYear = new Map<number, string>();
  for (const [year, rows] of groupDueRowsByYear(due.rows).entries()) {
    if (!contributionDataByYear.has(year)) {
      try {
        contributionDataByYear.set(
          year,
          await loadContributionDataForYear({
            year,
            committeeIds: rows.map((row) => row.committeeId),
            rawDataZipPath: input.rawDataZipPath,
            rawDataCacheDir: input.rawDataCacheDir,
          })
        );
      } catch (error) {
        contributionDataLoadErrorsByYear.set(year, error instanceof Error ? error.message : String(error));
      }
    }
  }

  const results: IndianaCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    const contributionDataLoadError = contributionDataLoadErrorsByYear.get(row.electionYear);
    if (contributionDataLoadError) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        committeeId: row.committeeId,
        ok: false,
        error: contributionDataLoadError,
      });
      continue;
    }

    const contributionData = contributionDataByYear.get(row.electionYear);
    const committeeKey = normalizeCommitteeId(row.committeeId);
    try {
      const result = await syncFn({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeScope: row.officeScope,
        officeName: row.officeName,
        district: row.district,
        sourceUrl: row.sourceUrl,
        contributionRows: contributionData?.rowsByCommitteeId.get(committeeKey) ?? [],
        contributionSourceUrl: contributionData?.sourceUrl,
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
    results,
  };
}
