import { stat } from "node:fs/promises";
import type { Pool, PoolClient } from "pg";

import { mergeCycleArtifactRows } from "../finance/cycleArtifactRows.js";
import {
  autoLinkMissingNebraskaCandidateFinanceLinks,
  buildNebraskaCandidateNamePredicate,
  listNebraskaCandidateElectionsMissingFinanceLinks,
  type NebraskaFinanceAutoLinkCandidateElection,
} from "./nebraskaCandidateFinanceAutoLink.js";
import { nebraskaElectionCycleStartYear } from "./nebraskaDirectContributionAggregator.js";
import {
  syncNebraskaCandidateFinance,
  type NebraskaCandidateFinanceSyncResult,
} from "./nebraskaCandidateFinanceSync.js";
import { NEBRASKA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./nebraskaFinanceEligibleOffices.js";
import {
  DEFAULT_NEBRASKA_NADC_CACHE_DIR,
  buildNebraskaNadcArtifactUrl,
  getNebraskaNadcArtifactCachePaths,
  readNebraskaNadcArtifactCacheMetadata,
} from "./nebraskaNadcArtifactCache.js";
import {
  readNebraskaNadcContributionRows,
  type NebraskaNadcContributionRow,
} from "./nebraskaNadcArtifactReader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type NebraskaCandidateFinanceDueRow = {
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

export type NebraskaContributionDataForYear = {
  year: number;
  zipPath: string;
  sourceUrl: string;
  rowsByCommitteeId: Map<string, NebraskaNadcContributionRow[]>;
};

export type NebraskaCandidateFinanceBatchSyncInput = {
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
  contributionDataByYear?: ReadonlyMap<number, NebraskaContributionDataForYear>;
  syncNebraskaCandidateFinanceFn?: typeof syncNebraskaCandidateFinance;
};

export type NebraskaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: NebraskaCandidateFinanceSyncResult;
  error?: string;
};

export type NebraskaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  results: NebraskaCandidateFinanceBatchSyncItemResult[];
};

type NebraskaCandidateFinanceDueQueryRow = {
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
// Keep one extra calendar day so UTC scheduler timing cannot skip election-night finance syncs.
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid Nebraska finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Nebraska finance batch sync ${label}: ${value}`);
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

function mapDueRow(row: NebraskaCandidateFinanceDueQueryRow): NebraskaCandidateFinanceDueRow {
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

function groupDueRowsByYear(rows: readonly NebraskaCandidateFinanceDueRow[]): Map<number, NebraskaCandidateFinanceDueRow[]> {
  const byYear = new Map<number, NebraskaCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function groupAutoLinkCandidatesByYear(
  rows: readonly NebraskaFinanceAutoLinkCandidateElection[]
): Map<number, NebraskaFinanceAutoLinkCandidateElection[]> {
  const byYear = new Map<number, NebraskaFinanceAutoLinkCandidateElection[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function groupContributionRowsByCommittee(
  rows: readonly NebraskaNadcContributionRow[]
): Map<string, NebraskaNadcContributionRow[]> {
  const byCommittee = new Map<string, NebraskaNadcContributionRow[]>();
  for (const row of rows) {
    const committeeId = normalizeCommitteeId(row["Org ID"]);
    if (!committeeId) {
      continue;
    }
    const existing = byCommittee.get(committeeId) ?? [];
    existing.push(row);
    byCommittee.set(committeeId, existing);
  }
  return byCommittee;
}

// Nebraska NADC bulk extracts are keyed by FILING year, but a Nebraska election
// cycle spans [electionYear - 1, electionYear] (the window the aggregator's
// isCycleYear already applies), so every cache load must read both filing years
// or in-cycle receipts filed in the earlier year are silently dropped. An
// explicit --raw-zip override can only satisfy the single year baked into its
// inner CSV name, so it stays single-year.
function nebraskaCycleFilingYears(electionYear: number, rawDataZipPath?: string): number[] {
  return rawDataZipPath ? [electionYear] : [nebraskaElectionCycleStartYear(electionYear), electionYear];
}

function nebraskaContributionRowIdentity(row: NebraskaNadcContributionRow): string {
  const receiptId = row["Receipt ID"].trim().toUpperCase();
  return receiptId ? `${normalizeCommitteeId(row["Org ID"])}\u0000${receiptId}` : "";
}

async function readCycleContributionRows(input: {
  electionYear: number;
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
  predicate: (row: NebraskaNadcContributionRow) => boolean;
}): Promise<{ rows: NebraskaNadcContributionRow[]; zipPath: string; sourceUrl: string }> {
  // `||` (not `??`): a whitespace-only NEBRASKA_NADC_CACHE_DIR trims to "" and
  // would otherwise resolve to the process CWD.
  const cacheDir =
    input.rawDataCacheDir ?? (process.env.NEBRASKA_NADC_CACHE_DIR?.trim() || DEFAULT_NEBRASKA_NADC_CACHE_DIR);
  const artifactRowsByYear: NebraskaNadcContributionRow[][] = [];
  let zipPath = "";
  let sourceUrl = "";
  let foundMatchingRows = false;
  for (const filingYear of nebraskaCycleFilingYears(input.electionYear, input.rawDataZipPath)) {
    const paths = getNebraskaNadcArtifactCachePaths({
      cacheDir,
      year: filingYear,
      artifactKind: "contribution_loan",
    });
    const artifactZipPath = input.rawDataZipPath ?? paths.zipPath;
    if (!(await fileExists(artifactZipPath))) {
      throw new Error(`Nebraska NADC contribution ZIP not found for ${filingYear}: ${artifactZipPath}`);
    }
    const metadata = input.rawDataZipPath ? null : await readNebraskaNadcArtifactCacheMetadata(paths.metadataPath);
    const artifactRows = await readNebraskaNadcContributionRows({
      zipPath: artifactZipPath,
      year: filingYear,
      predicate: input.predicate,
    });
    artifactRowsByYear.push(artifactRows);
    if (!foundMatchingRows) {
      zipPath = artifactZipPath;
      sourceUrl =
        metadata?.remote.url ?? buildNebraskaNadcArtifactUrl({ year: filingYear, artifactKind: "contribution_loan" });
      foundMatchingRows = artifactRows.length > 0;
    }
  }
  return {
    rows: mergeCycleArtifactRows({ artifacts: artifactRowsByYear, rowIdentity: nebraskaContributionRowIdentity }),
    zipPath,
    sourceUrl,
  };
}

async function loadContributionDataForYear(input: {
  year: number;
  committeeIds: readonly string[];
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
}): Promise<NebraskaContributionDataForYear> {
  const normalizedCommitteeIds = new Set(input.committeeIds.map(normalizeCommitteeId).filter(Boolean));
  const data = await readCycleContributionRows({
    electionYear: input.year,
    rawDataZipPath: input.rawDataZipPath,
    rawDataCacheDir: input.rawDataCacheDir,
    predicate: (row) => normalizedCommitteeIds.has(normalizeCommitteeId(row["Org ID"])),
  });

  return {
    year: input.year,
    zipPath: data.zipPath,
    sourceUrl: data.sourceUrl,
    rowsByCommitteeId: groupContributionRowsByCommittee(data.rows),
  };
}

async function loadAutoLinkContributionRowsForYear(input: {
  year: number;
  candidates: readonly NebraskaFinanceAutoLinkCandidateElection[];
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
  contributionDataByYear?: ReadonlyMap<number, NebraskaContributionDataForYear>;
}): Promise<{ rows: NebraskaNadcContributionRow[]; sourceUrl: string }> {
  const injected = input.contributionDataByYear?.get(input.year);
  if (injected) {
    return {
      rows: [...injected.rowsByCommitteeId.values()].flat(),
      sourceUrl: injected.sourceUrl,
    };
  }

  const data = await readCycleContributionRows({
    electionYear: input.year,
    rawDataZipPath: input.rawDataZipPath,
    rawDataCacheDir: input.rawDataCacheDir,
    predicate: buildNebraskaCandidateNamePredicate(input.candidates),
  });
  return { rows: data.rows, sourceUrl: data.sourceUrl };
}

export async function listDueNebraskaCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: NebraskaCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<NebraskaCandidateFinanceDueQueryRow>(
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
        FROM public.ne_candidate_finance_links AS link
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
        LEFT JOIN public.ne_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'NE'
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
      [...NEBRASKA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueNebraskaCandidateFinance(
  input: NebraskaCandidateFinanceBatchSyncInput
): Promise<NebraskaCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncNebraskaCandidateFinanceFn ?? syncNebraskaCandidateFinance;

  if (!dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const missingLinkCandidates = await listNebraskaCandidateElectionsMissingFinanceLinks(input.db, {
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
      });
      const contributionRowsByYear = new Map<number, readonly NebraskaNadcContributionRow[]>();
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
      await autoLinkMissingNebraskaCandidateFinanceLinks({
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
        "Nebraska finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueNebraskaCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });
  const contributionDataByYear = new Map<number, NebraskaContributionDataForYear>(
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

  const results: NebraskaCandidateFinanceBatchSyncItemResult[] = [];
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
