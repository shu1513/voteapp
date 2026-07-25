import { stat } from "node:fs/promises";
import { resolve } from "node:path";
import type { Pool, PoolClient } from "pg";

import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import { mergeCycleArtifactRows } from "../finance/cycleArtifactRows.js";
import {
  autoLinkMissingMichiganCandidateFinanceLinks,
  listMichiganCandidateElectionsMissingFinanceLinks,
  type MichiganFinanceAutoLinkCandidateElection,
} from "./michiganCandidateFinanceAutoLink.js";
import { michiganElectionCycleStartYear } from "./michiganDirectContributionAggregator.js";
import {
  syncMichiganCandidateFinance,
  type MichiganCandidateFinanceSyncResult,
} from "./michiganCandidateFinanceSync.js";
import {
  normalizeMichiganCandidateNameKeys,
  resolveMichiganCandidateCommittee,
} from "./michiganCandidateCommitteeResolver.js";
import { MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./michiganFinanceEligibleOffices.js";
import {
  DEFAULT_MICHIGAN_MITN_LEGACY_ARCHIVE_CACHE_DIR,
  buildMichiganMitnLegacyArchiveUrl,
  getMichiganMitnLegacyArchiveCachePaths,
  readMichiganMitnLegacyArchiveCacheMetadata,
} from "./michiganMitnLegacyArtifactCache.js";
import {
  readMichiganMitnLegacyContributionRows,
  readMichiganMitnLegacyExpenditureRows,
  type MichiganMitnLegacyContributionRow,
  type MichiganMitnLegacyExpenditureRow,
} from "./michiganMitnLegacyArchiveReader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type MichiganCandidateFinanceDueRow = {
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

export type MichiganMitnLegacyDataForYear = {
  year: number;
  extractedDir: string;
  sourceUrl: string;
  contributionRows: MichiganMitnLegacyContributionRow[];
  expenditureRows: MichiganMitnLegacyExpenditureRow[];
};

export type MichiganCandidateFinanceBatchSyncInput = {
  db: Queryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataExtractedDir?: string;
  rawDataCacheDir?: string;
  mitnDataByYear?: ReadonlyMap<number, MichiganMitnLegacyDataForYear>;
  autoLinkMissingLinks?: boolean;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  syncMichiganCandidateFinanceFn?: typeof syncMichiganCandidateFinance;
};

export type MichiganCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: MichiganCandidateFinanceSyncResult;
  error?: string;
};

export type MichiganCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  results: MichiganCandidateFinanceBatchSyncItemResult[];
};

type MichiganCandidateFinanceDueQueryRow = {
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
    throw new Error(`Invalid Michigan finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Michigan finance batch sync ${label}: ${normalized}`);
  }
  return normalized;
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
}

function parseMichiganMitnDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  return null;
}

function buildElectionCycleYearSet(rows: readonly MichiganCandidateFinanceDueRow[]): Set<number> {
  const years = new Set<number>();
  for (const row of rows) {
    years.add(row.electionYear - 1);
    years.add(row.electionYear);
  }
  return years;
}

function isDateInAnyElectionCycle(rawDate: string, electionCycleYears: ReadonlySet<number>): boolean {
  const year = parseMichiganMitnDateYear(rawDate);
  return year !== null && electionCycleYears.has(year);
}

function parseStatementYear(raw: string): number | null {
  const normalized = raw.trim();
  if (!/^\d{4}$/.test(normalized)) {
    return null;
  }
  return Number.parseInt(normalized, 10);
}

function isStatementYearInAnyElectionCycle(rawYear: string, electionCycleYears: ReadonlySet<number>): boolean {
  const year = parseStatementYear(rawYear);
  return year !== null && electionCycleYears.has(year);
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function mapDueRow(row: MichiganCandidateFinanceDueQueryRow): MichiganCandidateFinanceDueRow {
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

async function directoryExists(path: string): Promise<boolean> {
  try {
    const fileStat = await stat(path);
    return fileStat.isDirectory();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

function groupDueRowsByYear(rows: readonly MichiganCandidateFinanceDueRow[]): Map<number, MichiganCandidateFinanceDueRow[]> {
  const byYear = new Map<number, MichiganCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function groupAutoLinkCandidatesByYear(
  rows: readonly MichiganFinanceAutoLinkCandidateElection[]
): Map<number, MichiganFinanceAutoLinkCandidateElection[]> {
  const byYear = new Map<number, MichiganFinanceAutoLinkCandidateElection[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function defaultExtractedDir(input: { cacheDir: string; year: number }): string {
  return resolve(input.cacheDir, `${input.year}_mi_cfr`);
}

// `||` (not `??`) for the env fallback: a whitespace-only value trims to "" and
// would otherwise resolve to the process CWD.
function resolveMitnCacheDir(rawDataCacheDir?: string): string {
  return (
    rawDataCacheDir ??
    (process.env.MICHIGAN_MITN_LEGACY_ARCHIVE_CACHE_DIR?.trim() || DEFAULT_MICHIGAN_MITN_LEGACY_ARCHIVE_CACHE_DIR)
  );
}

async function sourceUrlForYear(input: { year: number; rawDataCacheDir?: string }): Promise<string> {
  const cacheDir = resolveMitnCacheDir(input.rawDataCacheDir);
  const metadata = await readMichiganMitnLegacyArchiveCacheMetadata(
    getMichiganMitnLegacyArchiveCachePaths({ cacheDir, year: input.year }).metadataPath
  );
  return metadata?.remote.url ?? buildMichiganMitnLegacyArchiveUrl({ year: input.year });
}

// Michigan MiTN legacy archives are keyed by FILING (statement) year, but a
// Michigan election cycle spans [electionYear - 1, electionYear] — the window
// the aggregators and the predicates below already apply. Annual statements
// filed in January of the election year carry mostly prior-year received
// dates, while receipts reported during the prior year live only in the
// prior-year archive, so every load must read both filing years or the
// prior-year-filed portion of the cycle is silently dropped. There is no
// single-year override exception: an explicit --raw-extracted-dir points at a
// directory, the readers filter files by their `{year}_` name prefix, and one
// directory can therefore hold both filing years' CSV files.
function michiganCycleFilingYears(electionYear: number): number[] {
  return [michiganElectionCycleStartYear(electionYear), electionYear];
}

function michiganContributionRowIdentity(row: MichiganMitnLegacyContributionRow): string {
  const documentSequenceNumber = row.doc_seq_no.trim();
  const contributionId = row.contribution_id.trim();
  return documentSequenceNumber && contributionId
    ? `${documentSequenceNumber}\u0000${contributionId}\u0000${row.cont_detail_id.trim()}`
    : "";
}

// expense_id / detail_id exist in the official export but are not part of the
// required typed columns; rows missing them fall back to the merge's unkeyed
// path, which keeps them (archives are disjoint filing-year partitions).
function michiganExpenditureRowIdentity(row: MichiganMitnLegacyExpenditureRow): string {
  const record = row as Record<string, string | undefined>;
  const documentSequenceNumber = row.doc_seq_no.trim();
  const expenseId = record["expense_id"]?.trim() ?? "";
  const detailId = record["detail_id"]?.trim() ?? "";
  return documentSequenceNumber && expenseId
    ? `${documentSequenceNumber}\u0000${expenseId}\u0000${detailId}`
    : "";
}

async function readCycleRows<Row>(input: {
  electionYear: number;
  rawDataExtractedDir?: string;
  rawDataCacheDir?: string;
  rowIdentity: (row: Row) => string;
  readRowsForFilingYear: (filingYear: number, extractedDir: string) => Promise<Row[]>;
}): Promise<{ rows: Row[]; extractedDir: string; sourceUrl: string }> {
  const cacheDir = resolveMitnCacheDir(input.rawDataCacheDir);
  const artifactRowsByYear: Row[][] = [];
  let selectedExtractedDir = "";
  let selectedSourceUrl = "";
  let foundMatchingRows = false;
  for (const filingYear of michiganCycleFilingYears(input.electionYear)) {
    const extractedDir = resolve(input.rawDataExtractedDir ?? defaultExtractedDir({ cacheDir, year: filingYear }));
    if (!(await directoryExists(extractedDir))) {
      throw new Error(`Michigan MiTN legacy extracted CSV directory not found for ${filingYear}: ${extractedDir}`);
    }
    const artifactRows = await input.readRowsForFilingYear(filingYear, extractedDir);
    artifactRowsByYear.push(artifactRows);
    if (!foundMatchingRows) {
      selectedExtractedDir = extractedDir;
      selectedSourceUrl = await sourceUrlForYear({ year: filingYear, rawDataCacheDir: input.rawDataCacheDir });
      foundMatchingRows = artifactRows.length > 0;
    }
  }
  return {
    rows: mergeCycleArtifactRows({ artifacts: artifactRowsByYear, rowIdentity: input.rowIdentity }),
    extractedDir: selectedExtractedDir,
    sourceUrl: selectedSourceUrl,
  };
}

function candidateNameKeysFromContributionRow(row: MichiganMitnLegacyContributionRow): Set<string> {
  return normalizeMichiganCandidateNameKeys([row.can_first_name, row.can_last_name].filter(Boolean).join(" "));
}

function buildMichiganCandidateContributionPredicate(
  candidates: readonly MichiganFinanceAutoLinkCandidateElection[]
): (row: MichiganMitnLegacyContributionRow) => boolean {
  const dueCandidateNameKeys = new Set<string>();
  for (const candidate of candidates) {
    for (const key of normalizeMichiganCandidateNameKeys(candidate.candidateName)) {
      dueCandidateNameKeys.add(key);
    }
  }

  return (row) => {
    for (const rowKey of candidateNameKeysFromContributionRow(row)) {
      if (dueCandidateNameKeys.has(rowKey)) {
        return true;
      }
    }
    return false;
  };
}

function candidateNameMatchesExpenditure(input: {
  row: MichiganMitnLegacyExpenditureRow;
  dueCandidateNameKeys: ReadonlySet<string>;
}): boolean {
  for (const rowKey of normalizeMichiganCandidateNameKeys(input.row.can_or_ballot)) {
    if (input.dueCandidateNameKeys.has(rowKey)) {
      return true;
    }
  }
  return false;
}

function buildMichiganOutsideExpenditurePredicate(
  rows: readonly MichiganCandidateFinanceDueRow[]
): (row: MichiganMitnLegacyExpenditureRow) => boolean {
  const dueCandidateNameKeys = new Set<string>();
  for (const candidate of rows) {
    for (const key of normalizeMichiganCandidateNameKeys(candidate.candidateName)) {
      dueCandidateNameKeys.add(key);
    }
  }
  const electionCycleYears = buildElectionCycleYearSet(rows);

  return (row) =>
    candidateNameMatchesExpenditure({ row, dueCandidateNameKeys }) &&
    isStatementYearInAnyElectionCycle(row.doc_stmnt_year, electionCycleYears);
}

function collectCommitteeIdsForContributionLoad(input: {
  dueRows: readonly MichiganCandidateFinanceDueRow[];
  expenditureRows: readonly MichiganMitnLegacyExpenditureRow[];
}): string[] {
  const committeeIds = new Set<string>();
  for (const row of input.dueRows) {
    const committeeId = normalizeId(row.committeeId);
    if (committeeId) {
      committeeIds.add(committeeId);
    }
  }
  for (const row of input.expenditureRows) {
    const committeeId = normalizeId(row.cfr_com_id);
    if (committeeId) {
      committeeIds.add(committeeId);
    }
  }
  return [...committeeIds];
}

async function loadContributionRows(input: {
  electionYear: number;
  committeeIds: readonly string[];
  dueRows: readonly MichiganCandidateFinanceDueRow[];
  rawDataExtractedDir?: string;
  rawDataCacheDir?: string;
}): Promise<{ rows: MichiganMitnLegacyContributionRow[]; extractedDir: string; sourceUrl: string }> {
  const committeeIds = new Set(input.committeeIds.map(normalizeId).filter(Boolean));
  const electionCycleYears = buildElectionCycleYearSet(input.dueRows);
  return await readCycleRows({
    electionYear: input.electionYear,
    rawDataExtractedDir: input.rawDataExtractedDir,
    rawDataCacheDir: input.rawDataCacheDir,
    rowIdentity: michiganContributionRowIdentity,
    readRowsForFilingYear: (filingYear, extractedDir) =>
      readMichiganMitnLegacyContributionRows({
        extractedDir,
        year: filingYear,
        predicate: (row) =>
          committeeIds.has(normalizeId(row.cfr_com_id)) &&
          isDateInAnyElectionCycle(row.received_date, electionCycleYears),
      }),
  });
}

async function loadMichiganMitnDataForYear(input: {
  year: number;
  dueRows: readonly MichiganCandidateFinanceDueRow[];
  rawDataExtractedDir?: string;
  rawDataCacheDir?: string;
}): Promise<MichiganMitnLegacyDataForYear> {
  const expenditureData = await readCycleRows({
    electionYear: input.year,
    rawDataExtractedDir: input.rawDataExtractedDir,
    rawDataCacheDir: input.rawDataCacheDir,
    rowIdentity: michiganExpenditureRowIdentity,
    readRowsForFilingYear: (filingYear, extractedDir) =>
      readMichiganMitnLegacyExpenditureRows({
        extractedDir,
        year: filingYear,
        predicate: buildMichiganOutsideExpenditurePredicate(input.dueRows),
      }),
  });
  const contributionData = await loadContributionRows({
    electionYear: input.year,
    committeeIds: collectCommitteeIdsForContributionLoad({
      dueRows: input.dueRows,
      expenditureRows: expenditureData.rows,
    }),
    dueRows: input.dueRows,
    rawDataExtractedDir: input.rawDataExtractedDir,
    rawDataCacheDir: input.rawDataCacheDir,
  });

  return {
    year: input.year,
    extractedDir: contributionData.extractedDir,
    sourceUrl: contributionData.sourceUrl,
    contributionRows: contributionData.rows,
    expenditureRows: expenditureData.rows,
  };
}

async function loadAutoLinkContributionRowsForYear(input: {
  year: number;
  candidates: readonly MichiganFinanceAutoLinkCandidateElection[];
  rawDataExtractedDir?: string;
  rawDataCacheDir?: string;
}): Promise<{ rows: MichiganMitnLegacyContributionRow[]; sourceUrl: string }> {
  const data = await readCycleRows({
    electionYear: input.year,
    rawDataExtractedDir: input.rawDataExtractedDir,
    rawDataCacheDir: input.rawDataCacheDir,
    rowIdentity: michiganContributionRowIdentity,
    readRowsForFilingYear: (filingYear, extractedDir) =>
      readMichiganMitnLegacyContributionRows({
        extractedDir,
        year: filingYear,
        predicate: buildMichiganCandidateContributionPredicate(input.candidates),
      }),
  });
  return { rows: data.rows, sourceUrl: data.sourceUrl };
}

export async function listDueMichiganCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: MichiganCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<MichiganCandidateFinanceDueQueryRow>(
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
        FROM public.mi_candidate_finance_links AS link
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
        LEFT JOIN public.mi_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'MI'
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
      [...MICHIGAN_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueMichiganCandidateFinance(
  input: MichiganCandidateFinanceBatchSyncInput
): Promise<MichiganCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncMichiganCandidateFinanceFn ?? syncMichiganCandidateFinance;

  if (!dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const missingLinkCandidates = await listMichiganCandidateElectionsMissingFinanceLinks(input.db, {
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
      });
      const contributionRowsByYear = new Map<number, MichiganMitnLegacyContributionRow[]>();
      const sourceUrlByYear = new Map<number, string>();
      for (const [year, candidates] of groupAutoLinkCandidatesByYear(missingLinkCandidates).entries()) {
        const injectedData = input.mitnDataByYear?.get(year);
        if (injectedData) {
          contributionRowsByYear.set(year, injectedData.contributionRows);
          sourceUrlByYear.set(year, injectedData.sourceUrl);
          continue;
        }
        const data = await loadAutoLinkContributionRowsForYear({
          year,
          candidates,
          rawDataExtractedDir: input.rawDataExtractedDir,
          rawDataCacheDir: input.rawDataCacheDir,
        });
        contributionRowsByYear.set(year, data.rows);
        sourceUrlByYear.set(year, data.sourceUrl);
      }
      await autoLinkMissingMichiganCandidateFinanceLinks({
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
        "Michigan finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueMichiganCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });
  const mitnDataByYear = new Map<number, MichiganMitnLegacyDataForYear>(
    input.mitnDataByYear ? [...input.mitnDataByYear.entries()] : []
  );
  const mitnLoadErrorsByYear = new Map<number, string>();
  for (const [year, rows] of groupDueRowsByYear(due.rows).entries()) {
    if (!mitnDataByYear.has(year)) {
      try {
        mitnDataByYear.set(
          year,
          await loadMichiganMitnDataForYear({
            year,
            dueRows: rows,
            rawDataExtractedDir: input.rawDataExtractedDir,
            rawDataCacheDir: input.rawDataCacheDir,
          })
        );
      } catch (error) {
        mitnLoadErrorsByYear.set(year, error instanceof Error ? error.message : String(error));
      }
    }
  }

  const results: MichiganCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    const mitnLoadError = mitnLoadErrorsByYear.get(row.electionYear);
    if (mitnLoadError) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        committeeId: row.committeeId,
        ok: false,
        error: `Michigan MiTN data load failed for ${row.electionYear}: ${mitnLoadError}`,
      });
      continue;
    }
    const mitnData = mitnDataByYear.get(row.electionYear);
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
        sourceUrl: row.sourceUrl ?? mitnData?.sourceUrl ?? null,
        contributionSourceUrl: mitnData?.sourceUrl,
        outsideSourceUrl: mitnData?.sourceUrl,
        contributionRows: mitnData?.contributionRows ?? [],
        expenditureRows: mitnData?.expenditureRows ?? [],
        trustedCommittee: {
          committeeId: row.committeeId,
          committeeName: row.committeeName,
          sourceUrl: row.sourceUrl ?? mitnData?.sourceUrl ?? null,
        },
        financeIndustryClassifier: input.financeIndustryClassifier,
        aiClassificationMinAmount: input.aiClassificationMinAmount,
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
