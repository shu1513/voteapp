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
  MICHIGAN_MITN_LEGACY_FINAL_ARCHIVE_YEAR,
  MICHIGAN_MITN_LEGACY_FIRST_ARCHIVE_YEAR,
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
import {
  MICHIGAN_MITN_PUBLIC_SEARCH_BASE_URL,
  MICHIGAN_MITN_STATEMENT_YEAR_IDS,
  dedupeMichiganMitnExportRows,
  fetchMichiganMitnContributionExportXlsx,
  michiganMitnExportRowsToLegacyContributionRows,
  parseMichiganMitnExportXlsxRows,
  resolveMichiganMitnCommitteeViaSearch,
  type MichiganMitnFetchFn,
} from "./michiganMitnPublicSearchClient.js";
import { toMichiganMitnOfficeSearchInput } from "./michiganFinanceEligibleOffices.js";
import { normalizeMichiganCandidateNameForStorage } from "./michiganCandidateCommitteeResolver.js";
import { upsertMichiganFinanceLink } from "./michiganFinanceWriter.js";
import { isMichiganMitnRawDataRefreshEnabled } from "../../config/featureFlags.js";

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
  // Provenance for the outside-spending rows, which may come from a different
  // cycle filing-year archive than the contributions. Falls back to sourceUrl.
  outsideSourceUrl?: string;
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
  mitnPublicSearchFetchFn?: MichiganMitnFetchFn;
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
// filed in January carry mostly prior-calendar-year received dates, so a
// cycle's receipts are split across filing-year archives by WHEN they were
// reported: prior-year receipts reported during the prior year live in the
// prior-year archive, and election-year receipts reported on the following
// January's annual statement live in the NEXT filing year's archive. Every
// load therefore reads [electionYear - 1, electionYear], plus the following
// filing year when that archive can exist (the legacy export is frozen at
// MICHIGAN_MITN_LEGACY_FINAL_ARCHIVE_YEAR, so an active 2026 cycle still
// requires — and loudly fails on — its own nonexistent archive rather than
// silently succeeding on partial prior-year data). There is no single-year
// override exception: an explicit --raw-extracted-dir points at a directory,
// the readers filter files by their `{year}_` name prefix, and one directory
// can therefore hold every cycle filing year's CSV files.
function michiganCycleFilingYears(electionYear: number): number[] {
  // Clamp the start to the first archive that exists (2020): a 2020 election's
  // 2019-filed statements were never exported anywhere, so skipping them is a
  // dataset boundary, not a silent undercount. The election year itself stays
  // unclamped — an active cycle past the frozen final archive must fail loudly
  // rather than succeed on partial prior-year data. The Set collapses the
  // duplicate when the clamped start equals the election year.
  const years = new Set<number>();
  years.add(Math.max(michiganElectionCycleStartYear(electionYear), MICHIGAN_MITN_LEGACY_FIRST_ARCHIVE_YEAR));
  years.add(electionYear);
  if (electionYear + 1 <= MICHIGAN_MITN_LEGACY_FINAL_ARCHIVE_YEAR) {
    years.add(electionYear + 1);
  }
  return [...years];
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
    outsideSourceUrl: expenditureData.sourceUrl,
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


const MICHIGAN_MITN_COMMITTEE_SEARCH_SOURCE_URL = `${MICHIGAN_MITN_PUBLIC_SEARCH_BASE_URL}?page=page.miboeCommitteePublicSearch`;
const MICHIGAN_MITN_CONTRIBUTION_SEARCH_SOURCE_URL = `${MICHIGAN_MITN_PUBLIC_SEARCH_BASE_URL}?page=page.miboeContributionPublicSearch`;

function isMitnPublicSearchYear(electionYear: number): boolean {
  return electionYear > MICHIGAN_MITN_LEGACY_FINAL_ARCHIVE_YEAR;
}

const MITN_PUBLIC_SEARCH_FETCH_TIMEOUT_MS = 120_000;

function defaultMitnPublicSearchFetchFn(): MichiganMitnFetchFn {
  // One hung MiTN response must not stall the whole batch — the due loop
  // awaits each committee's exports sequentially.
  return (url, init) => fetch(url, { ...init, signal: AbortSignal.timeout(MITN_PUBLIC_SEARCH_FETCH_TIMEOUT_MS) });
}

/**
 * Auto-links candidates whose election year has no legacy archive by asking
 * the MiTN public committee search (candidate name + office as server-side
 * filters; exactly one ACTIVE candidate committee links, anything else is
 * refused). Network access is gated by the raw-data-refresh flag.
 */
async function autoLinkMichiganCandidatesViaMitnPublicSearch(input: {
  db: Queryable;
  now: Date;
  candidates: readonly MichiganFinanceAutoLinkCandidateElection[];
  fetchFn: MichiganMitnFetchFn;
}): Promise<void> {
  for (const candidate of input.candidates) {
    try {
      const officeSearchInput = toMichiganMitnOfficeSearchInput({
        officeScope: candidate.officeScope,
        officeCanonicalName: candidate.officeName,
        district: candidate.district,
      });
      if (!officeSearchInput) {
        console.warn("Michigan MiTN public-search auto-link skipped candidate with unsupported office:", {
          candidateId: candidate.candidateId,
          officeName: candidate.officeName,
        });
        continue;
      }
      const resolution = await resolveMichiganMitnCommitteeViaSearch({
        candidateName: candidate.candidateName,
        mitnOffice: officeSearchInput.mitnOffice,
        fetchFn: input.fetchFn,
      });
      if (resolution.status !== "matched") {
        console.warn("Michigan MiTN public-search auto-link did not link candidate election:", {
          candidateId: candidate.candidateId,
          candidateName: candidate.candidateName,
          status: resolution.status,
          ...(resolution.status === "ambiguous"
            ? { matches: resolution.matches.map((match) => `${match.committeeId} ${match.committeeName}`) }
            : { reason: resolution.reason }),
        });
        continue;
      }
      await upsertMichiganFinanceLink({
        db: input.db,
        link: {
          candidateId: candidate.candidateId,
          electionId: candidate.electionId,
          electionYear: candidate.electionYear,
          candidateNameNormalized: normalizeMichiganCandidateNameForStorage(candidate.candidateName),
          officeName: candidate.officeName,
          district: candidate.district,
          committeeId: resolution.committeeId,
          committeeName: resolution.committeeName,
          linkStatus: "active",
          linkSource: "mitn_public_search",
          sourceUrl: MICHIGAN_MITN_COMMITTEE_SEARCH_SOURCE_URL,
          lastVerifiedAt: input.now,
        },
      });
    } catch (error) {
      console.warn("Michigan MiTN public-search auto-link failed for candidate election; continuing:", {
        candidateId: candidate.candidateId,
        electionId: candidate.electionId,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
}

/**
 * Loads a committee's contribution rows from the MiTN public-search exports
 * for the cycle's statement years, deduped ACROSS years (amendments repeat
 * receipts, and can restate a prior year's statement), then mapped onto the
 * legacy row shape. Raw exports are memoized per committee within one run.
 *
 * The cycle's own statement years (election year - 1 and the election year)
 * are REQUIRED — a missing year-id mapping throws rather than silently
 * writing a partial or zero snapshot. The following filing year (January
 * annual statements reporting election-year receipts, mirroring the legacy
 * loader) is included when its id is known.
 */
async function loadMitnPublicSearchContributionRows(input: {
  committeeId: string;
  electionYear: number;
  fetchFn: MichiganMitnFetchFn;
  cache: Map<string, string[][]>;
}): Promise<MichiganMitnLegacyContributionRow[]> {
  const requiredStatementYears = [input.electionYear - 1, input.electionYear];
  for (const year of requiredStatementYears) {
    if (!MICHIGAN_MITN_STATEMENT_YEAR_IDS.has(year)) {
      throw new Error(
        `No Michigan MiTN statement-year id for ${year}; add it to MICHIGAN_MITN_STATEMENT_YEAR_IDS before syncing ${input.electionYear} elections`
      );
    }
  }
  const statementYears = [...requiredStatementYears, input.electionYear + 1].filter((year) =>
    MICHIGAN_MITN_STATEMENT_YEAR_IDS.has(year)
  );

  const combinedRows: string[][] = [];
  for (const statementYear of statementYears) {
    const cacheKey = `${input.committeeId}:${statementYear}`;
    let rawRows = input.cache.get(cacheKey);
    if (!rawRows) {
      const xlsx = await fetchMichiganMitnContributionExportXlsx({
        committeeId: input.committeeId,
        statementYear,
        fetchFn: input.fetchFn,
      });
      rawRows = parseMichiganMitnExportXlsxRows(xlsx);
      input.cache.set(cacheKey, rawRows);
    }
    if (rawRows.length === 0) {
      continue;
    }
    if (combinedRows.length === 0) {
      combinedRows.push(...rawRows.map((row) => [...row]));
    } else {
      combinedRows.push(...rawRows.slice(1).map((row) => [...row]));
    }
  }
  if (combinedRows.length === 0) {
    return [];
  }
  return michiganMitnExportRowsToLegacyContributionRows(dedupeMichiganMitnExportRows(combinedRows));
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
      const allMissingLinkCandidates = await listMichiganCandidateElectionsMissingFinanceLinks(input.db, {
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
      });
      const missingLinkCandidates = allMissingLinkCandidates.filter(
        (candidate) => !isMitnPublicSearchYear(candidate.electionYear)
      );
      const publicSearchCandidates = allMissingLinkCandidates.filter((candidate) =>
        isMitnPublicSearchYear(candidate.electionYear)
      );
      if (publicSearchCandidates.length > 0) {
        if (isMichiganMitnRawDataRefreshEnabled()) {
          await autoLinkMichiganCandidatesViaMitnPublicSearch({
            db: input.db,
            now,
            candidates: publicSearchCandidates,
            fetchFn: input.mitnPublicSearchFetchFn ?? defaultMitnPublicSearchFetchFn(),
          });
        } else {
          console.warn(
            "Michigan MiTN public-search auto-link skipped: MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED is off",
            { skippedCandidateCount: publicSearchCandidates.length }
          );
        }
      }
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
      const autoLinkResults = await autoLinkMissingMichiganCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        contributionRowsByYear,
        sourceUrlByYear,
        candidateElections: missingLinkCandidates,
      });
      for (const result of autoLinkResults) {
        if (result.status !== "linked") {
          console.warn("Michigan finance auto-link did not link candidate election:", result);
        }
      }
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
    if (isMitnPublicSearchYear(year)) {
      continue;
    }
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
  const publicSearchExportCache = new Map<string, string[][]>();
  const publicSearchFetchFn = input.mitnPublicSearchFetchFn ?? defaultMitnPublicSearchFetchFn();
  for (const row of due.rows) {
    if (isMitnPublicSearchYear(row.electionYear)) {
      if (!isMichiganMitnRawDataRefreshEnabled()) {
        results.push({
          candidateId: row.candidateId,
          electionId: row.electionId,
          electionYear: row.electionYear,
          committeeId: row.committeeId,
          ok: false,
          error:
            "Michigan MiTN public-search fetch disabled (MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED is off); cannot sync a post-legacy election year",
        });
        continue;
      }
      try {
        const contributionRows = await loadMitnPublicSearchContributionRows({
          committeeId: row.committeeId,
          electionYear: row.electionYear,
          fetchFn: publicSearchFetchFn,
          cache: publicSearchExportCache,
        });
        const result = await syncFn({
          db: input.db,
          candidateId: row.candidateId,
          electionId: row.electionId,
          candidateName: row.candidateName,
          electionYear: row.electionYear,
          officeScope: row.officeScope,
          officeName: row.officeName,
          district: row.district,
          sourceUrl: row.sourceUrl ?? MICHIGAN_MITN_CONTRIBUTION_SEARCH_SOURCE_URL,
          contributionSourceUrl: MICHIGAN_MITN_CONTRIBUTION_SEARCH_SOURCE_URL,
          outsideSourceUrl: MICHIGAN_MITN_CONTRIBUTION_SEARCH_SOURCE_URL,
          contributionRows,
          // Outside spending is not ingested from MiTN yet — the expenditure
          // search is a separate integration. expenditureRows stays OMITTED:
          // a defined array (even empty) marks outside data as available and
          // would persist $0 totals and delete prior outside-group rows.
          linkSource: "mitn_public_search",
          trustedCommittee: {
            committeeId: row.committeeId,
            committeeName: row.committeeName,
            sourceUrl: row.sourceUrl ?? MICHIGAN_MITN_CONTRIBUTION_SEARCH_SOURCE_URL,
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
      continue;
    }
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
        outsideSourceUrl: mitnData?.outsideSourceUrl ?? mitnData?.sourceUrl,
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
