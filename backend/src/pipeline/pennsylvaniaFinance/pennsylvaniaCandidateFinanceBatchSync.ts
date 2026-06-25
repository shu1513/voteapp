import { stat } from "node:fs/promises";
import { resolve } from "node:path";
import type { Pool, PoolClient } from "pg";

import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR,
  buildPennsylvaniaCampaignFinanceExportUrl,
  getPennsylvaniaCampaignFinanceExportCachePaths,
  readPennsylvaniaCampaignFinanceExportCacheMetadata,
} from "./pennsylvaniaCampaignFinanceArtifactCache.js";
import {
  readPennsylvaniaCampaignFinanceContributionRows,
  readPennsylvaniaCampaignFinanceFilerRows,
  type PennsylvaniaCampaignFinanceContributionRow,
  type PennsylvaniaCampaignFinanceFilerRow,
} from "./pennsylvaniaCampaignFinanceReader.js";
import {
  syncPennsylvaniaCandidateFinance,
  type PennsylvaniaCandidateFinanceSyncResult,
} from "./pennsylvaniaCandidateFinanceSync.js";
import { PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./pennsylvaniaFinanceEligibleOffices.js";
import {
  resolvePennsylvaniaOutsideGroupsForContributionAggregation,
  type PennsylvaniaOutsideSpendingGroup,
} from "./pennsylvaniaOutsideGroupContributionAggregator.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type PennsylvaniaCandidateFinanceDueRow = {
  linkId: string;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  filerId: string;
  filerName: string;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export type PennsylvaniaCampaignFinanceDataForYear = {
  year: number;
  extractedDir: string;
  sourceUrl: string;
  filerRows: PennsylvaniaCampaignFinanceFilerRow[];
  contributionRows: PennsylvaniaCampaignFinanceContributionRow[];
};

export type PennsylvaniaCandidateFinanceBatchSyncInput = {
  db: Queryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataExtractedDir?: string;
  rawDataCacheDir?: string;
  paDataByYear?: ReadonlyMap<number, PennsylvaniaCampaignFinanceDataForYear>;
  outsideGroupsByLinkId?: ReadonlyMap<string, readonly PennsylvaniaOutsideSpendingGroup[]>;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  syncPennsylvaniaCandidateFinanceFn?: typeof syncPennsylvaniaCandidateFinance;
};

export type PennsylvaniaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  filerId: string;
  ok: boolean;
  result?: PennsylvaniaCandidateFinanceSyncResult;
  error?: string;
};

export type PennsylvaniaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  results: PennsylvaniaCandidateFinanceBatchSyncItemResult[];
};

type PennsylvaniaCandidateFinanceDueQueryRow = {
  link_id: string;
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  filer_id: string;
  filer_name: string;
  source_url: string | null;
  last_synced_at: string | null;
  total_due_rows: string | number;
};

type PennsylvaniaCandidateFinanceOutsideGroupQueryRow = {
  link_id: string;
  group_id: string;
  group_name: string;
  support_oppose: "support" | "oppose";
  amount: string | number;
  source_url: string | null;
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid Pennsylvania finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Pennsylvania finance batch sync ${label}: ${normalized}`);
  }
  return normalized;
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
}

function parsePennsylvaniaContributionDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const compactMatch = /^(\d{4})(\d{2})(\d{2})$/.exec(trimmed);
  if (compactMatch) {
    return Number(compactMatch[1]);
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

function buildElectionCycleYearSet(rows: readonly PennsylvaniaCandidateFinanceDueRow[]): Set<number> {
  const years = new Set<number>();
  for (const row of rows) {
    years.add(row.electionYear - 1);
    years.add(row.electionYear);
  }
  return years;
}

function isAnyContributionDateInElectionCycle(
  row: PennsylvaniaCampaignFinanceContributionRow,
  electionCycleYears: ReadonlySet<number>
): boolean {
  for (const rawDate of [row.CONTDATE1, row.CONTDATE2, row.CONTDATE3]) {
    const year = parsePennsylvaniaContributionDateYear(rawDate);
    if (year !== null && electionCycleYears.has(year)) {
      return true;
    }
  }
  return false;
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function mapDueRow(row: PennsylvaniaCandidateFinanceDueQueryRow): PennsylvaniaCandidateFinanceDueRow {
  return {
    linkId: row.link_id,
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
    filerId: row.filer_id,
    filerName: row.filer_name,
    sourceUrl: row.source_url,
    lastSyncedAt: row.last_synced_at,
  };
}

function parseAmount(value: string | number): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function groupOutsideRowsByLinkId(
  rows: readonly PennsylvaniaCandidateFinanceOutsideGroupQueryRow[]
): Map<string, PennsylvaniaOutsideSpendingGroup[]> {
  const result = new Map<string, PennsylvaniaOutsideSpendingGroup[]>();
  for (const row of rows) {
    const groupId = normalizeId(row.group_id);
    const groupName = row.group_name.trim();
    if (!row.link_id || !groupId || !groupName) {
      continue;
    }
    const groups = result.get(row.link_id) ?? [];
    groups.push({
      groupId,
      groupName,
      supportOppose: row.support_oppose,
      amount: parseAmount(row.amount),
      sourceUrl: row.source_url,
    });
    result.set(row.link_id, groups);
  }
  return result;
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

function groupDueRowsByYear(rows: readonly PennsylvaniaCandidateFinanceDueRow[]): Map<number, PennsylvaniaCandidateFinanceDueRow[]> {
  const byYear = new Map<number, PennsylvaniaCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function defaultExtractedDir(input: { cacheDir: string; year: number }): string {
  return getPennsylvaniaCampaignFinanceExportCachePaths({ cacheDir: input.cacheDir, year: input.year }).extractedDir;
}

async function sourceUrlForYear(input: { year: number; rawDataCacheDir?: string }): Promise<string> {
  const cacheDir =
    input.rawDataCacheDir ??
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR?.trim() ??
    DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR;
  const metadata = await readPennsylvaniaCampaignFinanceExportCacheMetadata(
    getPennsylvaniaCampaignFinanceExportCachePaths({ cacheDir, year: input.year }).metadataPath
  );
  return metadata?.remote.url ?? buildPennsylvaniaCampaignFinanceExportUrl({ year: input.year });
}

async function loadPennsylvaniaCampaignFinanceDataForYear(input: {
  year: number;
  dueRows: readonly PennsylvaniaCandidateFinanceDueRow[];
  outsideGroupsByLinkId: ReadonlyMap<string, readonly PennsylvaniaOutsideSpendingGroup[]>;
  rawDataExtractedDir?: string;
  rawDataCacheDir?: string;
}): Promise<PennsylvaniaCampaignFinanceDataForYear> {
  const cacheDir =
    input.rawDataCacheDir ??
    process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR?.trim() ??
    DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR;
  const extractedDir = resolve(input.rawDataExtractedDir ?? defaultExtractedDir({ cacheDir, year: input.year }));
  if (!(await directoryExists(extractedDir))) {
    throw new Error(`Pennsylvania campaign finance extracted CSV directory not found for ${input.year}: ${extractedDir}`);
  }

  const candidateFilerIds = new Set(input.dueRows.map((row) => normalizeId(row.filerId)).filter(Boolean));
  const filerRows = await readPennsylvaniaCampaignFinanceFilerRows({
    extractedDir,
    year: input.year,
  });
  const resolvedOutsideGroups = new Map<string, readonly PennsylvaniaOutsideSpendingGroup[]>();
  for (const dueRow of input.dueRows) {
    resolvedOutsideGroups.set(
      dueRow.linkId,
      resolvePennsylvaniaOutsideGroupsForContributionAggregation({
        outsideGroups: input.outsideGroupsByLinkId.get(dueRow.linkId) ?? [],
        filerRows,
      })
    );
  }
  const contributionFilerIds = new Set(candidateFilerIds);
  for (const groups of resolvedOutsideGroups.values()) {
    for (const group of groups) {
      const contributionFilerId = normalizeId(group.contributionFilerId ?? "");
      if (contributionFilerId) {
        contributionFilerIds.add(contributionFilerId);
      }
    }
  }
  const electionCycleYears = buildElectionCycleYearSet(input.dueRows);
  const sourceUrl = await sourceUrlForYear({ year: input.year, rawDataCacheDir: input.rawDataCacheDir });
  const contributionRows = await readPennsylvaniaCampaignFinanceContributionRows({
    extractedDir,
    year: input.year,
    predicate: (row) =>
      contributionFilerIds.has(normalizeId(row.FilerID)) &&
      isAnyContributionDateInElectionCycle(row as PennsylvaniaCampaignFinanceContributionRow, electionCycleYears),
  });

  return {
    year: input.year,
    extractedDir,
    sourceUrl,
    filerRows,
    contributionRows,
  };
}

export async function listDuePennsylvaniaCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: PennsylvaniaCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<PennsylvaniaCandidateFinanceDueQueryRow>(
    `
      WITH due AS (
        SELECT
          link.id::text AS link_id,
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
          link.filer_id,
          link.filer_name,
          link.source_url,
          summary.last_synced_at::text AS last_synced_at,
          COUNT(*) OVER () AS total_due_rows
        FROM public.pa_candidate_finance_links AS link
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
        LEFT JOIN public.pa_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'PA'
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
        link_id,
        election_id,
        candidate_name,
        election_year,
        office_scope,
        office_name,
        district,
        filer_id,
        filer_name,
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
      [...PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function listPennsylvaniaCandidateFinanceOutsideGroupsForLinks(
  db: Queryable,
  linkIds: readonly string[]
): Promise<Map<string, PennsylvaniaOutsideSpendingGroup[]>> {
  const ids = [...new Set(linkIds.map((id) => id.trim()).filter(Boolean))];
  if (ids.length === 0) {
    return new Map();
  }
  const result = await db.query<PennsylvaniaCandidateFinanceOutsideGroupQueryRow>(
    `
      SELECT
        link_id::text,
        group_id,
        group_name,
        support_oppose,
        amount::text AS amount,
        source_url
      FROM public.pa_candidate_finance_outside_groups
      WHERE link_id = ANY($1::uuid[])
      ORDER BY link_id ASC,
               amount DESC,
               group_name ASC,
               group_id ASC
    `,
    [ids]
  );
  return groupOutsideRowsByLinkId(result.rows);
}

export async function syncDuePennsylvaniaCandidateFinance(
  input: PennsylvaniaCandidateFinanceBatchSyncInput
): Promise<PennsylvaniaCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncPennsylvaniaCandidateFinanceFn ?? syncPennsylvaniaCandidateFinance;

  const due = await listDuePennsylvaniaCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });
  const outsideGroupsByLinkId = new Map<string, readonly PennsylvaniaOutsideSpendingGroup[]>(
    input.outsideGroupsByLinkId ? [...input.outsideGroupsByLinkId.entries()] : []
  );
  if (!input.outsideGroupsByLinkId && due.rows.length > 0) {
    for (const [linkId, groups] of await listPennsylvaniaCandidateFinanceOutsideGroupsForLinks(
      input.db,
      due.rows.map((row) => row.linkId)
    )) {
      outsideGroupsByLinkId.set(linkId, groups);
    }
  }
  const paDataByYear = new Map<number, PennsylvaniaCampaignFinanceDataForYear>(
    input.paDataByYear ? [...input.paDataByYear.entries()] : []
  );
  const paLoadErrorsByYear = new Map<number, string>();
  for (const [year, rows] of groupDueRowsByYear(due.rows).entries()) {
    if (!paDataByYear.has(year)) {
      try {
        paDataByYear.set(
          year,
          await loadPennsylvaniaCampaignFinanceDataForYear({
            year,
            dueRows: rows,
            outsideGroupsByLinkId,
            rawDataExtractedDir: input.rawDataExtractedDir,
            rawDataCacheDir: input.rawDataCacheDir,
          })
        );
      } catch (error) {
        paLoadErrorsByYear.set(year, error instanceof Error ? error.message : String(error));
      }
    }
  }

  const results: PennsylvaniaCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    const paLoadError = paLoadErrorsByYear.get(row.electionYear);
    if (paLoadError) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        filerId: row.filerId,
        ok: false,
        error: `Pennsylvania campaign finance data load failed for ${row.electionYear}: ${paLoadError}`,
      });
      continue;
    }
    const paData = paDataByYear.get(row.electionYear);
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
        sourceUrl: row.sourceUrl ?? paData?.sourceUrl ?? null,
        contributionSourceUrl: paData?.sourceUrl,
        filerRows: paData?.filerRows ?? [],
        contributionRows: paData?.contributionRows ?? [],
        outsideGroups: outsideGroupsByLinkId.get(row.linkId) ?? [],
        trustedFiler: {
          filerId: row.filerId,
          filerName: row.filerName,
          sourceUrl: row.sourceUrl ?? paData?.sourceUrl ?? null,
        },
        dryRun,
        now,
        financeIndustryClassifier: input.financeIndustryClassifier,
        aiClassificationMinAmount: input.aiClassificationMinAmount,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        filerId: row.filerId,
        ok: true,
        result,
      });
    } catch (error) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        filerId: row.filerId,
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
