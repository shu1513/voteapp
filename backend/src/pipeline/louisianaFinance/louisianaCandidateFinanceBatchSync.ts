import { stat } from "node:fs/promises";
import type { Pool, PoolClient } from "pg";

import {
  autoLinkMissingLouisianaCandidateFinanceLinks,
  buildLouisianaCandidateNamePredicate,
  listLouisianaCandidateElectionsMissingFinanceLinks,
  type LouisianaFinanceAutoLinkCandidateElection,
} from "./louisianaCandidateFinanceAutoLink.js";
import {
  DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_CACHE_DIR,
  getLouisianaCampaignFinanceArtifactCachePaths,
  readLouisianaCampaignFinanceArtifactCacheMetadata,
  refreshLouisianaCampaignFinanceArtifactCache,
} from "./louisianaCampaignFinanceArtifactCache.js";
import {
  readLouisianaCampaignFinanceContributionRows,
  readLouisianaCampaignFinanceExpenditureRows,
  type LouisianaCampaignFinanceCsvRow,
} from "./louisianaCampaignFinanceArtifactReader.js";
import { normalizeLouisianaCandidateNameKeys } from "./louisianaCandidateCommitteeResolver.js";
import { LOUISIANA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./louisianaFinanceEligibleOffices.js";
import {
  syncLouisianaCandidateFinance,
  type LouisianaCandidateFinanceSyncResult,
} from "./louisianaCandidateFinanceSync.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type LouisianaCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  filerNumber: string;
  filerName: string;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export type LouisianaCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
  autoLinkMissingLinks?: boolean;
  contributionRows?: readonly LouisianaCampaignFinanceCsvRow[];
  expenditureRows?: readonly LouisianaCampaignFinanceCsvRow[];
  contributionSourceUrl?: string | null;
  expenditureSourceUrl?: string | null;
  refreshArtifactCacheFn?: typeof refreshLouisianaCampaignFinanceArtifactCache;
  syncLouisianaCandidateFinanceFn?: typeof syncLouisianaCandidateFinance;
};

export type LouisianaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  filerNumber: string;
  ok: boolean;
  result?: LouisianaCandidateFinanceSyncResult;
  error?: string;
};

export type LouisianaCandidateFinanceBatchSyncResult = {
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
  results: LouisianaCandidateFinanceBatchSyncItemResult[];
};

type LouisianaCandidateFinanceDueQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  filer_number: string;
  filer_name: string;
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
    throw new Error(`Invalid Louisiana finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Louisiana finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(THE|OF|FOR|TO|ELECT|COMMITTEE|FRIENDS|CAMPAIGN)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeFilerNumber(value: string): string {
  return value.trim().replace(/\s+/g, "");
}

function firstNonEmpty(row: LouisianaCampaignFinanceCsvRow, keys: readonly string[]): string {
  for (const key of keys) {
    const value = row[key]?.trim() ?? "";
    if (value) {
      return value;
    }
  }
  return "";
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function mapDueRow(row: LouisianaCandidateFinanceDueQueryRow): LouisianaCandidateFinanceDueRow {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
    filerNumber: row.filer_number,
    filerName: row.filer_name,
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

function contributionSourceUrlFromMetadata(
  metadata: Awaited<ReturnType<typeof readLouisianaCampaignFinanceArtifactCacheMetadata>>,
  fallback: string
): string {
  return metadata?.downloads.contributions.remote.url ?? fallback;
}

function expenditureSourceUrlFromMetadata(
  metadata: Awaited<ReturnType<typeof readLouisianaCampaignFinanceArtifactCacheMetadata>>,
  fallback: string
): string {
  return metadata?.downloads.expenditures.remote.url ?? fallback;
}

function getCachePaths(rawDataCacheDir?: string) {
  return getLouisianaCampaignFinanceArtifactCachePaths(
    rawDataCacheDir ??
      (process.env.LOUISIANA_CAMPAIGN_FINANCE_CACHE_DIR?.trim() || DEFAULT_LOUISIANA_CAMPAIGN_FINANCE_CACHE_DIR)
  );
}

async function refreshLouisianaRawDataIfNeeded(input: {
  rawDataCacheDir?: string;
  contributionRows?: readonly LouisianaCampaignFinanceCsvRow[];
  expenditureRows?: readonly LouisianaCampaignFinanceCsvRow[];
  refreshArtifactCacheFn?: typeof refreshLouisianaCampaignFinanceArtifactCache;
  now: Date;
}): Promise<void> {
  if (input.contributionRows !== undefined && input.expenditureRows !== undefined) {
    return;
  }

  const paths = getCachePaths(input.rawDataCacheDir);
  await (input.refreshArtifactCacheFn ?? refreshLouisianaCampaignFinanceArtifactCache)({
    cacheDir: paths.cacheDir,
    now: input.now,
  });
}

async function loadLouisianaContributionRowsForCandidates(input: {
  candidates: readonly LouisianaFinanceAutoLinkCandidateElection[];
  rawDataCacheDir?: string;
}): Promise<{ rows: readonly LouisianaCampaignFinanceCsvRow[]; sourceUrl: string }> {
  const paths = getCachePaths(input.rawDataCacheDir);
  if (!(await fileExists(paths.downloads.contributions))) {
    throw new Error(`Louisiana campaign finance contribution artifact not found: ${paths.downloads.contributions}`);
  }
  const metadata = await readLouisianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  return {
    rows: await readLouisianaCampaignFinanceContributionRows({
      filePath: paths.downloads.contributions,
      predicate: buildLouisianaCandidateNamePredicate(input.candidates),
    }),
    sourceUrl: contributionSourceUrlFromMetadata(metadata, paths.downloads.contributions),
  };
}

async function loadLouisianaContributionRowsForFilerNumbers(input: {
  filerNumbers: readonly string[];
  rawDataCacheDir?: string;
}): Promise<{ rows: readonly LouisianaCampaignFinanceCsvRow[]; sourceUrl: string }> {
  const normalizedFilerNumbers = new Set(input.filerNumbers.map(normalizeFilerNumber).filter(Boolean));
  const paths = getCachePaths(input.rawDataCacheDir);
  if (!(await fileExists(paths.downloads.contributions))) {
    throw new Error(`Louisiana campaign finance contribution artifact not found: ${paths.downloads.contributions}`);
  }
  const metadata = await readLouisianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  return {
    rows: await readLouisianaCampaignFinanceContributionRows({
      filePath: paths.downloads.contributions,
      predicate: (row) => normalizedFilerNumbers.has(normalizeFilerNumber(firstNonEmpty(row, ["FilerNumber", "Filer Number"]))),
    }),
    sourceUrl: contributionSourceUrlFromMetadata(metadata, paths.downloads.contributions),
  };
}

function rowMatchesCandidateTarget(row: LouisianaCampaignFinanceCsvRow, targets: readonly LouisianaCandidateFinanceDueRow[]): boolean {
  const rowNames = [
    firstNonEmpty(row, ["CandidateBeneficiary", "Candidate Beneficiary"]),
    firstNonEmpty(row, ["RecipientName", "Recipient Name"]),
  ].filter(Boolean);
  if (rowNames.length === 0) {
    return false;
  }

  for (const target of targets) {
    const nameKeys = normalizeLouisianaCandidateNameKeys(target.candidateName);
    const aliasKeys = new Set([target.candidateName, target.filerName].map(normalizeTextKey).filter(Boolean));
    for (const rowName of rowNames) {
      for (const rowKey of normalizeLouisianaCandidateNameKeys(rowName)) {
        if (nameKeys.has(rowKey)) {
          return true;
        }
      }
      if (aliasKeys.has(normalizeTextKey(rowName))) {
        return true;
      }
    }
  }

  return false;
}

async function loadLouisianaExpenditureRowsForTargets(input: {
  targets: readonly LouisianaCandidateFinanceDueRow[];
  rawDataCacheDir?: string;
}): Promise<{ rows: readonly LouisianaCampaignFinanceCsvRow[]; sourceUrl: string }> {
  const paths = getCachePaths(input.rawDataCacheDir);
  if (!(await fileExists(paths.downloads.expenditures))) {
    throw new Error(`Louisiana campaign finance expenditure artifact not found: ${paths.downloads.expenditures}`);
  }
  const metadata = await readLouisianaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  return {
    rows: await readLouisianaCampaignFinanceExpenditureRows({
      filePath: paths.downloads.expenditures,
      predicate: (row) => rowMatchesCandidateTarget(row, input.targets),
    }),
    sourceUrl: expenditureSourceUrlFromMetadata(metadata, paths.downloads.expenditures),
  };
}

function collectFilerNumbers(rows: readonly LouisianaCandidateFinanceDueRow[]): string[] {
  const filerNumbers = new Set<string>();
  for (const row of rows) {
    const filerNumber = normalizeFilerNumber(row.filerNumber);
    if (filerNumber) {
      filerNumbers.add(filerNumber);
    }
  }
  return [...filerNumbers];
}

function collectExpenditureSpenderFilerNumbers(rows: readonly LouisianaCampaignFinanceCsvRow[]): string[] {
  const filerNumbers = new Set<string>();
  for (const row of rows) {
    const filerNumber = normalizeFilerNumber(firstNonEmpty(row, ["FilerNumber", "Filer Number"]));
    if (filerNumber) {
      filerNumbers.add(filerNumber);
    }
  }
  return [...filerNumbers];
}

export async function listDueLouisianaCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: LouisianaCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<LouisianaCandidateFinanceDueQueryRow>(
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
          link.filer_number,
          link.filer_name,
          link.source_url,
          summary.last_synced_at::text AS last_synced_at,
          COUNT(*) OVER () AS total_due_rows
        FROM public.la_candidate_finance_links AS link
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
        LEFT JOIN public.la_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'LA'
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
        filer_number,
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
      [...LOUISIANA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueLouisianaCandidateFinance(
  input: LouisianaCandidateFinanceBatchSyncInput
): Promise<LouisianaCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncLouisianaCandidateFinanceFn ?? syncLouisianaCandidateFinance;
  let autoLinkAttemptedCount = 0;
  let autoLinkLinkedCount = 0;

  let contributionSourceUrl = input.contributionSourceUrl ?? null;
  await refreshLouisianaRawDataIfNeeded({
    rawDataCacheDir: input.rawDataCacheDir,
    contributionRows: input.contributionRows,
    expenditureRows: input.expenditureRows,
    refreshArtifactCacheFn: input.refreshArtifactCacheFn,
    now,
  });

  if (!dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const missingLinkCandidates = await listLouisianaCandidateElectionsMissingFinanceLinks(input.db, {
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
      });
      autoLinkAttemptedCount = missingLinkCandidates.length;

      if (missingLinkCandidates.length > 0) {
        const loaded =
          input.contributionRows === undefined
            ? await loadLouisianaContributionRowsForCandidates({
                candidates: missingLinkCandidates,
                rawDataCacheDir: input.rawDataCacheDir,
              })
            : { rows: input.contributionRows, sourceUrl: contributionSourceUrl ?? null };
        contributionSourceUrl = loaded.sourceUrl;

        const autoLinkResults = await autoLinkMissingLouisianaCandidateFinanceLinks({
          db: input.db,
          now,
          maxCandidates,
          electionLookbackDays,
          electionLookaheadDays,
          candidateElections: missingLinkCandidates,
          contributionRows: loaded.rows,
          sourceUrl: contributionSourceUrl,
        });
        autoLinkLinkedCount = autoLinkResults.filter((result) => result.status === "linked").length;
        for (const result of autoLinkResults) {
          if (result.status !== "linked") {
            console.warn("Louisiana finance auto-link did not link candidate election:", result);
          }
        }
      }
    } catch (error) {
      console.warn(
        "Louisiana finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueLouisianaCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  let expenditureRowsForBatch = input.expenditureRows;
  let expenditureSourceUrl = input.expenditureSourceUrl ?? null;
  if (expenditureRowsForBatch === undefined) {
    try {
      if (due.rows.length > 0) {
        const loaded = await loadLouisianaExpenditureRowsForTargets({
          targets: due.rows,
          rawDataCacheDir: input.rawDataCacheDir,
        });
        expenditureRowsForBatch = loaded.rows;
        expenditureSourceUrl = loaded.sourceUrl;
      } else {
        expenditureRowsForBatch = [];
      }
    } catch (error) {
      console.warn(
        "Louisiana campaign finance expenditure artifact unavailable; syncing candidate links without outside support:",
        error instanceof Error ? error.message : error
      );
      expenditureRowsForBatch = undefined;
    }
  }

  let contributionRowsForBatch = input.contributionRows;
  if (contributionRowsForBatch === undefined) {
    const filerNumbers = [
      ...collectFilerNumbers(due.rows),
      ...collectExpenditureSpenderFilerNumbers(expenditureRowsForBatch ?? []),
    ];
    if (filerNumbers.length > 0) {
      const loaded = await loadLouisianaContributionRowsForFilerNumbers({
        filerNumbers,
        rawDataCacheDir: input.rawDataCacheDir,
      });
      contributionRowsForBatch = loaded.rows;
      contributionSourceUrl = loaded.sourceUrl;
    } else {
      contributionRowsForBatch = [];
    }
  }

  const results: LouisianaCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
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
        contributionRows: contributionRowsForBatch,
        contributionSourceUrl,
        expenditureRows: expenditureRowsForBatch,
        expenditureSourceUrl,
        trustedCommittee: {
          filerNumber: row.filerNumber,
          filerName: row.filerName,
          sourceUrl: row.sourceUrl,
        },
        dryRun,
        now,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        filerNumber: row.filerNumber,
        ok: true,
        result,
      });
    } catch (error) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        filerNumber: row.filerNumber,
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
