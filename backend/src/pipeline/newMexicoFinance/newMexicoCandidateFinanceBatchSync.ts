import { stat } from "node:fs/promises";
import type { Pool, PoolClient } from "pg";

import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  autoLinkMissingNewMexicoCandidateFinanceLinks,
  buildNewMexicoCandidateNamePredicate,
  listNewMexicoCandidateElectionsMissingFinanceLinks,
  type NewMexicoFinanceAutoLinkCandidateElection,
} from "./newMexicoCandidateFinanceAutoLink.js";
import {
  syncNewMexicoCandidateFinance,
  type NewMexicoCandidateFinanceSyncResult,
} from "./newMexicoCandidateFinanceSync.js";
import { NEW_MEXICO_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./newMexicoFinanceEligibleOffices.js";
import {
  DEFAULT_NEW_MEXICO_CFIS_CACHE_DIR,
  buildNewMexicoCfisArtifactUrl,
  getNewMexicoCfisArtifactCachePaths,
  readNewMexicoCfisArtifactCacheMetadata,
} from "./newMexicoCfisArtifactCache.js";
import {
  readNewMexicoCfisContributionRows,
  readNewMexicoCfisExpenditureRows,
  type NewMexicoCfisContributionRow,
  type NewMexicoCfisExpenditureRow,
} from "./newMexicoCfisArtifactReader.js";
import { normalizeNewMexicoCandidateNameKeys } from "./newMexicoCandidateCommitteeResolver.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type NewMexicoCandidateFinanceDueRow = {
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

export type NewMexicoContributionDataForYear = {
  year: number;
  filePath: string;
  sourceUrl: string;
  rowsByCommitteeId: Map<string, NewMexicoCfisContributionRow[]>;
};

export type NewMexicoExpenditureDataForYear = {
  year: number;
  filePath: string;
  sourceUrl: string;
  rows: NewMexicoCfisExpenditureRow[];
};

export type NewMexicoCandidateFinanceBatchSyncInput = {
  db: Queryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
  autoLinkMissingLinks?: boolean;
  contributionDataByYear?: ReadonlyMap<number, NewMexicoContributionDataForYear>;
  expenditureDataByYear?: ReadonlyMap<number, NewMexicoExpenditureDataForYear>;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  syncNewMexicoCandidateFinanceFn?: typeof syncNewMexicoCandidateFinance;
};

export type NewMexicoCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: NewMexicoCandidateFinanceSyncResult;
  error?: string;
};

export type NewMexicoCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  results: NewMexicoCandidateFinanceBatchSyncItemResult[];
};

type NewMexicoCandidateFinanceDueQueryRow = {
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
    throw new Error(`Invalid New Mexico finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Mexico finance batch sync ${label}: ${value}`);
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

function mapDueRow(row: NewMexicoCandidateFinanceDueQueryRow): NewMexicoCandidateFinanceDueRow {
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

function groupDueRowsByYear(rows: readonly NewMexicoCandidateFinanceDueRow[]): Map<number, NewMexicoCandidateFinanceDueRow[]> {
  const byYear = new Map<number, NewMexicoCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function groupAutoLinkCandidatesByYear(
  rows: readonly NewMexicoFinanceAutoLinkCandidateElection[]
): Map<number, NewMexicoFinanceAutoLinkCandidateElection[]> {
  const byYear = new Map<number, NewMexicoFinanceAutoLinkCandidateElection[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

function groupContributionRowsByCommittee(
  rows: readonly NewMexicoCfisContributionRow[]
): Map<string, NewMexicoCfisContributionRow[]> {
  const byCommittee = new Map<string, NewMexicoCfisContributionRow[]>();
  for (const row of rows) {
    const committeeId = normalizeCommitteeId(row.OrgID);
    if (!committeeId) {
      continue;
    }
    const existing = byCommittee.get(committeeId) ?? [];
    existing.push(row);
    byCommittee.set(committeeId, existing);
  }
  return byCommittee;
}

function buildNewMexicoExpenditureCandidatePredicate(
  rows: readonly NewMexicoCandidateFinanceDueRow[]
): (row: NewMexicoCfisExpenditureRow) => boolean {
  const candidateNameKeys = new Set<string>();
  for (const candidate of rows) {
    for (const key of normalizeNewMexicoCandidateNameKeys(candidate.candidateName)) {
      candidateNameKeys.add(key);
    }
  }

  return (row) => {
    for (const rowKey of normalizeNewMexicoCandidateNameKeys(row.Reason)) {
      if (candidateNameKeys.has(rowKey)) {
        return true;
      }
    }
    return false;
  };
}

function collectCommitteeIdsForContributionLoad(input: {
  dueRows: readonly NewMexicoCandidateFinanceDueRow[];
  expenditureRows?: readonly NewMexicoCfisExpenditureRow[];
}): string[] {
  const committeeIds = new Set<string>();
  for (const row of input.dueRows) {
    const committeeId = normalizeCommitteeId(row.committeeId);
    if (committeeId) {
      committeeIds.add(committeeId);
    }
  }
  for (const row of input.expenditureRows ?? []) {
    const committeeId = normalizeCommitteeId(row.OrgID);
    if (committeeId) {
      committeeIds.add(committeeId);
    }
  }
  return [...committeeIds];
}

function flattenContributionDataRows(
  contributionData: NewMexicoContributionDataForYear | undefined
): NewMexicoCfisContributionRow[] {
  return contributionData ? [...contributionData.rowsByCommitteeId.values()].flat() : [];
}

async function loadContributionDataForYear(input: {
  year: number;
  committeeIds: readonly string[];
  rawDataCacheDir?: string;
}): Promise<NewMexicoContributionDataForYear> {
  const normalizedCommitteeIds = new Set(input.committeeIds.map(normalizeCommitteeId).filter(Boolean));
  const paths = getNewMexicoCfisArtifactCachePaths({
    cacheDir:
      input.rawDataCacheDir ??
      process.env.NEW_MEXICO_CFIS_CACHE_DIR?.trim() ??
      DEFAULT_NEW_MEXICO_CFIS_CACHE_DIR,
    year: input.year,
    artifactKind: "contributions",
  });
  if (!(await fileExists(paths.filePath))) {
    throw new Error(`New Mexico CFIS contribution artifact not found for ${input.year}: ${paths.filePath}`);
  }

  const metadata = await readNewMexicoCfisArtifactCacheMetadata(paths.metadataPath);
  const rows = await readNewMexicoCfisContributionRows({
    filePath: paths.filePath,
    predicate: (row) => normalizedCommitteeIds.has(normalizeCommitteeId(row.OrgID)),
  });

  return {
    year: input.year,
    filePath: paths.filePath,
    sourceUrl: metadata?.remote.url ?? buildNewMexicoCfisArtifactUrl({ year: input.year, artifactKind: "contributions" }),
    rowsByCommitteeId: groupContributionRowsByCommittee(rows),
  };
}

async function loadAutoLinkContributionRowsForYear(input: {
  year: number;
  candidates: readonly NewMexicoFinanceAutoLinkCandidateElection[];
  rawDataCacheDir?: string;
  contributionDataByYear?: ReadonlyMap<number, NewMexicoContributionDataForYear>;
}): Promise<{ rows: NewMexicoCfisContributionRow[]; sourceUrl: string }> {
  const injected = input.contributionDataByYear?.get(input.year);
  if (injected) {
    return {
      rows: [...injected.rowsByCommitteeId.values()].flat(),
      sourceUrl: injected.sourceUrl,
    };
  }

  const paths = getNewMexicoCfisArtifactCachePaths({
    cacheDir:
      input.rawDataCacheDir ??
      process.env.NEW_MEXICO_CFIS_CACHE_DIR?.trim() ??
      DEFAULT_NEW_MEXICO_CFIS_CACHE_DIR,
    year: input.year,
    artifactKind: "contributions",
  });
  if (!(await fileExists(paths.filePath))) {
    throw new Error(`New Mexico CFIS contribution artifact not found for ${input.year}: ${paths.filePath}`);
  }

  const metadata = await readNewMexicoCfisArtifactCacheMetadata(paths.metadataPath);
  return {
    rows: await readNewMexicoCfisContributionRows({
      filePath: paths.filePath,
      predicate: buildNewMexicoCandidateNamePredicate(input.candidates),
    }),
    sourceUrl: metadata?.remote.url ?? buildNewMexicoCfisArtifactUrl({ year: input.year, artifactKind: "contributions" }),
  };
}

async function loadExpenditureDataForYear(input: {
  year: number;
  dueRows: readonly NewMexicoCandidateFinanceDueRow[];
  rawDataCacheDir?: string;
}): Promise<NewMexicoExpenditureDataForYear> {
  const paths = getNewMexicoCfisArtifactCachePaths({
    cacheDir:
      input.rawDataCacheDir ??
      process.env.NEW_MEXICO_CFIS_CACHE_DIR?.trim() ??
      DEFAULT_NEW_MEXICO_CFIS_CACHE_DIR,
    year: input.year,
    artifactKind: "expenditures",
  });
  if (!(await fileExists(paths.filePath))) {
    throw new Error(`New Mexico CFIS expenditure artifact not found for ${input.year}: ${paths.filePath}`);
  }

  const metadata = await readNewMexicoCfisArtifactCacheMetadata(paths.metadataPath);
  return {
    year: input.year,
    filePath: paths.filePath,
    sourceUrl: metadata?.remote.url ?? buildNewMexicoCfisArtifactUrl({ year: input.year, artifactKind: "expenditures" }),
    rows: await readNewMexicoCfisExpenditureRows({
      filePath: paths.filePath,
      predicate: buildNewMexicoExpenditureCandidatePredicate(input.dueRows),
    }),
  };
}

export async function listDueNewMexicoCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: NewMexicoCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<NewMexicoCandidateFinanceDueQueryRow>(
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
        FROM public.nm_candidate_finance_links AS link
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
        LEFT JOIN public.nm_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'NM'
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
      [...NEW_MEXICO_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueNewMexicoCandidateFinance(
  input: NewMexicoCandidateFinanceBatchSyncInput
): Promise<NewMexicoCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncNewMexicoCandidateFinanceFn ?? syncNewMexicoCandidateFinance;

  if (!dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const missingLinkCandidates = await listNewMexicoCandidateElectionsMissingFinanceLinks(input.db, {
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
      });
      const contributionRowsByYear = new Map<number, readonly NewMexicoCfisContributionRow[]>();
      const sourceUrlByYear = new Map<number, string>();
      const skippedAutoLinkYears = new Map<number, string>();
      for (const [year, candidates] of groupAutoLinkCandidatesByYear(missingLinkCandidates).entries()) {
        try {
          const data = await loadAutoLinkContributionRowsForYear({
            year,
            candidates,
            rawDataCacheDir: input.rawDataCacheDir,
            contributionDataByYear: input.contributionDataByYear,
          });
          contributionRowsByYear.set(year, data.rows);
          sourceUrlByYear.set(year, data.sourceUrl);
        } catch (error) {
          skippedAutoLinkYears.set(year, error instanceof Error ? error.message : String(error));
        }
      }
      const autoLinkCandidates = missingLinkCandidates.filter((candidate) =>
        contributionRowsByYear.has(candidate.electionYear)
      );
      const autoLinkResults = await autoLinkMissingNewMexicoCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        contributionRowsByYear,
        sourceUrlByYear,
        candidateElections: autoLinkCandidates,
      });
      for (const result of autoLinkResults) {
        if (result.status !== "linked") {
          console.warn("New Mexico finance auto-link did not link candidate election:", result);
        }
      }
      for (const [year, message] of skippedAutoLinkYears) {
        console.warn(`New Mexico finance auto-link skipped year ${year}:`, message);
      }
    } catch (error) {
      console.warn(
        "New Mexico finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueNewMexicoCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });
  const expenditureDataByYear = new Map<number, NewMexicoExpenditureDataForYear>(
    input.expenditureDataByYear ? [...input.expenditureDataByYear.entries()] : []
  );
  const expenditureDataLoadErrorsByYear = new Map<number, string>();
  for (const [year, rows] of groupDueRowsByYear(due.rows).entries()) {
    if (!expenditureDataByYear.has(year)) {
      try {
        expenditureDataByYear.set(
          year,
          await loadExpenditureDataForYear({
            year,
            dueRows: rows,
            rawDataCacheDir: input.rawDataCacheDir,
          })
        );
      } catch (error) {
        expenditureDataLoadErrorsByYear.set(year, error instanceof Error ? error.message : String(error));
        console.warn(
          "New Mexico CFIS expenditure artifact unavailable; syncing direct finance without outside spending:",
          error instanceof Error ? error.message : error
        );
      }
    }
  }

  const contributionDataByYear = new Map<number, NewMexicoContributionDataForYear>(
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
            committeeIds: collectCommitteeIdsForContributionLoad({
              dueRows: rows,
              expenditureRows: expenditureDataByYear.get(year)?.rows,
            }),
            rawDataCacheDir: input.rawDataCacheDir,
          })
        );
      } catch (error) {
        contributionDataLoadErrorsByYear.set(year, error instanceof Error ? error.message : String(error));
      }
    }
  }

  const results: NewMexicoCandidateFinanceBatchSyncItemResult[] = [];
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
    const expenditureData = expenditureDataByYear.get(row.electionYear);
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
        contributionRows: flattenContributionDataRows(contributionData),
        contributionSourceUrl: contributionData?.sourceUrl,
        expenditureRows: expenditureDataLoadErrorsByYear.has(row.electionYear) ? undefined : expenditureData?.rows ?? [],
        expenditureSourceUrl: expenditureData?.sourceUrl,
        trustedCommittee: {
          committeeId: row.committeeId,
          committeeName: row.committeeName,
          sourceUrl: row.sourceUrl,
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
