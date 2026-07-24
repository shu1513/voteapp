import { stat } from "node:fs/promises";
import type { Pool, PoolClient } from "pg";

import {
  autoLinkMissingMinnesotaCandidateFinanceLinks,
  buildMinnesotaCandidateNamePredicate,
  listMinnesotaCandidateElectionsMissingFinanceLinks,
  type MinnesotaFinanceAutoLinkCandidateElection,
} from "./minnesotaCandidateFinanceAutoLink.js";
import {
  DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR,
  getMinnesotaCampaignFinanceArtifactCachePaths,
  readMinnesotaCampaignFinanceArtifactCacheMetadata,
} from "./minnesotaCampaignFinanceArtifactCache.js";
import {
  readMinnesotaCampaignFinanceContributionRows,
  readMinnesotaCampaignFinanceIndependentExpenditureContributionRows,
  readMinnesotaCampaignFinanceIndependentExpenditureRows,
  type MinnesotaCampaignFinanceCsvRow,
} from "./minnesotaCampaignFinanceArtifactReader.js";
import { MINNESOTA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./minnesotaFinanceEligibleOffices.js";
import {
  syncMinnesotaCandidateFinance,
  type MinnesotaCandidateFinanceSyncResult,
} from "./minnesotaCandidateFinanceSync.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type MinnesotaCandidateFinanceDueRow = {
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

export type MinnesotaCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
  autoLinkMissingLinks?: boolean;
  contributionRows?: readonly MinnesotaCampaignFinanceCsvRow[];
  expenditureRows?: readonly MinnesotaCampaignFinanceCsvRow[];
  outsideContributionRows?: readonly MinnesotaCampaignFinanceCsvRow[];
  contributionSourceUrl?: string | null;
  expenditureSourceUrl?: string | null;
  outsideSourceUrl?: string | null;
  syncMinnesotaCandidateFinanceFn?: typeof syncMinnesotaCandidateFinance;
};

export type MinnesotaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: MinnesotaCandidateFinanceSyncResult;
  error?: string;
};

export type MinnesotaCandidateFinanceBatchSyncResult = {
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
  results: MinnesotaCandidateFinanceBatchSyncItemResult[];
};

type MinnesotaCandidateFinanceDueQueryRow = {
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
    throw new Error(`Invalid Minnesota finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Minnesota finance batch sync ${label}: ${value}`);
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

function mapDueRow(row: MinnesotaCandidateFinanceDueQueryRow): MinnesotaCandidateFinanceDueRow {
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

function groupDueRowsByElectionYear(rows: readonly MinnesotaCandidateFinanceDueRow[]): Map<number, MinnesotaCandidateFinanceDueRow[]> {
  const byYear = new Map<number, MinnesotaCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const existing = byYear.get(row.electionYear) ?? [];
    existing.push(row);
    byYear.set(row.electionYear, existing);
  }
  return byYear;
}

function collectCommitteeIds(rows: readonly MinnesotaCandidateFinanceDueRow[]): string[] {
  const committeeIds = new Set<string>();
  for (const row of rows) {
    const committeeId = normalizeCommitteeId(row.committeeId);
    if (committeeId) {
      committeeIds.add(committeeId);
    }
  }
  return [...committeeIds];
}

function collectOutsideSpenderCommitteeIds(rows: readonly MinnesotaCampaignFinanceCsvRow[]): string[] {
  const committeeIds = new Set<string>();
  for (const row of rows) {
    const committeeId = normalizeCommitteeId(
      row["Spender Reg Num"] ?? row["Spender reg num"] ?? row["Spender ID"] ?? ""
    );
    if (committeeId) {
      committeeIds.add(committeeId);
    }
  }
  return [...committeeIds];
}

function contributionSourceUrlFromMetadata(
  metadata: Awaited<ReturnType<typeof readMinnesotaCampaignFinanceArtifactCacheMetadata>>,
  fallback: string
): string {
  return metadata?.downloads.contributions_received.remote.url ?? fallback;
}

function expenditureSourceUrlFromMetadata(
  metadata: Awaited<ReturnType<typeof readMinnesotaCampaignFinanceArtifactCacheMetadata>>,
  fallback: string
): string {
  return metadata?.downloads.independent_expenditures.remote.url ?? fallback;
}

function outsideContributionSourceUrlFromMetadata(
  metadata: Awaited<ReturnType<typeof readMinnesotaCampaignFinanceArtifactCacheMetadata>>,
  fallback: string
): string {
  return metadata?.downloads.independent_expenditure_contributions.remote.url ?? fallback;
}

async function loadMinnesotaContributionRowsForCandidates(input: {
  candidates: readonly MinnesotaFinanceAutoLinkCandidateElection[];
  rawDataCacheDir?: string;
}): Promise<{ rows: readonly MinnesotaCampaignFinanceCsvRow[]; sourceUrl: string }> {
  const paths = getMinnesotaCampaignFinanceArtifactCachePaths(
    input.rawDataCacheDir ??
      (process.env.MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR?.trim() || DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR)
  );
  if (!(await fileExists(paths.downloads.contributions_received))) {
    throw new Error(`Minnesota campaign finance contribution artifact not found: ${paths.downloads.contributions_received}`);
  }
  const metadata = await readMinnesotaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  return {
    rows: await readMinnesotaCampaignFinanceContributionRows({
      filePath: paths.downloads.contributions_received,
      predicate: buildMinnesotaCandidateNamePredicate(input.candidates),
    }),
    sourceUrl: contributionSourceUrlFromMetadata(metadata, paths.downloads.contributions_received),
  };
}

async function loadMinnesotaExpenditureRowsForCommittees(input: {
  committeeIds: readonly string[];
  rawDataCacheDir?: string;
}): Promise<{ rows: readonly MinnesotaCampaignFinanceCsvRow[]; sourceUrl: string }> {
  const normalizedCommitteeIds = new Set(input.committeeIds.map(normalizeCommitteeId).filter(Boolean));
  const paths = getMinnesotaCampaignFinanceArtifactCachePaths(
    input.rawDataCacheDir ??
      (process.env.MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR?.trim() || DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR)
  );
  if (!(await fileExists(paths.downloads.independent_expenditures))) {
    throw new Error(`Minnesota campaign finance independent expenditure artifact not found: ${paths.downloads.independent_expenditures}`);
  }
  const metadata = await readMinnesotaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  return {
    rows: await readMinnesotaCampaignFinanceIndependentExpenditureRows({
      filePath: paths.downloads.independent_expenditures,
      predicate: (row) => {
        const committeeId = normalizeCommitteeId(
          row["Affected Cmte Reg Num"] ?? row["Affected Committee Reg Num"] ?? row["Affected Cmte ID"] ?? ""
        );
        return normalizedCommitteeIds.has(committeeId);
      },
    }),
    sourceUrl: expenditureSourceUrlFromMetadata(metadata, paths.downloads.independent_expenditures),
  };
}

async function loadMinnesotaOutsideContributionRowsForCommittees(input: {
  committeeIds: readonly string[];
  rawDataCacheDir?: string;
}): Promise<{ rows: readonly MinnesotaCampaignFinanceCsvRow[]; sourceUrl: string }> {
  const normalizedCommitteeIds = new Set(input.committeeIds.map(normalizeCommitteeId).filter(Boolean));
  const paths = getMinnesotaCampaignFinanceArtifactCachePaths(
    input.rawDataCacheDir ??
      (process.env.MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR?.trim() || DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR)
  );
  if (!(await fileExists(paths.downloads.independent_expenditure_contributions))) {
    throw new Error(
      `Minnesota campaign finance IE contribution artifact not found: ${paths.downloads.independent_expenditure_contributions}`
    );
  }
  const metadata = await readMinnesotaCampaignFinanceArtifactCacheMetadata(paths.metadataPath);
  return {
    rows: await readMinnesotaCampaignFinanceIndependentExpenditureContributionRows({
      filePath: paths.downloads.independent_expenditure_contributions,
      predicate: (row) =>
        normalizedCommitteeIds.has(
          normalizeCommitteeId(row["Recipient reg num"] ?? row["Recipient Reg Num"] ?? row["Recipient ID"] ?? "")
        ),
    }),
    sourceUrl: outsideContributionSourceUrlFromMetadata(
      metadata,
      paths.downloads.independent_expenditure_contributions
    ),
  };
}

export async function listDueMinnesotaCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: MinnesotaCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<MinnesotaCandidateFinanceDueQueryRow>(
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
        FROM public.mn_candidate_finance_links AS link
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
        LEFT JOIN public.mn_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE link.link_status = 'active'
          AND candidate.deleted_at IS NULL
          AND district.state = 'MN'
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
      [...MINNESOTA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueMinnesotaCandidateFinance(
  input: MinnesotaCandidateFinanceBatchSyncInput
): Promise<MinnesotaCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncMinnesotaCandidateFinanceFn ?? syncMinnesotaCandidateFinance;
  let autoLinkAttemptedCount = 0;
  let autoLinkLinkedCount = 0;

  let contributionRowsForAutoLink = input.contributionRows;
  let contributionSourceUrl = input.contributionSourceUrl ?? null;

  if (input.contributionRows === undefined && !dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const missingLinkCandidates = await listMinnesotaCandidateElectionsMissingFinanceLinks(input.db, {
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
      });
      autoLinkAttemptedCount = missingLinkCandidates.length;
      if (missingLinkCandidates.length > 0) {
        const loaded = await loadMinnesotaContributionRowsForCandidates({
          candidates: missingLinkCandidates,
          rawDataCacheDir: input.rawDataCacheDir,
        });
        contributionRowsForAutoLink = loaded.rows;
        contributionSourceUrl = loaded.sourceUrl;
      } else {
        contributionRowsForAutoLink = [];
      }

      const autoLinkResults = await autoLinkMissingMinnesotaCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        candidateElections: missingLinkCandidates,
        contributionRows: contributionRowsForAutoLink ?? [],
        sourceUrl: contributionSourceUrl,
      });
      autoLinkLinkedCount = autoLinkResults.filter((result) => result.status === "linked").length;
      for (const result of autoLinkResults) {
        if (result.status !== "linked") {
          console.warn("Minnesota finance auto-link did not link candidate election:", result);
        }
      }
    } catch (error) {
      console.warn(
        "Minnesota finance auto-link skipped; continuing with already-linked candidate sync:",
        error instanceof Error ? error.message : error
      );
    }
  }

  const due = await listDueMinnesotaCandidateFinanceSyncRows(input.db, {
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
        const loaded = await loadMinnesotaExpenditureRowsForCommittees({
          committeeIds: collectCommitteeIds(due.rows),
          rawDataCacheDir: input.rawDataCacheDir,
        });
        expenditureRowsForBatch = loaded.rows;
        expenditureSourceUrl = loaded.sourceUrl;
      } else {
        expenditureRowsForBatch = [];
      }
    } catch (error) {
      console.warn(
        "Minnesota campaign finance expenditure artifact unavailable; syncing candidate links without outside spending:",
        error instanceof Error ? error.message : error
      );
      expenditureRowsForBatch = undefined;
    }
  }

  let outsideContributionRowsForBatch = input.outsideContributionRows;
  let outsideContributionSourceUrl = input.outsideSourceUrl ?? null;
  if (outsideContributionRowsForBatch === undefined && expenditureRowsForBatch !== undefined) {
    try {
      const outsideCommitteeIds = collectOutsideSpenderCommitteeIds(expenditureRowsForBatch);
      if (outsideCommitteeIds.length > 0) {
        const loaded = await loadMinnesotaOutsideContributionRowsForCommittees({
          committeeIds: outsideCommitteeIds,
          rawDataCacheDir: input.rawDataCacheDir,
        });
        outsideContributionRowsForBatch = loaded.rows;
        outsideContributionSourceUrl = loaded.sourceUrl;
      } else {
        outsideContributionRowsForBatch = [];
      }
    } catch (error) {
      console.warn(
        "Minnesota campaign finance IE contributor artifact unavailable; syncing candidate links without industry backtrace:",
        error instanceof Error ? error.message : error
      );
      outsideContributionRowsForBatch = undefined;
    }
  }

  const results: MinnesotaCandidateFinanceBatchSyncItemResult[] = [];
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
        contributionRows: contributionRowsForAutoLink ?? [],
        contributionSourceUrl,
        expenditureRows: expenditureRowsForBatch,
        expenditureSourceUrl,
        outsideContributionRows: outsideContributionRowsForBatch,
        outsideSourceUrl: outsideContributionSourceUrl,
        trustedCommittee: {
          committeeId: row.committeeId,
          committeeName: row.committeeName,
          sourceUrl: row.sourceUrl,
        },
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
