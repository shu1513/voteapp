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
import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueRow,
} from "../finance/standardStateFinanceDueListQuery.js";
import { MINNESOTA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./minnesotaFinanceEligibleOffices.js";
import {
  syncMinnesotaCandidateFinance,
  type MinnesotaCandidateFinanceSyncResult,
} from "./minnesotaCandidateFinanceSync.js";
import {
  fetchMinnesotaCandidateFinancialSummary,
  type MinnesotaCandidateFinancialSummary,
} from "./minnesotaCandidateFinancialSummaryClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type MinnesotaCandidateFinanceDueRow = StandardStateFinanceDueRow;

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
  fetchMinnesotaCandidateFinancialSummaryFn?: typeof fetchMinnesotaCandidateFinancialSummary;
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

export const listDueMinnesotaCandidateFinanceSyncRows = createStandardStateFinanceDueListQuery({
  state: "MN",
  tables: {
    links: "mn_candidate_finance_links",
    summaries: "mn_candidate_finance_summaries",
  },
  eligibleOfficeKeys: MINNESOTA_FINANCE_ELIGIBLE_OFFICE_KEYS,
});

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
  const fetchFinancialSummaryFn =
    input.fetchMinnesotaCandidateFinancialSummaryFn ?? fetchMinnesotaCandidateFinancialSummary;
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
  const financialSummaryRequests = new Map<string, Promise<MinnesotaCandidateFinancialSummary | null>>();
  for (const row of due.rows) {
    let financialSummary: MinnesotaCandidateFinancialSummary | undefined;
    try {
      const requestKey = `${normalizeCommitteeId(row.committeeId)}:${row.electionYear}`;
      let financialSummaryRequest = financialSummaryRequests.get(requestKey);
      if (!financialSummaryRequest) {
        financialSummaryRequest = fetchFinancialSummaryFn({
          committeeId: row.committeeId,
          electionYear: row.electionYear,
        });
        financialSummaryRequests.set(requestKey, financialSummaryRequest);
      }
      // Explicit CFB "no data" is not deletion evidence. Passing undefined lets the
      // writer preserve previously synced totals while still refreshing other data.
      financialSummary = (await financialSummaryRequest) ?? undefined;
    } catch (error) {
      console.warn(
        `Minnesota CFB financial summary unavailable for committee ${row.committeeId}; preserving existing direct totals:`,
        error instanceof Error ? error.message : error
      );
    }

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
        financialSummary,
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
