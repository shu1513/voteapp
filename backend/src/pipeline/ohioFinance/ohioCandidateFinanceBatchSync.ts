import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueRow,
} from "../finance/standardStateFinanceDueListQuery.js";
import {
  autoLinkMissingOhioCandidateFinanceLinks,
} from "./ohioCandidateCommitteeAutoLinker.js";
import {
  syncOhioCandidateFinance,
  type OhioCandidateOutsideFinanceInput,
  type OhioCandidateFinanceSyncResult,
} from "./ohioCandidateFinanceSync.js";
import {
  createOhioDirectContributionAccumulator,
  type OhioDirectContributionAggregationResult,
} from "./ohioDirectContributionAggregator.js";
import { OHIO_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./ohioFinanceEligibleOffices.js";
import {
  aggregateOhioOutsideSpending,
  type OhioOutsideSpendingCandidateTarget,
} from "./ohioOutsideSpendingAggregator.js";
import { readOhioSos31uDetailBundle } from "./ohioSosArtifactAcquisition.js";
import {
  DEFAULT_OHIO_SOS_CACHE_DIR,
  getOhioSosArtifactPaths,
  OHIO_SOS_FILE_TRANSFER_PAGE_URL,
  readOhioSosArtifactManifest,
  type OhioSosProductKey,
} from "./ohioSosArtifactCache.js";
import {
  isOhioSos31uExpenditureRow,
  streamOhioSosBulkFile,
  OHIO_SOS_CANDIDATE_CONTRIBUTIONS_FAMILY,
  OHIO_SOS_CANDIDATE_COVER_FAMILY,
  OHIO_SOS_CANDIDATE_EXPENDITURES_FAMILY,
  OHIO_SOS_CANDIDATE_LIST_FAMILY,
  OHIO_SOS_PAC_COVER_FAMILY,
  OHIO_SOS_PAC_EXPENDITURES_FAMILY,
  OHIO_SOS_PARTY_COVER_FAMILY,
  OHIO_SOS_PARTY_EXPENDITURES_FAMILY,
  type OhioSosBulkFileFamily,
  type OhioSosCandidateCommitteeListRow,
  type OhioSosCoverPageRow,
  type OhioSosExpenditureRow,
} from "./ohioSosBulkFiles.js";

// Batch sync for Ohio candidate finance (ohio_plan.md PR 7), maryland
// pattern with one structural difference: the ~90 MB CAC_CON files are
// never materialized (decision 10). Each cycle year streams its two
// contribution files exactly once, feeding one direct accumulator per
// linked committee; the cover files and 31-U rows are small enough to hold.
//
// Outside spending is aggregated once per election year over the DEDUPED
// (candidate name, office) targets of that year's due rows — a candidate
// whose primary and general elections are both due would otherwise appear
// twice in the target list and quarantine every row aimed at them as
// ambiguous. Identical (name, office) pairs share one result; genuinely
// different candidates with the same name stay ambiguous by design
// (decision 5).
//
// Artifact failure policy: missing direct artifacts (contributions or the
// candidate cover file) fail every due row of the year — receipts without
// them would be fabricated. Missing outside artifacts (expenditure files,
// PAC/party covers, or the 31-U detail bundle) only disable the outside
// leg: the sync passes outsideFinance null, the writer's preserveWhenNull
// policy keeps the stored outside totals, and the group rows are left
// untouched.

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type OhioCandidateFinanceDueRow = StandardStateFinanceDueRow;

export type OhioCandidateListData = {
  rows: OhioSosCandidateCommitteeListRow[];
  sourceUrl: string | null;
};

export type OhioCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
  autoLinkMissingLinks?: boolean;
  // Cents of slack for the 31-U reconciliation gates (default 0 — the real
  // files agree exactly).
  outsideToleranceCents?: number;
  // Test injection points.
  candidateListData?: OhioCandidateListData;
  syncOhioCandidateFinanceFn?: typeof syncOhioCandidateFinance;
};

export type OhioCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: OhioCandidateFinanceSyncResult;
  error?: string;
};

// Per-year outside-spending health, surfaced so a live run can report its
// reconciliation and match rates without re-deriving them.
export type OhioOutsideAggregationYearSummary = {
  electionYear: number;
  available: boolean;
  error?: string;
  reportCount?: number;
  quarantinedReportCount?: number;
  missingDetailReportKeyCount?: number;
  unmatchedTargetCount?: number;
  ambiguousTargetCount?: number;
  attributedRowCount?: number;
  attributedCents?: number;
};

export type OhioCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  outsideAggregationByYear: OhioOutsideAggregationYearSummary[];
  results: OhioCandidateFinanceBatchSyncItemResult[];
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid Ohio finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Ohio finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function rawDataCacheDir(inputCacheDir?: string): string {
  return inputCacheDir ?? (process.env.OHIO_SOS_RAW_DATA_CACHE_DIR?.trim() || DEFAULT_OHIO_SOS_CACHE_DIR);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function groupDueRowsByYear(rows: readonly OhioCandidateFinanceDueRow[]): Map<number, OhioCandidateFinanceDueRow[]> {
  const byYear = new Map<number, OhioCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

async function collectBulkFileRows<T>(input: {
  cacheDir: string;
  productKey: OhioSosProductKey;
  transactionYear?: number;
  family: OhioSosBulkFileFamily<T>;
  now?: Date;
  filter?: (row: T) => boolean;
}): Promise<T[]> {
  const { filePath } = getOhioSosArtifactPaths(input);
  const rows: T[] = [];
  await streamOhioSosBulkFile<T>({
    path: filePath,
    family: input.family,
    now: input.now,
    visit: (row) => {
      if (!input.filter || input.filter(row)) {
        rows.push(row);
      }
    },
  });
  return rows;
}

async function loadCandidateListData(input: {
  cacheDir: string;
  now?: Date;
}): Promise<OhioCandidateListData> {
  const paths = getOhioSosArtifactPaths({ cacheDir: input.cacheDir, productKey: "candidate_list" });
  const rows = await collectBulkFileRows<OhioSosCandidateCommitteeListRow>({
    cacheDir: input.cacheDir,
    productKey: "candidate_list",
    family: OHIO_SOS_CANDIDATE_LIST_FAMILY,
    now: input.now,
  });
  const manifest = await readOhioSosArtifactManifest(paths.manifestPath);
  return { rows, sourceUrl: manifest?.fileTransferPageUrl ?? OHIO_SOS_FILE_TRANSFER_PAGE_URL };
}

// The three cumulative cover files are shared by every election year, so
// they are loaded at most once per run.
type CoverRowsLoader = {
  candidateCoverRows: () => Promise<OhioSosCoverPageRow[]>;
  allCoverRows: () => Promise<OhioSosCoverPageRow[]>;
};

function createCoverRowsLoader(input: { cacheDir: string; now?: Date }): CoverRowsLoader {
  let candidatePromise: Promise<OhioSosCoverPageRow[]> | null = null;
  let pacAndPartyPromise: Promise<OhioSosCoverPageRow[]> | null = null;

  const candidateCoverRows = (): Promise<OhioSosCoverPageRow[]> => {
    candidatePromise ??= collectBulkFileRows<OhioSosCoverPageRow>({
      cacheDir: input.cacheDir,
      productKey: "candidate_cover",
      family: OHIO_SOS_CANDIDATE_COVER_FAMILY,
      now: input.now,
    });
    return candidatePromise;
  };

  const allCoverRows = async (): Promise<OhioSosCoverPageRow[]> => {
    pacAndPartyPromise ??= (async () => {
      const pacRows = await collectBulkFileRows<OhioSosCoverPageRow>({
        cacheDir: input.cacheDir,
        productKey: "pac_cover",
        family: OHIO_SOS_PAC_COVER_FAMILY,
        now: input.now,
      });
      const partyRows = await collectBulkFileRows<OhioSosCoverPageRow>({
        cacheDir: input.cacheDir,
        productKey: "party_cover",
        family: OHIO_SOS_PARTY_COVER_FAMILY,
        now: input.now,
      });
      return [...pacRows, ...partyRows];
    })();
    return [...(await candidateCoverRows()), ...(await pacAndPartyPromise)];
  };

  return { candidateCoverRows, allCoverRows };
}

// One direct-aggregation pass for a year: stream CAC_CON_{Y-1,Y} once,
// feeding one accumulator per distinct linked committee.
async function aggregateDirectForYear(input: {
  electionYear: number;
  committeeIds: readonly string[];
  cacheDir: string;
  coverRows: CoverRowsLoader;
  sourceUrl: string;
  now?: Date;
}): Promise<Map<string, OhioDirectContributionAggregationResult>> {
  const accumulators = new Map(
    input.committeeIds.map((committeeId) => [
      committeeId,
      createOhioDirectContributionAccumulator({
        committeeId,
        electionYear: input.electionYear,
        sourceUrl: input.sourceUrl,
      }),
    ])
  );

  for (const transactionYear of [input.electionYear - 1, input.electionYear]) {
    const { filePath } = getOhioSosArtifactPaths({
      cacheDir: input.cacheDir,
      productKey: "candidate_contributions",
      transactionYear,
    });
    await streamOhioSosBulkFile({
      path: filePath,
      family: OHIO_SOS_CANDIDATE_CONTRIBUTIONS_FAMILY,
      now: input.now,
      visit: (row) => {
        for (const accumulator of accumulators.values()) {
          accumulator.add(row);
        }
      },
    });
  }

  const coverRows = await input.coverRows.candidateCoverRows();
  return new Map(
    [...accumulators.entries()].map(([committeeId, accumulator]) => [
      committeeId,
      accumulator.finish({ coverRows }),
    ])
  );
}

const OUTSIDE_EXPENDITURE_PRODUCTS: ReadonlyArray<{
  productKey: OhioSosProductKey;
  family: OhioSosBulkFileFamily<OhioSosExpenditureRow>;
}> = [
  { productKey: "candidate_expenditures", family: OHIO_SOS_CANDIDATE_EXPENDITURES_FAMILY },
  { productKey: "pac_expenditures", family: OHIO_SOS_PAC_EXPENDITURES_FAMILY },
  { productKey: "party_expenditures", family: OHIO_SOS_PARTY_EXPENDITURES_FAMILY },
];

function outsideTargetKey(row: Pick<OhioCandidateFinanceDueRow, "candidateName" | "officeName">): string {
  return `${row.candidateName}\u0000${row.officeName}`;
}

type OutsideAggregationForYear = {
  // Null when the outside leg was unavailable for the year.
  byTargetKey: Map<string, OhioCandidateOutsideFinanceInput> | null;
  summary: OhioOutsideAggregationYearSummary;
};

async function aggregateOutsideForYear(input: {
  electionYear: number;
  dueRows: readonly OhioCandidateFinanceDueRow[];
  cacheDir: string;
  coverRows: CoverRowsLoader;
  sourceUrl: string;
  toleranceCents?: number;
  now?: Date;
}): Promise<OutsideAggregationForYear> {
  try {
    const annualExpenditureRows: OhioSosExpenditureRow[] = [];
    for (const { productKey, family } of OUTSIDE_EXPENDITURE_PRODUCTS) {
      for (const transactionYear of [input.electionYear - 1, input.electionYear]) {
        const { filePath } = getOhioSosArtifactPaths({ cacheDir: input.cacheDir, productKey, transactionYear });
        await streamOhioSosBulkFile<OhioSosExpenditureRow>({
          path: filePath,
          family,
          now: input.now,
          visit: (row) => {
            if (isOhioSos31uExpenditureRow(row)) {
              annualExpenditureRows.push(row);
            }
          },
        });
      }
    }

    const detailReports = await readOhioSos31uDetailBundle({
      cacheDir: input.cacheDir,
      cycleYear: input.electionYear,
    });
    const coverRows = await input.coverRows.allCoverRows();

    const targetsByKey = new Map<string, OhioOutsideSpendingCandidateTarget>();
    for (const row of input.dueRows) {
      const key = outsideTargetKey(row);
      if (!targetsByKey.has(key)) {
        targetsByKey.set(key, {
          candidateKey: key,
          candidateName: row.candidateName,
          officeName: row.officeName,
        });
      }
    }

    const aggregation = aggregateOhioOutsideSpending({
      electionYear: input.electionYear,
      annualExpenditureRows,
      detailReports,
      coverRows,
      candidates: [...targetsByKey.values()],
      toleranceCents: input.toleranceCents,
      sourceUrl: input.sourceUrl,
    });

    const byTargetKey = new Map<string, OhioCandidateOutsideFinanceInput>();
    for (const key of targetsByKey.keys()) {
      // The aggregation only lists candidates with attributed rows; every
      // other target really has zero attributed outside spending.
      byTargetKey.set(key, { supportTotal: 0, opposeTotal: 0, groups: [] });
    }
    for (const candidate of aggregation.candidates) {
      byTargetKey.set(candidate.candidateKey, {
        supportTotal: candidate.supportTotal,
        opposeTotal: candidate.opposeTotal,
        groups: candidate.groups,
      });
    }

    return {
      byTargetKey,
      summary: {
        electionYear: input.electionYear,
        available: true,
        reportCount: aggregation.reports.length,
        quarantinedReportCount: aggregation.quarantinedReportCount,
        missingDetailReportKeyCount: aggregation.missingDetailReportKeys.length,
        unmatchedTargetCount: aggregation.unmatchedTargets.length,
        ambiguousTargetCount: aggregation.ambiguousTargets.length,
        attributedRowCount: aggregation.attributedRowCount,
        attributedCents: aggregation.attributedCents,
      },
    };
  } catch (error) {
    const message = errorMessage(error);
    console.warn(
      `Ohio SoS outside-spending artifacts unavailable for ${input.electionYear}; syncing direct finance and preserving stored outside totals:`,
      message
    );
    return {
      byTargetKey: null,
      summary: { electionYear: input.electionYear, available: false, error: message },
    };
  }
}

export const listDueOhioCandidateFinanceSyncRows = createStandardStateFinanceDueListQuery({
  state: "OH",
  tables: {
    links: "oh_candidate_finance_links",
    summaries: "oh_candidate_finance_summaries",
  },
  eligibleOfficeKeys: OHIO_FINANCE_ELIGIBLE_OFFICE_KEYS,
});

export async function syncDueOhioCandidateFinance(
  input: OhioCandidateFinanceBatchSyncInput
): Promise<OhioCandidateFinanceBatchSyncResult> {
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
  const cacheDir = rawDataCacheDir(input.rawDataCacheDir);
  const syncFn = input.syncOhioCandidateFinanceFn ?? syncOhioCandidateFinance;

  if (!dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const candidateListData = input.candidateListData ?? (await loadCandidateListData({ cacheDir, now }));
      const autoLinkResults = await autoLinkMissingOhioCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        candidateListRows: candidateListData.rows,
        sourceUrl: candidateListData.sourceUrl,
      });
      for (const result of autoLinkResults) {
        if (result.status !== "linked") {
          console.warn("Ohio finance auto-link did not link candidate election:", result);
        }
      }
    } catch (error) {
      console.warn(
        "Ohio finance auto-link skipped; continuing with already-linked candidate sync:",
        errorMessage(error)
      );
    }
  }

  const due = await listDueOhioCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  const coverRows = createCoverRowsLoader({ cacheDir, now });
  const directByYear = new Map<number, Map<string, OhioDirectContributionAggregationResult>>();
  const directLoadErrorsByYear = new Map<number, string>();
  const outsideByYear = new Map<number, OutsideAggregationForYear>();
  const invalidCommitteeIdErrors = new Map<string, string>();

  for (const [year, yearRows] of groupDueRowsByYear(due.rows).entries()) {
    // A non-numeric committee id on a link is upstream damage; it fails that
    // row alone, never the year.
    const committeeIds = new Set<string>();
    for (const row of yearRows) {
      const committeeId = row.committeeId.trim();
      if (/^[0-9]+$/.test(committeeId)) {
        committeeIds.add(committeeId);
      } else {
        invalidCommitteeIdErrors.set(
          `${row.candidateId}\u0000${row.electionId}`,
          `Ohio finance link committee id is not a numeric SOS master key: ${row.committeeId}`
        );
      }
    }

    try {
      directByYear.set(
        year,
        await aggregateDirectForYear({
          electionYear: year,
          committeeIds: [...committeeIds],
          cacheDir,
          coverRows,
          sourceUrl: OHIO_SOS_FILE_TRANSFER_PAGE_URL,
          now,
        })
      );
    } catch (error) {
      directLoadErrorsByYear.set(year, errorMessage(error));
      console.warn(
        `Ohio SoS direct-finance artifacts unavailable for ${year}; failing the year's due candidates:`,
        errorMessage(error)
      );
      continue;
    }

    outsideByYear.set(
      year,
      await aggregateOutsideForYear({
        electionYear: year,
        dueRows: yearRows,
        cacheDir,
        coverRows,
        sourceUrl: OHIO_SOS_FILE_TRANSFER_PAGE_URL,
        toleranceCents: input.outsideToleranceCents,
        now,
      })
    );
  }

  const results: OhioCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    const base = {
      candidateId: row.candidateId,
      electionId: row.electionId,
      electionYear: row.electionYear,
      committeeId: row.committeeId,
    };

    const invalidCommitteeIdError = invalidCommitteeIdErrors.get(`${row.candidateId}\u0000${row.electionId}`);
    if (invalidCommitteeIdError) {
      results.push({ ...base, ok: false, error: invalidCommitteeIdError });
      continue;
    }
    const directLoadError = directLoadErrorsByYear.get(row.electionYear);
    if (directLoadError) {
      results.push({ ...base, ok: false, error: directLoadError });
      continue;
    }
    const directFinance = directByYear.get(row.electionYear)?.get(row.committeeId.trim());
    if (!directFinance) {
      results.push({ ...base, ok: false, error: "Ohio direct finance aggregation missing for committee" });
      continue;
    }
    const outside = outsideByYear.get(row.electionYear);
    const outsideFinance =
      outside?.byTargetKey?.get(outsideTargetKey(row)) ??
      (outside?.byTargetKey ? { supportTotal: 0, opposeTotal: 0, groups: [] } : null);

    try {
      const result = await syncFn({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeName: row.officeName,
        district: row.district,
        committee: {
          committeeId: row.committeeId,
          committeeName: row.committeeName,
          sourceUrl: row.sourceUrl,
        },
        directFinance,
        outsideFinance,
        sourceUrl: OHIO_SOS_FILE_TRANSFER_PAGE_URL,
        now,
        dryRun,
      });
      results.push({ ...base, ok: true, result });
    } catch (error) {
      results.push({ ...base, ok: false, error: errorMessage(error) });
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
    outsideAggregationByYear: [...outsideByYear.values()].map((outside) => outside.summary),
    results,
  };
}
