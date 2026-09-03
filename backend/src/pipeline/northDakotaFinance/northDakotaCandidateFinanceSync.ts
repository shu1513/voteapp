// North Dakota per-candidate finance sync (plan Phase 2). CACHE-ONLY: it
// never touches the live portal — the refresh CLI (its own flag) fills the
// artifact cache. Every gate runs before anything is written and every gate
// fails closed, preserving the prior snapshot:
//   1. eligible office + valid entityId + Nov-2026-scope election year;
//   2. the candidacy window resolves from the schedule files (gate 4);
//   3. every window year's contributions CSV and API harvest are cached and
//      parse clean (a missing year is a missing year, never an empty one);
//   4. per year, the committee's CSV rows and API rows reconcile cent-exact
//      and multiset-exact on (date, amount) — the committee-level amendment
//      evidence Phase 0A pinned (gate 5), re-proved on every run so a CSV
//      that ever starts carrying superseded versions stops publication;
//   5. no category / contributor-type value outside the pinned vocabulary.
// Only totalReceipts and directContributionTotal are written on the direct
// side; the writer's preserve-when-NULL policy leaves totalDisbursements and
// cashOnHand untouched (year-end spending is a later phase).
// Direct breakdowns: contribution-size buckets from the CSV rows and, when
// the display gate passes (Phase 3, hard fact 3), filed occupations from
// the reconciled API rows — the API is the only surface that carries them.
//
// Outside spending (Phase 4, component isolation): support/oppose groups
// from the cached IE harvest, the target resolved through the cached
// registry. Any failure on that path — a missing IE or registry artifact,
// an ambiguous target name, a YTD control mismatch — skips ONLY the outside
// component (totals stay NULL = preserved, stored groups untouched) and is
// reported on the result; the direct component still publishes. A clean
// pass with no IE rows for the candidate writes $0 and clears stale groups:
// the harvest is authoritative for what has been filed.
//
// Zero filed rows in the window is NOT $0 raised: the bulk file cannot tell
// a committee that reported no contributions from one whose first
// cumulative report is not yet due (plan: "not yet due" is never zero). Such
// a candidate gets a synced summary row with NULL money and no breakdowns,
// so the card shows nothing rather than a false zero.

import { resolve } from "node:path";

import type { Pool, PoolClient } from "pg";

import type { NorthDakotaCommitteeRow, NorthDakotaTransactionRow } from "./northDakotaCfrsClient.js";
import {
  DEFAULT_NORTH_DAKOTA_CFRS_CACHE_DIR,
  readNorthDakotaApiContributionsArtifact,
  readNorthDakotaApiIndependentExpendituresArtifact,
  readNorthDakotaBulkArtifact,
  readNorthDakotaRegistryArtifact,
} from "./northDakotaCfrsArtifactCache.js";
import {
  parseNorthDakotaContributionCsv,
  parseNorthDakotaReportingScheduleCsv,
  type NorthDakotaContributionCsvRow,
  type NorthDakotaReportingScheduleCsvRow,
} from "./northDakotaCfrsCsv.js";
import { normalizeNorthDakotaCandidateNameForStorage } from "./northDakotaCandidateCommitteeResolver.js";
import {
  aggregateNorthDakotaDirectFinance,
  type NorthDakotaDirectFinanceAggregationResult,
} from "./northDakotaDirectContributionAggregator.js";
import { isNorthDakotaFinanceEligibleOffice } from "./northDakotaFinanceEligibleOffices.js";
import {
  aggregateNorthDakotaOutsideSpending,
  resolveNorthDakotaIeTargetName,
  type NorthDakotaOutsideSpendingAggregationResult,
} from "./northDakotaOutsideSpendingAggregator.js";
import {
  NORTH_DAKOTA_CFRS_SOURCE_URL,
  normalizeNorthDakotaEntityId,
  replaceNorthDakotaCandidateFinanceSnapshot,
  type NorthDakotaFinanceLinkSource,
} from "./northDakotaFinanceWriter.js";
import { reconcileNorthDakotaCommittee } from "./northDakotaPhaseZero.js";
import {
  northDakotaCycleWindowYears,
  northDakotaScheduleYearsForElection,
  resolveNorthDakotaCandidateCycleWindow,
  type NorthDakotaCycleWindow,
} from "./northDakotaReportingCycleWindows.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

export class NorthDakotaCandidateFinanceSyncError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NorthDakotaCandidateFinanceSyncError";
  }
}

export type NorthDakotaFinanceArtifactLoader = {
  scheduleRows: (year: number) => Promise<NorthDakotaReportingScheduleCsvRow[]>;
  contributionRows: (year: number) => Promise<NorthDakotaContributionCsvRow[]>;
  apiContributionRows: (year: number) => Promise<NorthDakotaTransactionRow[]>;
  apiIndependentExpenditureRows: (year: number) => Promise<NorthDakotaTransactionRow[]>;
  registryRows: (electionYear: number) => Promise<NorthDakotaCommitteeRow[]>;
};

function requireClean<T>(label: string, result: { rows: T[]; errors: { line: number; reason: string }[] }): T[] {
  if (result.errors.length > 0) {
    throw new NorthDakotaCandidateFinanceSyncError(
      `cached ${label} artifact has ${result.errors.length} row errors (line ${result.errors[0]!.line}: ${result.errors[0]!.reason})`
    );
  }
  return result.rows;
}

export function resolveNorthDakotaCfrsCacheDir(cacheDir?: string): string {
  return resolve(cacheDir ?? (process.env.NORTH_DAKOTA_CFRS_RAW_DATA_CACHE_DIR?.trim() || DEFAULT_NORTH_DAKOTA_CFRS_CACHE_DIR));
}

/** Reads and parses each cached artifact once per (kind, year); no live fetches. */
export function createNorthDakotaFinanceArtifactLoader(cacheDir?: string): NorthDakotaFinanceArtifactLoader {
  const resolvedCacheDir = resolveNorthDakotaCfrsCacheDir(cacheDir);
  const memo = new Map<string, Promise<unknown>>();
  const once = <T>(key: string, load: () => Promise<T>): Promise<T> => {
    let entry = memo.get(key) as Promise<T> | undefined;
    if (entry === undefined) {
      entry = load();
      memo.set(key, entry);
    }
    return entry;
  };
  const bulkText = (kind: "contributions" | "reporting_schedules", year: number) =>
    readNorthDakotaBulkArtifact({ kind, year, cacheDir: resolvedCacheDir }).then((read) => read.csvText);
  return {
    scheduleRows: (year) =>
      once(`reps:${year}`, async () =>
        requireClean(`reporting-schedules ${year}`, parseNorthDakotaReportingScheduleCsv(await bulkText("reporting_schedules", year)))
      ),
    contributionRows: (year) =>
      once(`con:${year}`, async () =>
        requireClean(`contributions ${year}`, parseNorthDakotaContributionCsv(await bulkText("contributions", year)))
      ),
    apiContributionRows: (year) =>
      once(`api:${year}`, async () => (await readNorthDakotaApiContributionsArtifact({ year, cacheDir: resolvedCacheDir })).rows),
    apiIndependentExpenditureRows: (year) =>
      once(`ie:${year}`, async () => (await readNorthDakotaApiIndependentExpendituresArtifact({ year, cacheDir: resolvedCacheDir })).rows),
    registryRows: (electionYear) =>
      once(`registry:${electionYear}`, async () => (await readNorthDakotaRegistryArtifact({ electionYear, cacheDir: resolvedCacheDir })).rows),
  };
}

/** "synced": totals + groups written ($0 and no groups is a real, clean result). "skipped": nothing on the outside side was touched. */
export type NorthDakotaOutsideSyncOutcome =
  | ({ status: "synced"; registryCandidateName: string } & Omit<NorthDakotaOutsideSpendingAggregationResult, "groups">)
  | { status: "skipped"; reason: string };

export type NorthDakotaYearReconciliation = {
  year: number;
  csvRowCount: number;
  apiRowCount: number;
  csvTotalCents: number;
  apiTotalCents: number;
  amendedApiRowCount: number;
};

export type NorthDakotaCandidateFinanceSyncResult = {
  dryRun: boolean;
  /** "no_filed_rows": the committee has no contribution rows in the window; money stays NULL. */
  status: "synced" | "no_filed_rows";
  entityId: string;
  window: Pick<NorthDakotaCycleWindow, "election" | "windowStart" | "windowEnd">;
  windowYears: number[];
  totalReceipts: number | null;
  directContributionTotal: number | null;
  reconciliation: NorthDakotaYearReconciliation[];
  aggregation: Omit<NorthDakotaDirectFinanceAggregationResult, "breakdowns">;
  breakdownCounts: { occupation: number; contribution_size: number };
  outside: NorthDakotaOutsideSyncOutcome;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
};

async function computeOutsideSpending(input: {
  entityId: string;
  electionYear: number;
  windowYears: readonly number[];
  loader: NorthDakotaFinanceArtifactLoader;
}): Promise<{ outcome: NorthDakotaOutsideSyncOutcome; groups: NorthDakotaOutsideSpendingAggregationResult["groups"] | undefined }> {
  try {
    const target = resolveNorthDakotaIeTargetName({
      entityId: input.entityId,
      electionYear: input.electionYear,
      committees: await input.loader.registryRows(input.electionYear),
    });
    if (target.status === "unresolved") {
      return { outcome: { status: "skipped", reason: `${target.reason}: ${target.detail}` }, groups: undefined };
    }
    const rows: NorthDakotaTransactionRow[] = [];
    for (const year of input.windowYears) {
      rows.push(...(await input.loader.apiIndependentExpenditureRows(year)));
    }
    const { groups, ...aggregation } = aggregateNorthDakotaOutsideSpending({
      targetName: target.targetName,
      electionYear: input.electionYear,
      rows,
    });
    return { outcome: { status: "synced", registryCandidateName: target.registryCandidateName, ...aggregation }, groups };
  } catch (error) {
    return { outcome: { status: "skipped", reason: error instanceof Error ? error.message : String(error) }, groups: undefined };
  }
}

export async function syncNorthDakotaCandidateFinance(input: {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  link: {
    entityId: string;
    committeeName: string;
    linkSource: NorthDakotaFinanceLinkSource;
    sourceUrl?: string | null;
  };
  now?: Date;
  dryRun?: boolean;
  cacheDir?: string;
  loadArtifacts?: NorthDakotaFinanceArtifactLoader;
}): Promise<NorthDakotaCandidateFinanceSyncResult> {
  const candidateName = input.candidateName.trim();
  if (!candidateName) {
    throw new NorthDakotaCandidateFinanceSyncError("candidateName is required");
  }
  const committeeName = input.link.committeeName.trim();
  if (!committeeName) {
    throw new NorthDakotaCandidateFinanceSyncError("link.committeeName is required");
  }
  if (!isNorthDakotaFinanceEligibleOffice({ officeScope: input.officeScope, officeCanonicalName: input.officeName })) {
    throw new NorthDakotaCandidateFinanceSyncError(
      `office ${input.officeScope}::${input.officeName} is not North Dakota-finance eligible`
    );
  }
  let entityId: string;
  try {
    entityId = normalizeNorthDakotaEntityId(input.link.entityId);
  } catch (error) {
    throw new NorthDakotaCandidateFinanceSyncError(error instanceof Error ? error.message : String(error));
  }
  if (!Number.isSafeInteger(input.electionYear) || input.electionYear < 2026 || input.electionYear > 2100) {
    throw new NorthDakotaCandidateFinanceSyncError(`invalid election year: ${input.electionYear}`);
  }
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new NorthDakotaCandidateFinanceSyncError("invalid now");
  }
  const dryRun = input.dryRun === true;
  const loader = input.loadArtifacts ?? createNorthDakotaFinanceArtifactLoader(input.cacheDir);

  // --- Window (gate 4): both schedule files that carry the election's
  // periods. A missing file fails closed rather than silently shortening
  // the window; the resolver also insists on the year-end period.
  const scheduleRows: NorthDakotaReportingScheduleCsvRow[] = [];
  for (const year of northDakotaScheduleYearsForElection(input.electionYear)) {
    scheduleRows.push(...(await loader.scheduleRows(year)));
  }
  const window = resolveNorthDakotaCandidateCycleWindow({ scheduleRows, electionYear: input.electionYear });
  const windowYears = northDakotaCycleWindowYears(window);

  // --- Artifacts + per-year CSV<->API reconciliation (gate 5).
  const contributionRows: NorthDakotaContributionCsvRow[] = [];
  const apiRows: NorthDakotaTransactionRow[] = [];
  const reconciliation: NorthDakotaYearReconciliation[] = [];
  for (const year of windowYears) {
    const yearContributions = await loader.contributionRows(year);
    const yearApiRows = await loader.apiContributionRows(year);
    const check = reconcileNorthDakotaCommittee({ entityId, csvRows: yearContributions, apiRows: yearApiRows });
    if (!check.totalsMatch || !check.multisetMatch) {
      throw new NorthDakotaCandidateFinanceSyncError(
        `${year} contributions do not reconcile for ${entityId}: CSV ${check.csvRowCount} rows / ${check.csvTotalCents}c ` +
          `vs API ${check.apiRowCount} rows / ${check.apiTotalCents}c (only-in-CSV ${check.onlyInCsv}, only-in-API ${check.onlyInApi})`
      );
    }
    reconciliation.push({
      year,
      csvRowCount: check.csvRowCount,
      apiRowCount: check.apiRowCount,
      csvTotalCents: check.csvTotalCents,
      apiTotalCents: check.apiTotalCents,
      amendedApiRowCount: check.amendedApiRowCount,
    });
    contributionRows.push(...yearContributions);
    apiRows.push(...yearApiRows);
  }

  // --- Aggregation (hard facts 1 and 3); unknown vocabulary fails closed.
  const aggregation = aggregateNorthDakotaDirectFinance({ entityId, window, contributionRows, apiRows });
  const unrecognized = [
    ...aggregation.unrecognizedContributionCategories.map((value) => `contribution category "${value}"`),
    ...aggregation.unrecognizedContributorTypes.map((value) => `contributor type "${value}"`),
  ];
  if (unrecognized.length > 0) {
    throw new NorthDakotaCandidateFinanceSyncError(`unrecognized values in window for ${entityId}: ${unrecognized.join(", ")}`);
  }
  // The summaries table pins every amount >= 0; a net-negative total is an
  // evidence problem to look at, not a number to publish.
  if (aggregation.totalReceiptsCents < 0 || aggregation.directContributionCents < 0) {
    throw new NorthDakotaCandidateFinanceSyncError(
      `negative window total for ${entityId}: receipts ${aggregation.totalReceiptsCents}c, direct ${aggregation.directContributionCents}c`
    );
  }

  const hasFiledRows = aggregation.contributionRowCount > 0;
  const totalReceipts = hasFiledRows ? aggregation.totalReceiptsCents / 100 : null;
  const directContributionTotal = hasFiledRows ? aggregation.directContributionCents / 100 : null;
  const sourceUrl = input.link.sourceUrl ?? NORTH_DAKOTA_CFRS_SOURCE_URL;
  const { breakdowns, ...aggregationSummary } = aggregation;

  // --- Outside spending (Phase 4): isolated — never fails the candidate.
  const outside = await computeOutsideSpending({ entityId, electionYear: input.electionYear, windowYears, loader });
  const outsideSupportTotal = outside.outcome.status === "synced" ? outside.outcome.supportTotal : null;
  const outsideOpposeTotal = outside.outcome.status === "synced" ? outside.outcome.opposeTotal : null;

  let summaryWritten = false;
  let directBreakdownsWritten = 0;
  let outsideGroupsWritten = 0;
  if (!dryRun) {
    const writeResult = await replaceNorthDakotaCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId: input.candidateId,
        electionId: input.electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeNorthDakotaCandidateNameForStorage(candidateName),
        officeName: input.officeName,
        district: input.district ?? null,
        entityId,
        committeeName,
        linkStatus: "active",
        linkSource: input.link.linkSource,
        sourceUrl,
        lastVerifiedAt: now,
      },
      syncedAt: now,
      summary: { totalReceipts, directContributionTotal, outsideSupportTotal, outsideOpposeTotal, sourceUrl },
      directBreakdowns: breakdowns.map((breakdown) => ({
        categoryType: breakdown.categoryType,
        categoryName: breakdown.categoryName,
        amount: breakdown.amount,
        contributorCount: breakdown.contributorCount,
        sourceUrl,
      })),
      // undefined leaves stored groups alone (component skipped); [] clears them.
      outsideGroups: outside.groups?.map((group) => ({
        committeeId: group.entityId,
        committeeName: group.committeeName,
        supportOppose: group.supportOppose,
        amount: group.amount,
        sourceUrl,
      })),
    });
    summaryWritten = writeResult.summaryWritten;
    directBreakdownsWritten = writeResult.directBreakdownsWritten;
    outsideGroupsWritten = writeResult.outsideGroupsWritten;
  }

  return {
    dryRun,
    status: hasFiledRows ? "synced" : "no_filed_rows",
    entityId,
    window: { election: window.election, windowStart: window.windowStart, windowEnd: window.windowEnd },
    windowYears,
    totalReceipts,
    directContributionTotal,
    reconciliation,
    aggregation: aggregationSummary,
    breakdownCounts: {
      occupation: breakdowns.filter((breakdown) => breakdown.categoryType === "occupation").length,
      contribution_size: breakdowns.filter((breakdown) => breakdown.categoryType === "contribution_size").length,
    },
    outside: outside.outcome,
    summaryWritten,
    directBreakdownsWritten,
    outsideGroupsWritten,
  };
}
