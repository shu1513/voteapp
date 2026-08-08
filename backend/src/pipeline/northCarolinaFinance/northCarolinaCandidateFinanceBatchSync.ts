import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceDueListQuery,
  type StandardStateFinanceDueRow,
} from "../finance/standardStateFinanceDueListQuery.js";
import {
  autoLinkMissingNorthCarolinaCandidateFinanceLinks,
  type NorthCarolinaCandidateSearchRowsLoader,
} from "./northCarolinaCandidateCommitteeAutoLinker.js";
import {
  syncNorthCarolinaCandidateFinance,
  type NorthCarolinaCandidateOutsideFinanceInput,
  type NorthCarolinaCandidateFinanceSyncResult,
} from "./northCarolinaCandidateFinanceSync.js";
import {
  aggregateNorthCarolinaDirectFinance,
  type NorthCarolinaDirectAggregationResult,
  type NorthCarolinaDirectReportInput,
} from "./northCarolinaDirectContributionAggregator.js";
import { NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./northCarolinaFinanceEligibleOffices.js";
import type { NorthCarolinaFinanceLinkSource } from "./northCarolinaFinanceWriter.js";
import { selectNcsbeCycleReportRows } from "./northCarolinaNcsbeArtifactAcquisition.js";
import {
  DEFAULT_NCSBE_CACHE_DIR,
  readNcsbeArtifact,
  type NcsbeArtifactKey,
} from "./northCarolinaNcsbeArtifactCache.js";
import { NCSBE_TRANSACTION_PAGE_SIZE } from "./northCarolinaNcsbeClient.js";
import {
  parseNcsbeCommitteeSearchPage,
  parseNcsbeDocumentListPage,
  parseNcsbeExpendituresPage,
  parseNcsbeReceiptsPage,
  parseNcsbeReportDetailPage,
  type NcsbeDocumentRow,
  type NcsbeTransactionPage,
} from "./northCarolinaNcsbeParsers.js";
import {
  aggregateNorthCarolinaOutsideSpending,
  type NorthCarolinaOutsideCandidateTarget,
  type NorthCarolinaOutsideReportInput,
} from "./northCarolinaOutsideSpendingAggregator.js";

// Batch sync for North Carolina candidate finance (north_carolina_plan.md
// PR 7), ohio pattern with the state's structural difference: instead of
// eleven bulk files there are many small per-report artifacts, all read from
// the local cache the acquisition script installs — this module NEVER touches
// the portal (decision 10: sync reads cache only). A missing or stale
// artifact fails closed per candidate (direct) or per year (outside), with
// the raw-refresh script named in the error.
//
// Identity plumbing: the sync needs no OrgGroupID — document inventories are
// cached by SBoEID alone, and OGID is a fetch-time parameter the acquisition
// derives (PR 5's SBoEID→OGID note lands there, where the portal URL is
// built). Auto-link resolves against the cached committee-search artifact for
// the candidate's name; a candidate without a cached search errors per
// candidate and is retried after the next acquisition run.
//
// Artifact failure policy (mirrors the PR 6 three-status contract):
// - Direct: a missing inventory artifact fails the candidate's due rows; a
//   missing/stale report artifact surfaces through the aggregator as
//   "incomplete_artifacts" — no write, previous snapshot kept.
// - Outside: a missing IE inventory OR any selected structured IE report
//   without readable artifacts makes the whole year's outside leg
//   unavailable (Ohio's stale-31-U-bundle precedent: a quarantined-missing
//   report's money is invisible, so any published total could be a silent
//   undercount). The sync then passes outsideFinance null and the writer's
//   preserveWhenNull policy keeps the stored outside totals. Reports
//   quarantined for PORTAL reasons (reconciliation mismatch, null IEAmount)
//   are the aggregator's own fail-closed exclusions and stay diagnostics.

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

// link_source rides along (ohio/alaska pattern) so a sync refresh writes the
// link back with its ORIGINAL provenance — rewriting a manual link as
// ncsbe_portal would both lose the provenance and expose the manual pin to
// auto-link supersession.
export type NorthCarolinaCandidateFinanceDueRow = StandardStateFinanceDueRow & {
  linkSource: NorthCarolinaFinanceLinkSource;
};

export type NorthCarolinaCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
  autoLinkMissingLinks?: boolean;
  // Test injection points.
  loadCandidateSearchRows?: NorthCarolinaCandidateSearchRowsLoader;
  syncNorthCarolinaCandidateFinanceFn?: typeof syncNorthCarolinaCandidateFinance;
};

export type NorthCarolinaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  // Decision-3 inverse-miss cross-check: this committee's regular reports
  // carry IE-typed declared rows, but no IE-inventory report of theirs was
  // aggregated — their IEs may live only in regular reports, which the
  // single-source rule never counts. Flagged for the PR 9 audit.
  ieInverseMissSuspected?: boolean;
  result?: NorthCarolinaCandidateFinanceSyncResult;
  error?: string;
};

// Per-year outside-spending health, surfaced so a live run can report its
// reconciliation and match rates without re-deriving them.
export type NorthCarolinaOutsideAggregationYearSummary = {
  electionYear: number;
  available: boolean;
  error?: string;
  reportCount?: number;
  quarantinedReportCount?: number;
  missingReportIdCount?: number;
  coverageGapCount?: number;
  unmatchedTargetCount?: number;
  ambiguousTargetCount?: number;
  attributedRowCount?: number;
  attributedCents?: number;
};

export type NorthCarolinaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  outsideAggregationByYear: NorthCarolinaOutsideAggregationYearSummary[];
  results: NorthCarolinaCandidateFinanceBatchSyncItemResult[];
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid North Carolina finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid North Carolina finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function rawDataCacheDir(inputCacheDir?: string): string {
  return (
    inputCacheDir ?? (process.env.NORTH_CAROLINA_NCSBE_RAW_DATA_CACHE_DIR?.trim() || DEFAULT_NCSBE_CACHE_DIR)
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function groupDueRowsByYear(
  rows: readonly NorthCarolinaCandidateFinanceDueRow[]
): Map<number, NorthCarolinaCandidateFinanceDueRow[]> {
  const byYear = new Map<number, NorthCarolinaCandidateFinanceDueRow[]>();
  for (const row of rows) {
    const yearRows = byYear.get(row.electionYear) ?? [];
    yearRows.push(row);
    byYear.set(row.electionYear, yearRows);
  }
  return byYear;
}

// The cached-search query convention: the candidate's display name, trimmed,
// exactly as the acquisition passes it to the portal search. One function so
// the cache writer and this reader can never disagree on the key.
export function northCarolinaCommitteeSearchQueryForCandidateName(candidateName: string): string {
  const query = candidateName.trim();
  if (query.length === 0) {
    throw new Error("North Carolina committee search needs a non-empty candidate name");
  }
  return query;
}

// Auto-link evidence from the artifact cache: the committee-search page
// cached for this candidate's name. Fail-closed — a candidate without a
// cached search throws (the auto-linker records it per candidate and moves
// on), because an empty rows array would read as "searched, found nothing"
// and mask the missing acquisition.
export function createNcsbeCachedCommitteeSearchLoader(
  cacheDir: string
): NorthCarolinaCandidateSearchRowsLoader {
  return async (candidateElection) => {
    const query = northCarolinaCommitteeSearchQueryForCandidateName(candidateElection.candidateName);
    const { body, manifest } = await readNcsbeArtifact({
      cacheDir,
      key: { type: "committee_search", query },
    });
    return { rows: parseNcsbeCommitteeSearchPage(body), sourceUrl: manifest.url };
  };
}

async function readArtifactOrExplain(input: {
  cacheDir: string;
  key: NcsbeArtifactKey;
}): Promise<{ body: string; url: string }> {
  try {
    const { body, manifest } = await readNcsbeArtifact(input);
    return { body, url: manifest.url };
  } catch (error) {
    throw new Error(
      `${errorMessage(error)} — run north-carolina-candidates:finance:raw:refresh to (re)acquire it`
    );
  }
}

// Reads and reassembles one report's complete transaction row set from its
// cached pages. Every page must agree on recordCountKey and the reassembled
// row count must equal it — a partial or mixed-vintage page set fails closed
// (decision 9's completeness contract, replayed at read time).
async function readTransactionRows<Row>(input: {
  cacheDir: string;
  reportId: string;
  kind: "receipts" | "expenditures";
  parse: (body: string) => NcsbeTransactionPage<Row>;
}): Promise<Row[]> {
  const label = `NCSBE report ${input.reportId} ${input.kind}`;
  const firstPage = await readArtifactOrExplain({
    cacheDir: input.cacheDir,
    key: { type: "report_transactions", reportId: input.reportId, kind: input.kind, page: 0 },
  });
  const parsedFirst = input.parse(firstPage.body);
  const recordCount = parsedFirst.recordCount;
  const rows = [...parsedFirst.rows];
  const pageCount = Math.max(1, Math.ceil(recordCount / NCSBE_TRANSACTION_PAGE_SIZE));
  for (let page = 1; page < pageCount; page += 1) {
    const pageArtifact = await readArtifactOrExplain({
      cacheDir: input.cacheDir,
      key: { type: "report_transactions", reportId: input.reportId, kind: input.kind, page },
    });
    const parsed = input.parse(pageArtifact.body);
    if (parsed.recordCount !== recordCount) {
      throw new Error(
        `${label} page ${page} reports recordCountKey ${parsed.recordCount}, page 0 reported ${recordCount} — ` +
          "mixed-vintage cached pages; run north-carolina-candidates:finance:raw:refresh"
      );
    }
    rows.push(...parsed.rows);
  }
  if (rows.length !== recordCount) {
    throw new Error(
      `${label} cached pages hold ${rows.length} rows but recordCountKey says ${recordCount} — ` +
        "incomplete cached page set; run north-carolina-candidates:finance:raw:refresh"
    );
  }
  return rows;
}

type DirectAggregationForCommittee = {
  result: NorthCarolinaDirectAggregationResult;
  // Per-report artifact read failures — the aggregator sees these reports as
  // missing and returns incomplete_artifacts; the messages ride into the item
  // error so the operator sees WHY the report could not be supplied.
  reportReadFailures: Array<{ reportId: string; message: string }>;
};

// One committee's direct-money aggregation from cache. The inventory artifact
// is required; per-report artifacts are supplied best-effort and the
// aggregator's own missing-report accounting decides the status.
async function aggregateDirectForCommittee(input: {
  cacheDir: string;
  electionYear: number;
  sboeId: string;
}): Promise<DirectAggregationForCommittee> {
  const inventory = await readArtifactOrExplain({
    cacheDir: input.cacheDir,
    key: { type: "document_inventory", sboeId: input.sboeId },
  });
  const inventoryRows = parseNcsbeDocumentListPage(inventory.body);

  // Same selection the acquisition fetches by (period overlap incl. unusable
  // dates), so the supplied report set and the aggregator's expectations can
  // only diverge when the cache is actually missing something.
  const { selected } = selectNcsbeCycleReportRows({
    rows: inventoryRows,
    cycleYear: input.electionYear,
  });
  const reports: NorthCarolinaDirectReportInput[] = [];
  const reportReadFailures: Array<{ reportId: string; message: string }> = [];
  const seenReportIds = new Set<string>();
  for (const row of selected) {
    const reportId = row.dataLink;
    if (reportId === null || seenReportIds.has(reportId)) {
      continue;
    }
    seenReportIds.add(reportId);
    try {
      const cover = await readArtifactOrExplain({
        cacheDir: input.cacheDir,
        key: { type: "report_cover", reportId },
      });
      reports.push({
        reportId,
        cover: parseNcsbeReportDetailPage(cover.body),
        receiptRows: await readTransactionRows({
          cacheDir: input.cacheDir,
          reportId,
          kind: "receipts",
          parse: parseNcsbeReceiptsPage,
        }),
        expenditureRows: await readTransactionRows({
          cacheDir: input.cacheDir,
          reportId,
          kind: "expenditures",
          parse: parseNcsbeExpendituresPage,
        }),
      });
    } catch (error) {
      reportReadFailures.push({ reportId, message: errorMessage(error) });
    }
  }

  return {
    result: aggregateNorthCarolinaDirectFinance({
      electionYear: input.electionYear,
      inventoryRows,
      reports,
      sourceUrl: inventory.url,
    }),
    reportReadFailures,
  };
}

const SECTION_TOTAL_EXPENDITURES = 90;

function officialExpenditureTotalCents(cover: ReturnType<typeof parseNcsbeReportDetailPage>): number | null {
  const section = cover.summarySections.find((row) => row.sequence === SECTION_TOTAL_EXPENDITURES);
  return section ? section.periodCents : null;
}

// Keyed by candidateId + office so one PERSON due for several elections
// (primary + general) shares a single target, while two DIFFERENT people who
// happen to share a name stay separate targets — the aggregator then
// quarantines rows aimed at that shared name as ambiguous instead of paying
// the same money to both (decision 5 fail-closed).
function outsideTargetKey(
  row: Pick<NorthCarolinaCandidateFinanceDueRow, "candidateId" | "officeName">
): string {
  return `${row.candidateId}\u0000${row.officeName}`;
}

type NorthCarolinaOutsideTargetCandidateRow = {
  candidateId: string;
  candidateName: string;
  officeName: string;
  officeScope: string;
  district: string | null;
};

// The ambiguity guard is only as good as its target universe: matching
// against just the current due page (stale-filtered and capped at
// maxCandidates) would let a same-name candidate look unique whenever their
// double is not due in the same run, and attribution would then depend on
// sync timing. The universe is every ACTIVE link of the election year —
// written rows stay limited to the due page (ohio pattern).
async function listNorthCarolinaOutsideTargetCandidatesForYear(
  db: Queryable,
  electionYear: number
): Promise<NorthCarolinaOutsideTargetCandidateRow[]> {
  const result = await db.query<{
    candidate_id: string;
    candidate_name: string | null;
    office_name: string;
    office_scope: string | null;
    district: string | null;
  }>(
    `
      SELECT DISTINCT
        candidate.id::text AS candidate_id,
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), ''),
          link.candidate_name_normalized
        ) AS candidate_name,
        link.office_name,
        office.scope AS office_scope,
        link.district
      FROM public.nc_candidate_finance_links AS link
      JOIN public.candidates AS candidate
        ON candidate.id = link.candidate_id
      JOIN public.elections AS election
        ON election.id = link.election_id
      LEFT JOIN public.offices AS office
        ON office.id = election.office_id
      WHERE link.link_status = 'active'
        AND link.election_year = $1::int
        AND candidate.deleted_at IS NULL
    `,
    [electionYear]
  );
  return result.rows
    .filter((row) => (row.candidate_name ?? "").trim().length > 0)
    .map((row) => ({
      candidateId: row.candidate_id,
      candidateName: row.candidate_name!,
      officeName: row.office_name,
      officeScope: row.office_scope ?? "",
      district: row.district,
    }));
}

type OutsideAggregationForYear = {
  // Null when the outside leg was unavailable for the year.
  byTargetKey: Map<string, NorthCarolinaCandidateOutsideFinanceInput> | null;
  // Filer keys (SBoEID / NC-IE-FILER hash) whose IE reports were aggregated
  // (not quarantined) — the inverse-miss cross-check's evidence set.
  aggregatedIeFilerKeys: Set<string>;
  summary: NorthCarolinaOutsideAggregationYearSummary;
};

async function aggregateOutsideForYear(input: {
  db: Queryable;
  cacheDir: string;
  electionYear: number;
  dueRows: readonly NorthCarolinaCandidateFinanceDueRow[];
}): Promise<OutsideAggregationForYear> {
  const unavailable = (message: string): OutsideAggregationForYear => {
    console.warn(
      `North Carolina outside-spending data unavailable for ${input.electionYear}; syncing direct finance and preserving stored outside totals:`,
      message
    );
    return {
      byTargetKey: null,
      aggregatedIeFilerKeys: new Set(),
      summary: { electionYear: input.electionYear, available: false, error: message },
    };
  };

  try {
    // Both cycle years' IE doc-type inventories (decision 9).
    const ieInventoryRows: NcsbeDocumentRow[] = [];
    let sourceUrl: string | null = null;
    for (const year of [input.electionYear - 1, input.electionYear]) {
      const inventory = await readArtifactOrExplain({
        cacheDir: input.cacheDir,
        key: { type: "ie_doc_type_inventory", year },
      });
      ieInventoryRows.push(...parseNcsbeDocumentListPage(inventory.body));
      sourceUrl = inventory.url;
    }

    // Best-effort report inputs for every structured filing; the aggregator
    // treats an unsupplied selected report as missing_artifacts, and any
    // missing report fails the year closed below.
    const reports: NorthCarolinaOutsideReportInput[] = [];
    const seenReportIds = new Set<string>();
    for (const row of ieInventoryRows) {
      const reportId = row.dataLink;
      if (reportId === null || seenReportIds.has(reportId)) {
        continue;
      }
      seenReportIds.add(reportId);
      try {
        const cover = await readArtifactOrExplain({
          cacheDir: input.cacheDir,
          key: { type: "report_cover", reportId },
        });
        reports.push({
          reportId,
          officialExpenditureTotalCents: officialExpenditureTotalCents(parseNcsbeReportDetailPage(cover.body)),
          expenditureRows: await readTransactionRows({
            cacheDir: input.cacheDir,
            reportId,
            kind: "expenditures",
            parse: parseNcsbeExpendituresPage,
          }),
        });
      } catch {
        // Left unsupplied — the aggregator's missing-report accounting (and
        // the fail-closed gate below) owns the consequence.
      }
    }

    // Due rows first (their result slices are the ones written), then the
    // year's full active-link universe so same-name doubles are visible to
    // the ambiguity guard even when they are not due this run.
    const universeRows = await listNorthCarolinaOutsideTargetCandidatesForYear(input.db, input.electionYear);
    const targetsByKey = new Map<string, NorthCarolinaOutsideCandidateTarget>();
    for (const row of [...input.dueRows, ...universeRows]) {
      const key = outsideTargetKey(row);
      if (!targetsByKey.has(key)) {
        targetsByKey.set(key, {
          candidateKey: key,
          candidateName: row.candidateName,
          officeScope: row.officeScope,
          district: row.district,
        });
      }
    }

    const aggregation = aggregateNorthCarolinaOutsideSpending({
      ieInventoryRows,
      reports,
      candidates: [...targetsByKey.values()],
      sourceUrl,
    });

    // A selected structured IE report with no readable cached artifacts means
    // its money is invisible — any total published around it could be a
    // false zero or an undercount, so the whole year fails closed to
    // "unavailable" (stored outside data is preserved) until re-acquisition.
    if (aggregation.missingReportIds.length > 0) {
      return unavailable(
        `${aggregation.missingReportIds.length} selected IE report(s) have no readable cached artifacts ` +
          `(run north-carolina-candidates:finance:raw:refresh): ${aggregation.missingReportIds.join(", ")}`
      );
    }

    const byTargetKey = new Map<string, NorthCarolinaCandidateOutsideFinanceInput>();
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

    const aggregatedIeFilerKeys = new Set<string>();
    for (const report of aggregation.reports) {
      if (!report.quarantined) {
        aggregatedIeFilerKeys.add(report.filerKey);
      }
    }

    return {
      byTargetKey,
      aggregatedIeFilerKeys,
      summary: {
        electionYear: input.electionYear,
        available: true,
        reportCount: aggregation.reports.length,
        quarantinedReportCount: aggregation.quarantinedReportCount,
        missingReportIdCount: aggregation.missingReportIds.length,
        coverageGapCount: aggregation.coverageGaps.length,
        unmatchedTargetCount: aggregation.unmatchedTargets.length,
        ambiguousTargetCount: aggregation.ambiguousTargets.length,
        attributedRowCount: aggregation.attributedRowCount,
        attributedCents: aggregation.attributedCents,
      },
    };
  } catch (error) {
    return unavailable(errorMessage(error));
  }
}

export const listDueNorthCarolinaCandidateFinanceSyncRows = createStandardStateFinanceDueListQuery({
  state: "NC",
  tables: {
    links: "nc_candidate_finance_links",
    summaries: "nc_candidate_finance_summaries",
  },
  eligibleOfficeKeys: NORTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  linkColumns: ["committee_id", "committee_name", "link_source"],
  mapRow: (row): NorthCarolinaCandidateFinanceDueRow => ({
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
    committeeId: row.committee_id as string,
    committeeName: row.committee_name as string,
    // The DB CHECK constraint pins link_source to this union.
    linkSource: row.link_source as NorthCarolinaFinanceLinkSource,
    sourceUrl: row.source_url,
    lastSyncedAt: row.last_synced_at,
  }),
});

export async function syncDueNorthCarolinaCandidateFinance(
  input: NorthCarolinaCandidateFinanceBatchSyncInput
): Promise<NorthCarolinaCandidateFinanceBatchSyncResult> {
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
  const syncFn = input.syncNorthCarolinaCandidateFinanceFn ?? syncNorthCarolinaCandidateFinance;

  if (!dryRun && input.autoLinkMissingLinks !== false) {
    try {
      const autoLinkResults = await autoLinkMissingNorthCarolinaCandidateFinanceLinks({
        db: input.db,
        now,
        maxCandidates,
        electionLookbackDays,
        electionLookaheadDays,
        loadCandidateSearchRows:
          input.loadCandidateSearchRows ?? createNcsbeCachedCommitteeSearchLoader(cacheDir),
      });
      for (const result of autoLinkResults) {
        if (result.status !== "linked") {
          console.warn("North Carolina finance auto-link did not link candidate election:", result);
        }
      }
    } catch (error) {
      console.warn(
        "North Carolina finance auto-link skipped; continuing with already-linked candidate sync:",
        errorMessage(error)
      );
    }
  }

  const due = await listDueNorthCarolinaCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  const directByYearAndCommittee = new Map<string, DirectAggregationForCommittee>();
  const directLoadErrors = new Map<string, string>();
  const outsideByYear = new Map<number, OutsideAggregationForYear>();

  for (const [year, yearRows] of groupDueRowsByYear(due.rows).entries()) {
    // Per-committee aggregation, isolated per committee — NC artifacts are
    // per committee, so one broken cache entry never fails the year.
    const committeeIds = new Set(yearRows.map((row) => row.committeeId.trim().toUpperCase()));
    for (const committeeId of committeeIds) {
      const key = `${year}\u0000${committeeId}`;
      try {
        directByYearAndCommittee.set(
          key,
          await aggregateDirectForCommittee({ cacheDir, electionYear: year, sboeId: committeeId })
        );
      } catch (error) {
        directLoadErrors.set(key, errorMessage(error));
      }
    }

    outsideByYear.set(
      year,
      await aggregateOutsideForYear({ db: input.db, cacheDir, electionYear: year, dueRows: yearRows })
    );
  }

  const results: NorthCarolinaCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    const committeeId = row.committeeId.trim().toUpperCase();
    const base = {
      candidateId: row.candidateId,
      electionId: row.electionId,
      electionYear: row.electionYear,
      committeeId: row.committeeId,
    };
    const directKey = `${row.electionYear}\u0000${committeeId}`;

    const directLoadError = directLoadErrors.get(directKey);
    if (directLoadError) {
      results.push({ ...base, ok: false, error: directLoadError });
      continue;
    }
    const direct = directByYearAndCommittee.get(directKey);
    if (!direct) {
      results.push({ ...base, ok: false, error: "North Carolina direct finance aggregation missing for committee" });
      continue;
    }
    // The three-status contract's no-write leg, decided here so the item
    // error can carry the read failures that explain WHY reports are missing.
    if (direct.result.status === "incomplete_artifacts") {
      const details = [
        ...direct.result.missingReportIds.map((reportId) => {
          const failure = direct.reportReadFailures.find((entry) => entry.reportId === reportId);
          return failure ? `report ${reportId}: ${failure.message}` : `report ${reportId}: not cached`;
        }),
        ...direct.result.coverPeriodMismatchReportIds.map((reportId) => `report ${reportId}: mispaired cover`),
      ];
      results.push({
        ...base,
        ok: false,
        error:
          "North Carolina finance artifacts incomplete; keeping previous snapshot " +
          `(run north-carolina-candidates:finance:raw:refresh): ${details.join("; ")}`,
      });
      continue;
    }

    const outside = outsideByYear.get(row.electionYear);
    const outsideFinance =
      outside?.byTargetKey?.get(outsideTargetKey(row)) ??
      (outside?.byTargetKey ? { supportTotal: 0, opposeTotal: 0, groups: [] } : null);
    const ieInverseMissSuspected =
      direct.result.ieTypedRegularReportRowCount > 0 &&
      outside?.byTargetKey != null &&
      !outside.aggregatedIeFilerKeys.has(committeeId);

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
          linkSource: row.linkSource,
          sourceUrl: row.sourceUrl,
        },
        directFinance: direct.result,
        outsideFinance,
        now,
        dryRun,
      });
      results.push({ ...base, ok: true, ...(ieInverseMissSuspected ? { ieInverseMissSuspected } : {}), result });
      if (ieInverseMissSuspected) {
        console.warn(
          "North Carolina finance inverse-miss suspect: committee has IE-typed regular-report rows but no " +
            "aggregated IE-inventory report (decision 3 cross-check):",
          { committeeId: row.committeeId, candidateId: row.candidateId, electionYear: row.electionYear }
        );
      }
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
