// Idaho per-link finance sync (docs/plans/idaho-finance.md, Phase 3).
//
// Live-fetch design (the New Hampshire shape, same Civix CFIS vendor): the
// batch pulls the candidate grid and the all-time IE list ONCE per run and
// hands both to this function per link; this function fetches the link's
// contribution rows (one page by filer name), aggregates, and writes one
// snapshot. There is no artifact-first path: the grid and the IE list are
// whole-dataset pulls, so a per-registration cache cannot feed a sync. The
// per-registration artifact is still stored — as the evidence record of what
// this run used, never as an input.
//
// Presence semantics:
// - the summary is always written from the grid (official per-registration
//   totals) — a registration with no search rows is a $0/$0 registration,
//   not an absent one (the link exists because the grid row exists);
// - contribution rows feed only the breakdowns; coverage vs the grid total
//   is reported (rowCoverage), never enforced (Phase 2a decision);
// - the contribution search is fetched as ONE page and fails closed when
//   the service reports more rows than it served (date-sorted paging is
//   unstable — Phase 2a acquisition rule);
// - outside totals are the aggregator's (0 after a successful run with no
//   rows); they are null — and the prior outside snapshot is preserved by
//   the writer's preserveWhenNull policy — only when the IE list could not
//   be fetched this run;
// - any thrown error writes nothing and preserves the prior snapshot.

import type { Pool, PoolClient } from "pg";

import { IDAHO_CFS_GRID_PAGE_SIZE } from "./idahoCandidateFinanceAutoLink.js";
import {
  getAllIdahoCandidateRegistrations,
  getAllIdahoIndependentExpenditures,
  getIdahoContributionPage,
  idahoRegistrationProfileUrl,
  idahoRegistrationSearchName,
  normalizeIdahoRegistrationGuid,
  type IdahoCandidateRegistrationRow,
  type IdahoCfsClientOptions,
  type IdahoCfsPage,
  type IdahoContributionRow,
  type IdahoIndependentExpenditureRow,
} from "./idahoCfsClient.js";
import {
  aggregateIdahoContributions,
  type IdahoContributionAggregationResult,
  type IdahoRowCoverage,
} from "./idahoContributionAggregator.js";
import { isIdahoFinanceEligibleOffice } from "./idahoFinanceEligibleOffices.js";
import {
  normalizeIdahoCandidateNameForStorage,
  replaceIdahoCandidateFinanceSnapshot,
  type IdahoFinanceLinkSource,
} from "./idahoFinanceWriter.js";
import {
  aggregateIdahoOutsideSpending,
  type IdahoOutsideSpendingAggregationResult,
} from "./idahoOutsideSpendingAggregator.js";
import { selectIdahoRegistrationContributions } from "./idahoPhaseZero.js";
import {
  IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION,
  storeIdahoRegistrationArtifact,
  type IdahoRegistrationArtifactManifest,
} from "./idahoRegistrationArtifactCache.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

// The largest 2026 filer-name result is Little at 4,725 rows (survey
// 2026-09-02); one page of 10,000 covers every filer. Paging the search is
// never attempted: date-sorted pages at 500 duplicate and drop rows.
export const IDAHO_CFS_CONTRIBUTION_PAGE_SIZE = 10_000;
// All-time IE list = 9,897 rows (2026-09-01): one page today; the client
// paginates (with a consistent-total check) if it ever grows past this.
export const IDAHO_CFS_INDEPENDENT_EXPENDITURE_PAGE_SIZE = 10_000;

export class IdahoCandidateFinanceSyncError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IdahoCandidateFinanceSyncError";
  }
}

export type IdahoCfsDataClient = {
  getRegistrations: (
    input: { pageSize: number },
    options?: IdahoCfsClientOptions
  ) => Promise<IdahoCandidateRegistrationRow[]>;
  getContributionPage: (
    input: { filerName: string; pageSize: number },
    options?: IdahoCfsClientOptions
  ) => Promise<IdahoCfsPage<IdahoContributionRow>>;
  getIndependentExpenditures: (
    input: { pageSize: number },
    options?: IdahoCfsClientOptions
  ) => Promise<IdahoIndependentExpenditureRow[]>;
};

const DEFAULT_CFS_CLIENT: IdahoCfsDataClient = {
  getRegistrations: getAllIdahoCandidateRegistrations,
  getContributionPage: getIdahoContributionPage,
  getIndependentExpenditures: getAllIdahoIndependentExpenditures,
};

export function mergeIdahoCfsDataClient(client: Partial<IdahoCfsDataClient> | undefined): IdahoCfsDataClient {
  return { ...DEFAULT_CFS_CLIENT, ...(client ?? {}) };
}

export type IdahoCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  link: {
    registrationGuid: string;
    filerName: string;
    linkSource: IdahoFinanceLinkSource;
    sourceUrl?: string | null;
  };
  /** The whole candidate grid; undefined = fetch it (the batch passes its one pull). */
  registrations?: readonly IdahoCandidateRegistrationRow[];
  /**
   * The all-time IE list; undefined = fetch it; null = the batch already
   * knows the fetch failed this run (skip the outside leg and preserve the
   * prior outside snapshot).
   */
  expenditureRows?: readonly IdahoIndependentExpenditureRow[] | null;
  cfsClient?: Partial<IdahoCfsDataClient>;
  cfsClientOptions?: IdahoCfsClientOptions;
  cacheDir?: string;
  now?: Date;
  dryRun?: boolean;
  maxOutsideGroups?: number;
  storeArtifactFn?: typeof storeIdahoRegistrationArtifact;
  writeSnapshotFn?: typeof replaceIdahoCandidateFinanceSnapshot;
};

export type IdahoCandidateFinanceSyncResult = {
  dryRun: boolean;
  candidateId: string;
  electionId: string;
  registrationGuid: string;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  totalReceipts: number;
  totalDisbursements: number;
  cashOnHand: number;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  rowCoverage: IdahoRowCoverage;
  /** Set when the search rows do not reconcile to the grid total (rowCoverage != exact). */
  directCoverageNote: string | null;
  direct: IdahoContributionAggregationResult;
  outside: IdahoOutsideSpendingAggregationResult | null;
  outsideSkippedReason: string | null;
  artifact: IdahoRegistrationArtifactManifest | null;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new IdahoCandidateFinanceSyncError(`${fieldName} is required`);
  return trimmed;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function formatDollars(value: number): string {
  return `$${value.toFixed(2)}`;
}

function directCoverageNote(direct: IdahoContributionAggregationResult): string | null {
  if (direct.rowCoverage === "exact") return null;
  return (
    `contribution search rows total ${formatDollars(direct.rowTotal)} against the ${formatDollars(direct.gridTotalRaised)} ` +
    `state total (${direct.rowCoverage}); size and source-type breakdowns sum to ${formatDollars(direct.directContributionRowTotal)}`
  );
}

/**
 * The registration's contribution rows: one page by the grid's
 * "First Middle Last" search name, filtered to the registration guid by the
 * caller (the service ignores its guid filter). Fails closed on a partial
 * page — the search cannot be paged reliably.
 */
export async function fetchIdahoRegistrationContributionRows(input: {
  registration: IdahoCandidateRegistrationRow;
  cfsClient: IdahoCfsDataClient;
  cfsClientOptions?: IdahoCfsClientOptions;
}): Promise<IdahoContributionRow[]> {
  const filerName = idahoRegistrationSearchName(input.registration);
  const page = await input.cfsClient.getContributionPage(
    { filerName, pageSize: IDAHO_CFS_CONTRIBUTION_PAGE_SIZE },
    input.cfsClientOptions
  );
  if (page.totalItems > page.items.length) {
    throw new IdahoCandidateFinanceSyncError(
      `Idaho contribution search for ${JSON.stringify(filerName)} served ${page.items.length} of ${page.totalItems} rows; ` +
        "paging is unstable, refusing a partial result"
    );
  }
  return page.items;
}

export async function syncIdahoCandidateFinance(
  input: IdahoCandidateFinanceSyncInput
): Promise<IdahoCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const filerName = requireNonEmpty(input.link.filerName, "link filer name");
  if (!isIdahoFinanceEligibleOffice({ officeScope: input.officeScope, officeCanonicalName: officeName })) {
    throw new IdahoCandidateFinanceSyncError(
      `office ${input.officeScope}::${officeName} is not Idaho-finance eligible`
    );
  }
  if (!Number.isSafeInteger(input.electionYear) || input.electionYear < 2026 || input.electionYear > 2100) {
    throw new IdahoCandidateFinanceSyncError(`invalid Idaho finance election year: ${input.electionYear}`);
  }
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new IdahoCandidateFinanceSyncError("invalid Idaho finance sync timestamp");
  }
  const dryRun = input.dryRun === true;
  const registrationGuid = normalizeIdahoRegistrationGuid(input.link.registrationGuid);
  const cfsClient = mergeIdahoCfsDataClient(input.cfsClient);

  // Grid row: present, and on the link's cycle (validation gate).
  const registrations =
    input.registrations ??
    (await cfsClient.getRegistrations({ pageSize: IDAHO_CFS_GRID_PAGE_SIZE }, input.cfsClientOptions));
  const registration = registrations.find(
    (row) => normalizeIdahoRegistrationGuid(row.registrationGuid) === registrationGuid
  );
  if (registration === undefined) {
    throw new IdahoCandidateFinanceSyncError(`Idaho registration ${registrationGuid} is not in the candidate grid`);
  }
  if (registration.electionYear !== input.electionYear) {
    throw new IdahoCandidateFinanceSyncError(
      `Idaho registration ${registrationGuid} is for election year ${registration.electionYear}, link is ${input.electionYear}`
    );
  }

  // Direct leg: grid totals + one-page search rows for the breakdowns.
  const contributionRows = await fetchIdahoRegistrationContributionRows({
    registration,
    cfsClient,
    cfsClientOptions: input.cfsClientOptions,
  });
  const direct = aggregateIdahoContributions({ registration, contributionRows });

  // Outside leg: the all-time IE list, selected by entity + office + window.
  let expenditureRows = input.expenditureRows;
  let outsideSkippedReason: string | null = null;
  if (expenditureRows === null) {
    outsideSkippedReason = "independent expenditure list unavailable this run";
  } else if (expenditureRows === undefined) {
    try {
      expenditureRows = await cfsClient.getIndependentExpenditures(
        { pageSize: IDAHO_CFS_INDEPENDENT_EXPENDITURE_PAGE_SIZE },
        input.cfsClientOptions
      );
    } catch (error) {
      expenditureRows = null;
      outsideSkippedReason = errorMessage(error);
    }
  }
  const outside =
    expenditureRows === null
      ? null
      : aggregateIdahoOutsideSpending({
          registration,
          registrations,
          expenditureRows,
          maxGroups: input.maxOutsideGroups,
        });

  const profileUrl = idahoRegistrationProfileUrl(registrationGuid);
  let artifact: IdahoRegistrationArtifactManifest | null = null;
  let summaryWritten = false;
  let directBreakdownsWritten = 0;
  let outsideGroupsWritten = 0;
  if (!dryRun) {
    // Evidence first: the registration's own rows (the cache contract admits
    // only rows keyed to this guid, so IE rows on the entity's prior
    // registrations — counted by the aggregator — are not in this file).
    const store = input.storeArtifactFn ?? storeIdahoRegistrationArtifact;
    artifact = await store({
      cacheDir: input.cacheDir,
      registrationGuid,
      artifact: {
        version: IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION,
        registration,
        contributions: selectIdahoRegistrationContributions(contributionRows, registrationGuid),
        independentExpenditures: (expenditureRows ?? []).filter(
          (row) =>
            row.candidateMeasureFilerRegistrationGuid !== null &&
            normalizeIdahoRegistrationGuid(row.candidateMeasureFilerRegistrationGuid) === registrationGuid
        ),
      },
      sourceUrl: profileUrl,
      retrievedAt: now,
    });

    const write = input.writeSnapshotFn ?? replaceIdahoCandidateFinanceSnapshot;
    const writeResult = await write({
      db: input.db,
      link: {
        candidateId,
        electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeIdahoCandidateNameForStorage(candidateName),
        officeName,
        district: input.district ?? null,
        registrationGuid,
        filerName,
        linkStatus: "active",
        linkSource: input.link.linkSource,
        sourceUrl: input.link.sourceUrl ?? profileUrl,
        lastVerifiedAt: now,
      },
      syncedAt: now,
      summary: {
        totalReceipts: direct.summary.totalReceipts,
        directContributionTotal: direct.summary.directContributionTotal,
        totalDisbursements: direct.summary.totalDisbursements,
        cashOnHand: direct.summary.cashOnHand,
        outsideSupportTotal: outside?.summary.supportTotal ?? null,
        outsideOpposeTotal: outside?.summary.opposeTotal ?? null,
        sourceUrl: direct.summary.sourceUrl,
      },
      directBreakdowns: direct.directBreakdowns,
      outsideGroups: outside?.summary.groups.map((group) => ({
        filerKey: group.filerKey,
        filerName: group.filerName,
        supportOppose: group.supportOppose,
        amount: group.amount,
        sourceUrl: group.sourceUrl,
      })),
    });
    summaryWritten = writeResult.summaryWritten;
    directBreakdownsWritten = writeResult.directBreakdownsWritten;
    outsideGroupsWritten = writeResult.outsideGroupsWritten;
  }

  return {
    dryRun,
    candidateId,
    electionId,
    registrationGuid,
    summaryWritten,
    directBreakdownsWritten,
    outsideGroupsWritten,
    totalReceipts: direct.summary.totalReceipts,
    totalDisbursements: direct.summary.totalDisbursements,
    cashOnHand: direct.summary.cashOnHand,
    outsideSupportTotal: outside?.summary.supportTotal ?? null,
    outsideOpposeTotal: outside?.summary.opposeTotal ?? null,
    rowCoverage: direct.rowCoverage,
    directCoverageNote: directCoverageNote(direct),
    direct,
    outside,
    outsideSkippedReason,
    artifact,
  };
}
