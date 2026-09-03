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
// - the contribution search and the IE list are each fetched as ONE page
//   and fail closed when the service reports more rows than it served
//   (neither search has a stable sort, so paging can duplicate or drop rows
//   — Phase 2a acquisition rule);
// - outside totals are the aggregator's (0 after a successful run with no
//   rows). There is no partial write: an IE list that cannot be fetched
//   fails the sync, so a summary's last_synced_at always dates BOTH legs;
// - any thrown error writes nothing and preserves the prior snapshot.

import type { Pool, PoolClient } from "pg";

import { IDAHO_CFS_GRID_PAGE_SIZE } from "./idahoCandidateFinanceAutoLink.js";
import {
  getAllIdahoCandidateRegistrations,
  getIdahoContributionPage,
  getIdahoIndependentExpenditurePage,
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
// All-time IE list = 9,899 rows (2026-09-03), growing ~5,000 a year; the
// service returns unsorted rows, so it is never paged either: one page of
// 50,000 (accepted live) with the same fail-closed guard.
export const IDAHO_CFS_INDEPENDENT_EXPENDITURE_PAGE_SIZE = 50_000;

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
  getIndependentExpenditurePage: (
    input: { pageSize: number },
    options?: IdahoCfsClientOptions
  ) => Promise<IdahoCfsPage<IdahoIndependentExpenditureRow>>;
};

const DEFAULT_CFS_CLIENT: IdahoCfsDataClient = {
  getRegistrations: getAllIdahoCandidateRegistrations,
  getContributionPage: getIdahoContributionPage,
  getIndependentExpenditurePage: getIdahoIndependentExpenditurePage,
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
  /** The all-time IE list; undefined = fetch it (the batch passes its one pull). */
  expenditureRows?: readonly IdahoIndependentExpenditureRow[];
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
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  rowCoverage: IdahoRowCoverage;
  /** Set when the search rows do not reconcile to the grid total (rowCoverage != exact). */
  directCoverageNote: string | null;
  direct: IdahoContributionAggregationResult;
  outside: IdahoOutsideSpendingAggregationResult;
  artifact: IdahoRegistrationArtifactManifest | null;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new IdahoCandidateFinanceSyncError(`${fieldName} is required`);
  return trimmed;
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

/** The all-time IE list as ONE page; fails closed on a partial page (unsorted service, never paged). */
export async function fetchIdahoIndependentExpenditureRows(input: {
  cfsClient: IdahoCfsDataClient;
  cfsClientOptions?: IdahoCfsClientOptions;
}): Promise<IdahoIndependentExpenditureRow[]> {
  const page = await input.cfsClient.getIndependentExpenditurePage(
    { pageSize: IDAHO_CFS_INDEPENDENT_EXPENDITURE_PAGE_SIZE },
    input.cfsClientOptions
  );
  if (page.totalItems > page.items.length) {
    throw new IdahoCandidateFinanceSyncError(
      `Idaho independent expenditure list served ${page.items.length} of ${page.totalItems} rows; ` +
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
  const expenditureRows =
    input.expenditureRows ??
    (await fetchIdahoIndependentExpenditureRows({ cfsClient, cfsClientOptions: input.cfsClientOptions }));
  const outside = aggregateIdahoOutsideSpending({
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
    // only rows keyed to this guid; the IE rows on the entity's prior
    // registrations that the aggregator also counts are in the batch's run
    // artifact — storeIdahoRunArtifact).
    const store = input.storeArtifactFn ?? storeIdahoRegistrationArtifact;
    artifact = await store({
      cacheDir: input.cacheDir,
      registrationGuid,
      artifact: {
        version: IDAHO_REGISTRATION_ARTIFACT_SCHEMA_VERSION,
        registration,
        contributions: selectIdahoRegistrationContributions(contributionRows, registrationGuid),
        independentExpenditures: expenditureRows.filter(
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
        outsideSupportTotal: outside.summary.supportTotal,
        outsideOpposeTotal: outside.summary.opposeTotal,
        sourceUrl: direct.summary.sourceUrl,
      },
      directBreakdowns: direct.directBreakdowns,
      outsideGroups: outside.summary.groups.map((group) => ({
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
    outsideSupportTotal: outside.summary.supportTotal,
    outsideOpposeTotal: outside.summary.opposeTotal,
    rowCoverage: direct.rowCoverage,
    directCoverageNote: directCoverageNote(direct),
    direct,
    outside,
    artifact,
  };
}
