import type { Pool, PoolClient } from "pg";

import {
  getAllNewHampshireFilingEntities,
  getAllNewHampshireIndependentExpenditures,
  getAllNewHampshireReceipts,
  type NewHampshireCfsClientOptions,
  type NewHampshireFilingEntityRow,
  type NewHampshireIndependentExpenditureRow,
  type NewHampshireReceiptRow,
} from "./newHampshireCfsClient.js";
import {
  normalizeNewHampshireCandidateNameForStorage,
  resolveNewHampshireCandidateFiler,
  type NewHampshireCandidateFilerResolution,
} from "./newHampshireCandidateFilerResolver.js";
import {
  aggregateNewHampshireDirectContributions,
  type NewHampshireDirectContributionAggregationResult,
} from "./newHampshireDirectContributionAggregator.js";
import {
  replaceNewHampshireCandidateFinanceSnapshot,
  type NewHampshireFinanceSnapshotWriteResult,
} from "./newHampshireFinanceWriter.js";
import {
  aggregateNewHampshireOutsideSpending,
  type NewHampshireOutsideSpendingAggregationResult,
} from "./newHampshireOutsideSpendingAggregator.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type NewHampshireCfsDataClient = {
  getFilingEntities: (
    input: { electionCycleId: number },
    options?: NewHampshireCfsClientOptions
  ) => Promise<NewHampshireFilingEntityRow[]>;
  getReceipts: (
    input: { filerName: string; electionCycleId: number },
    options?: NewHampshireCfsClientOptions
  ) => Promise<NewHampshireReceiptRow[]>;
  getIndependentExpenditures: (
    input: { electionCycleId: number },
    options?: NewHampshireCfsClientOptions
  ) => Promise<NewHampshireIndependentExpenditureRow[]>;
};

export type NewHampshireCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionCycleId: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  sourceUrl?: string | null;
  cfsClientOptions?: NewHampshireCfsClientOptions;
  cfsClient?: Partial<NewHampshireCfsDataClient>;
  now?: Date;
  dryRun?: boolean;
  maxOutsideGroups?: number;
};

export type NewHampshireCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  electionCycleId: number;
  dryRun: boolean;
  resolution: NewHampshireCandidateFilerResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  directAggregation: NewHampshireDirectContributionAggregationResult | null;
  outsideAggregation: NewHampshireOutsideSpendingAggregationResult | null;
  directSkippedReason: string | null;
  outsideSkippedReason: string | null;
};

const NEW_HAMPSHIRE_CFS_PUBLIC_URL = "https://cfs.sos.nh.gov/";

const DEFAULT_CFS_CLIENT: NewHampshireCfsDataClient = {
  getFilingEntities: getAllNewHampshireFilingEntities,
  getReceipts: getAllNewHampshireReceipts,
  getIndependentExpenditures: getAllNewHampshireIndependentExpenditures,
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new Error(`${fieldName} is required`);
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2016 || value > 2100) {
    throw new Error(`Invalid New Hampshire finance sync election year: ${value}`);
  }
  return value;
}

function normalizeElectionCycleId(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid New Hampshire finance sync election-cycle ID: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid New Hampshire finance sync timestamp");
  }
  return normalized;
}

function normalizeMaxOutsideGroups(value: number | undefined): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid New Hampshire finance sync maxOutsideGroups: ${value}`);
  }
  return value;
}

function mergeCfsClient(client: Partial<NewHampshireCfsDataClient> | undefined): NewHampshireCfsDataClient {
  return { ...DEFAULT_CFS_CLIENT, ...(client ?? {}) };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function validateFilingEntityCycle(input: {
  rows: readonly NewHampshireFilingEntityRow[];
  electionCycleId: number;
  electionYear: number;
}): void {
  const expectedCycle = `${input.electionYear} Election Cycle`;
  for (const row of input.rows) {
    if (row.electionCycleId !== input.electionCycleId) {
      throw new Error(
        `New Hampshire filing-entity search was not exact: expected cycle ID ${input.electionCycleId}, ` +
          `received ${row.electionCycleId}`
      );
    }
    if (row.electionYear !== input.electionYear || row.electionCycle !== expectedCycle) {
      throw new Error(
        `New Hampshire election-cycle ID ${input.electionCycleId} does not match ` +
          `${expectedCycle}: received ${row.electionCycle}`
      );
    }
  }
}

function unresolvedResult(input: {
  candidateId: string;
  electionId: string;
  electionYear: number;
  electionCycleId: number;
  dryRun: boolean;
  resolution: NewHampshireCandidateFilerResolution;
}): NewHampshireCandidateFinanceSyncResult {
  return {
    ...input,
    linkWritten: false,
    summaryWritten: false,
    directBreakdownsWritten: 0,
    outsideGroupsWritten: 0,
    totalReceipts: null,
    directContributionTotal: null,
    outsideSupportTotal: null,
    outsideOpposeTotal: null,
    directAggregation: null,
    outsideAggregation: null,
    directSkippedReason: null,
    outsideSkippedReason: null,
  };
}

export async function syncNewHampshireCandidateFinance(
  input: NewHampshireCandidateFinanceSyncInput
): Promise<NewHampshireCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const electionCycleId = normalizeElectionCycleId(input.electionCycleId);
  const now = normalizeTimestamp(input.now);
  const maxOutsideGroups = normalizeMaxOutsideGroups(input.maxOutsideGroups);
  const dryRun = input.dryRun === true;
  const sourceUrl = input.sourceUrl === undefined || input.sourceUrl === null
    ? NEW_HAMPSHIRE_CFS_PUBLIC_URL
    : requireNonEmpty(input.sourceUrl, "New Hampshire finance source URL");
  const cfsClient = mergeCfsClient(input.cfsClient);

  const filingEntityRows = await cfsClient.getFilingEntities(
    { electionCycleId },
    input.cfsClientOptions
  );
  validateFilingEntityCycle({ rows: filingEntityRows, electionCycleId, electionYear });
  const resolution = resolveNewHampshireCandidateFiler({
    candidateName,
    officeScope,
    officeName,
    district: input.district,
    electionCycleId,
    filingEntityRows,
    sourceUrl,
  });
  if (resolution.status !== "matched") {
    return unresolvedResult({
      candidateId,
      electionId,
      electionYear,
      electionCycleId,
      dryRun,
      resolution,
    });
  }

  let directAggregation: NewHampshireDirectContributionAggregationResult | null = null;
  let directSkippedReason: string | null = null;
  try {
    const receiptRows = await cfsClient.getReceipts(
      { filerName: resolution.filerName, electionCycleId },
      input.cfsClientOptions
    );
    directAggregation = aggregateNewHampshireDirectContributions({
      filingEntityId: resolution.filingEntityId,
      electionYear,
      receiptRows,
      sourceUrl,
    });
  } catch (error) {
    directSkippedReason = errorMessage(error);
  }

  let outsideAggregation: NewHampshireOutsideSpendingAggregationResult | null = null;
  let outsideSkippedReason: string | null = null;
  try {
    const expenditureRows = await cfsClient.getIndependentExpenditures(
      { electionCycleId },
      input.cfsClientOptions
    );
    outsideAggregation = aggregateNewHampshireOutsideSpending({
      candidateAliases: resolution.candidateAliases,
      electionYear,
      expenditureRows,
      sourceUrl,
      maxGroups: maxOutsideGroups,
    });
  } catch (error) {
    outsideSkippedReason = errorMessage(error);
  }

  const totalReceipts = directAggregation?.summary.totalReceipts ?? null;
  const directContributionTotal = directAggregation?.summary.directContributionTotal ?? null;
  const outsideSupportTotal = outsideAggregation
    ? outsideAggregation.summary?.supportTotal ?? 0
    : null;
  const outsideOpposeTotal = outsideAggregation
    ? outsideAggregation.summary?.opposeTotal ?? 0
    : null;

  let write: NewHampshireFinanceSnapshotWriteResult | null = null;
  if (!dryRun && (directAggregation !== null || outsideAggregation !== null)) {
    write = await replaceNewHampshireCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId,
        electionId,
        electionYear,
        candidateNameNormalized: normalizeNewHampshireCandidateNameForStorage(candidateName),
        officeName: resolution.officeName,
        district: resolution.district,
        filingEntityId: resolution.filingEntityId,
        filerName: resolution.filerName,
        linkStatus: "active",
        linkSource: resolution.source,
        sourceUrl: resolution.sourceUrl,
        lastVerifiedAt: now,
      },
      syncedAt: now,
      summary: {
        totalReceipts,
        directContributionTotal,
        outsideSupportTotal,
        outsideOpposeTotal,
        sourceUrl,
      },
      directBreakdowns: directAggregation?.directBreakdowns,
      outsideGroups: outsideAggregation
        ? (outsideAggregation.summary?.groups ?? []).map((group) => ({
            filingEntityId: group.filerEntityId,
            filerName: group.filerName,
            supportOppose: group.supportOppose,
            amount: group.amount,
            sourceUrl: group.sourceUrl,
          }))
        : undefined,
    });
  }

  return {
    candidateId,
    electionId,
    electionYear,
    electionCycleId,
    dryRun,
    resolution,
    linkWritten: write !== null,
    summaryWritten: write?.summaryWritten ?? false,
    directBreakdownsWritten: write?.directBreakdownsWritten ?? 0,
    outsideGroupsWritten: write?.outsideGroupsWritten ?? 0,
    totalReceipts,
    directContributionTotal,
    outsideSupportTotal,
    outsideOpposeTotal,
    directAggregation,
    outsideAggregation,
    directSkippedReason,
    outsideSkippedReason,
  };
}
