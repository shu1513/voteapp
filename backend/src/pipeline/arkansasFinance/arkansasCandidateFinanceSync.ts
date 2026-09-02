// Arkansas per-candidate finance sync (plan-arkansas-finance.md, Phase 3).
// The link is the input (auto-link or an operator wrote it); this locates the
// linked filer's registration for the cycle in the registry sweep, pulls the
// registration-scoped receipts, aggregates, and replaces the snapshot. Any
// failure throws so the prior snapshot survives; only a complete, exact
// pull writes.

import type { Pool, PoolClient } from "pg";

import {
  ARKANSAS_CFIS_PUBLIC_URL,
  getAllArkansasTransactions,
  type ArkansasCfisClientOptions,
  type ArkansasFilerRegistrationRow,
} from "./arkansasCfisClient.js";
import { normalizeArkansasCandidateNameForStorage } from "./arkansasCandidateFilerResolver.js";
import { createArkansasRegistrationSweepLoader } from "./arkansasCandidateFinanceAutoLink.js";
import {
  aggregateArkansasDirectContributions,
  type ArkansasDirectContributionAggregationResult,
} from "./arkansasDirectContributionAggregator.js";
import {
  replaceArkansasCandidateFinanceSnapshot,
  type ArkansasFinanceLinkSource,
  type ArkansasFinanceSnapshotWriteResult,
} from "./arkansasFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

const RECEIPT_PAGE_SIZE = 1_000;

export type ArkansasCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  link: {
    filingEntityId: number;
    filerName: string;
    linkSource: ArkansasFinanceLinkSource;
    sourceUrl?: string | null;
  };
  now?: Date;
  dryRun?: boolean;
  maxOccupationBreakdowns?: number;
  clientOptions?: ArkansasCfisClientOptions;
  /** Memoized registry sweep; a batch shares one across auto-link and every sync. */
  loadRegistrations?: () => Promise<ArkansasFilerRegistrationRow[]>;
  fetchTransactions?: typeof getAllArkansasTransactions;
};

export type ArkansasCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  filingEntityId: number;
  registrationGuid: string;
  receiptRowCount: number;
  aggregation: ArkansasDirectContributionAggregationResult;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new Error(`${fieldName} is required`);
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2026 || value > 2100) {
    throw new Error(`Invalid Arkansas finance sync election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) throw new Error("Invalid Arkansas finance sync timestamp");
  return normalized;
}

/** Exactly one candidate registration for (entity, cycle); anything else fails closed. */
export function selectArkansasCandidateRegistration(
  rows: readonly ArkansasFilerRegistrationRow[],
  filingEntityId: number,
  electionYear: number
): ArkansasFilerRegistrationRow {
  const entityRows = rows.filter((row) => row.filerEntityId === filingEntityId && row.filerTypeCode === "CAN");
  let matches = entityRows.filter((row) => row.electionYear === electionYear);
  if (matches.length === 0) {
    // Some live 2026 registrations carry no electionYear at all (61 of 480
    // legislative candidate rows on 2026-09-02, e.g. Holladay HD70, Teeter
    // HD44, Wilson SD1). The link already pins the entity to this cycle, so
    // the entity's single year-less registration stands in; a registration
    // for a different cycle never does.
    matches = entityRows.filter((row) => row.electionYear === null);
  }
  if (matches.length === 0) {
    throw new Error(`Arkansas CFIS has no candidate registration for entity ${filingEntityId} in the ${electionYear} cycle`);
  }
  if (matches.length > 1) {
    throw new Error(`Arkansas CFIS carries entity ${filingEntityId} ${matches.length} times for the ${electionYear} cycle`);
  }
  return matches[0]!;
}

export async function syncArkansasCandidateFinance(
  input: ArkansasCandidateFinanceSyncInput
): Promise<ArkansasCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const filerName = requireNonEmpty(input.link.filerName, "filer name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const now = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  if (!Number.isSafeInteger(input.link.filingEntityId) || input.link.filingEntityId <= 0) {
    throw new Error(`Invalid Arkansas filing entity ID: ${input.link.filingEntityId}`);
  }

  const loadRegistrations =
    input.loadRegistrations ?? createArkansasRegistrationSweepLoader({ clientOptions: input.clientOptions });
  const registration = selectArkansasCandidateRegistration(
    await loadRegistrations(),
    input.link.filingEntityId,
    electionYear
  );

  const fetchTransactions = input.fetchTransactions ?? getAllArkansasTransactions;
  const receiptRows = await fetchTransactions(
    { filerRegistrationGuid: registration.registrationGuid, transactionTypeCode: "TCON", pageSize: RECEIPT_PAGE_SIZE },
    input.clientOptions
  );

  const aggregation = aggregateArkansasDirectContributions({
    registration,
    receiptRows,
    sourceUrl: ARKANSAS_CFIS_PUBLIC_URL,
    maxOccupationBreakdowns: input.maxOccupationBreakdowns,
  });

  let write: ArkansasFinanceSnapshotWriteResult | null = null;
  if (!dryRun) {
    write = await replaceArkansasCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId,
        electionId,
        electionYear,
        candidateNameNormalized: normalizeArkansasCandidateNameForStorage(candidateName),
        officeName,
        district: input.district,
        filingEntityId: input.link.filingEntityId,
        filerName,
        linkStatus: "active",
        linkSource: input.link.linkSource,
        sourceUrl: input.link.sourceUrl,
        lastVerifiedAt: now,
      },
      syncedAt: now,
      summary: { ...aggregation.summary, sourceUrl: ARKANSAS_CFIS_PUBLIC_URL },
      // [] after an unreconciled pull clears stale breakdowns; the totals
      // above are the state's own figures and publish either way.
      directBreakdowns: aggregation.directBreakdowns,
    });
  }

  return {
    candidateId,
    electionId,
    electionYear,
    dryRun,
    filingEntityId: input.link.filingEntityId,
    registrationGuid: registration.registrationGuid,
    receiptRowCount: receiptRows.length,
    aggregation,
    summaryWritten: write?.summaryWritten ?? false,
    directBreakdownsWritten: write?.directBreakdownsWritten ?? 0,
  };
}
