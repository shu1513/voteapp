import type { Pool, PoolClient } from "pg";

import {
  type CaliforniaIndependentSpendingSummary,
  type CaliforniaPowerSearchClientOptions,
  summarizeCaliforniaIndependentSpendingByCandidate,
} from "./californiaPowerSearchClient.js";
import {
  type CaliforniaFinanceLinkInput,
  replaceCaliforniaCandidateFinanceSnapshot,
} from "./californiaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type CaliforniaCandidateFinanceSyncInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  controlledCommitteeId: string;
  controlledCommitteeName: string;
  sourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  includeOutside?: boolean;
  powerSearchOptions?: CaliforniaPowerSearchClientOptions;
  powerSearchClient?: CaliforniaCandidateFinancePowerSearchClient;
};

export type CaliforniaCandidateFinancePowerSearchClient = {
  summarizeIndependentSpendingByCandidate: typeof summarizeCaliforniaIndependentSpendingByCandidate;
};

export type CaliforniaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  outsideIncluded: boolean;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
};

const DEFAULT_POWER_SEARCH_CLIENT: CaliforniaCandidateFinancePowerSearchClient = {
  summarizeIndependentSpendingByCandidate: summarizeCaliforniaIndependentSpendingByCandidate,
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2001 || value > 2100) {
    throw new Error(`Invalid California finance election year: ${value}`);
  }
  return value;
}

function normalizeCandidateNameForStorage(value: string): string {
  return requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid California finance sync timestamp");
  }
  return normalized;
}

function toFinanceLink(input: CaliforniaCandidateFinanceSyncInput): CaliforniaFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: normalizeElectionYear(input.electionYear),
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    controlledCommitteeId: requireNonEmpty(input.controlledCommitteeId, "California controlled committee id"),
    controlledCommitteeName: requireNonEmpty(input.controlledCommitteeName, "California controlled committee name"),
    linkSource: "manual",
    sourceUrl: input.sourceUrl ?? null,
    lastVerifiedAt: input.now ?? null,
  };
}

function toOutsideGroups(summary: CaliforniaIndependentSpendingSummary) {
  return summary.groups.map((group) => ({
    committeeId: group.expenderId,
    committeeName: group.expenderName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl,
  }));
}

export async function syncCaliforniaCandidateFinance(
  input: CaliforniaCandidateFinanceSyncInput
): Promise<CaliforniaCandidateFinanceSyncResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const includeOutside = input.includeOutside !== false;
  const powerSearchClient = input.powerSearchClient ?? DEFAULT_POWER_SEARCH_CLIENT;
  const link = toFinanceLink({ ...input, electionYear, now: syncedAt });

  const outsideSummary = includeOutside
    ? await powerSearchClient.summarizeIndependentSpendingByCandidate(
        { candidateName: input.candidateName, electionYear },
        input.powerSearchOptions ?? {}
      )
    : null;

  if (!input.dryRun) {
    await replaceCaliforniaCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary: outsideSummary
        ? {
            outsideSupportTotal: outsideSummary.supportTotal,
            outsideOpposeTotal: outsideSummary.opposeTotal,
            sourceUrl: outsideSummary.sourceUrl,
          }
        : undefined,
      outsideGroups: outsideSummary ? toOutsideGroups(outsideSummary) : undefined,
    });
  }

  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear,
    dryRun: input.dryRun === true,
    outsideIncluded: includeOutside,
    linkWritten: !input.dryRun,
    summaryWritten: !input.dryRun && outsideSummary !== null,
    directBreakdownsWritten: 0,
    outsideGroupsWritten: input.dryRun ? 0 : outsideSummary?.groups.length ?? 0,
    outsideGroupBreakdownsWritten: 0,
    outsideSupportTotal: outsideSummary?.supportTotal ?? null,
    outsideOpposeTotal: outsideSummary?.opposeTotal ?? null,
  };
}
