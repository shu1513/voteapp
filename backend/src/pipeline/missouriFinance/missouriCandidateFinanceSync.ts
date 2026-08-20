import type { Pool, PoolClient } from "pg";

import { aggregateMissouriDirectFinance, type MissouriDirectFinanceAggregationResult } from "./missouriDirectContributionAggregator.js";
import { readMissouriMecCandidateFinanceArtifacts } from "./missouriMecArtifactCache.js";
import { normalizeMissouriCandidateNameForStorage } from "./missouriCandidateCommitteeResolver.js";
import { replaceMissouriCandidateFinanceSnapshot, type MissouriFinanceLinkSource } from "./missouriFinanceWriter.js";
import type { MissouriMecCommitteeInfo, MissouriMecContributionRow, MissouriMecExpenditureRow, MissouriMecReportInventoryRow } from "./missouriMecParsers.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & { connect: () => Promise<PoolClient> };

export type MissouriCandidateFinanceArtifacts = {
  committeeInfo: MissouriMecCommitteeInfo;
  inventory: readonly MissouriMecReportInventoryRow[];
  contributionRows: readonly MissouriMecContributionRow[];
  expenditureRows: readonly MissouriMecExpenditureRow[];
  contributionSourceUrl: string;
  expenditureSourceUrl: string;
};

export type MissouriCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  dryRun: boolean;
  cycleStart: string;
  cycleEnd: string;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  aggregation: MissouriDirectFinanceAggregationResult;
};

function requireText(value: string, label: string): string {
  const normalized = value.trim();
  if (!normalized) throw new Error(`${label} is required`);
  return normalized;
}

function nextIsoDate(value: string): string {
  const date = new Date(`${value}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

export function resolveMissouriCandidateFinanceCycleWindow(input: {
  electionDate: string;
  committeeInfo: MissouriMecCommitteeInfo;
}): { cycleStart: string; cycleEnd: string; primaryElectionDate: string } {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(input.electionDate)) {
    throw new Error(`Invalid Missouri target election date: ${input.electionDate}`);
  }
  const year = input.electionDate.slice(0, 4);
  const targetHistory = input.committeeInfo.electionHistory.find((row) => row.electionDate === input.electionDate);
  if (!targetHistory || !/GENERAL/i.test(targetHistory.electionType)) {
    throw new Error(`Missouri direct-finance v1 requires target general election in MEC history: ${input.electionDate}`);
  }
  const primaries = input.committeeInfo.electionHistory
    .filter((row) => row.electionDate.startsWith(`${year}-`) && row.electionDate < input.electionDate && /PRIMARY/i.test(row.electionType))
    .sort((a, b) => b.electionDate.localeCompare(a.electionDate));
  const primary = primaries[0];
  if (!primary) {
    throw new Error(`Missouri MEC history has no same-year primary boundary before ${input.electionDate}`);
  }
  return { cycleStart: nextIsoDate(primary.electionDate), cycleEnd: input.electionDate, primaryElectionDate: primary.electionDate };
}

export async function syncMissouriCandidateFinance(input: {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionDate: string;
  officeName: string;
  district?: string | null;
  committee: {
    committeeId: string;
    committeeName: string;
    linkSource: MissouriFinanceLinkSource;
    sourceUrl?: string | null;
  };
  cacheDir?: string;
  artifacts?: MissouriCandidateFinanceArtifacts;
  now?: Date;
  dryRun?: boolean;
  maxOccupationBreakdowns?: number;
}): Promise<MissouriCandidateFinanceSyncResult> {
  const candidateId = requireText(input.candidateId, "candidate id");
  const electionId = requireText(input.electionId, "election id");
  const candidateName = requireText(input.candidateName, "candidate name");
  const officeName = requireText(input.officeName, "office name");
  const committeeId = requireText(input.committee.committeeId, "Missouri committee id").toUpperCase();
  if (!/^[A-Z]\d{6}$/.test(committeeId)) throw new Error(`Invalid Missouri MECID: ${committeeId}`);
  if (!Number.isSafeInteger(input.electionYear) || input.electionYear < 2024 || input.electionYear > 2100) {
    throw new Error(`Invalid Missouri finance election year: ${input.electionYear}`);
  }
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) throw new Error("Invalid Missouri finance sync timestamp");
  const artifacts = input.artifacts ?? await readMissouriMecCandidateFinanceArtifacts({
    cacheDir: input.cacheDir,
    mecid: committeeId,
    year: input.electionYear,
  });
  if (artifacts.committeeInfo.mecid !== committeeId) {
    throw new Error(`Missouri MEC artifact mismatch: expected ${committeeId}, got ${artifacts.committeeInfo.mecid}`);
  }
  for (const row of [...artifacts.contributionRows, ...artifacts.expenditureRows]) {
    if (row.mecid !== committeeId) throw new Error(`Missouri MEC export contains unexpected committee ${row.mecid}`);
  }
  const window = resolveMissouriCandidateFinanceCycleWindow({
    electionDate: input.electionDate,
    committeeInfo: artifacts.committeeInfo,
  });
  const sourceUrl = artifacts.contributionSourceUrl || input.committee.sourceUrl || null;
  const aggregation = aggregateMissouriDirectFinance({
    inventory: artifacts.inventory,
    contributionRows: artifacts.contributionRows,
    expenditureRows: artifacts.expenditureRows,
    cycleStart: window.cycleStart,
    cycleEnd: window.cycleEnd,
    sourceUrl,
    maxOccupationBreakdowns: input.maxOccupationBreakdowns,
  });
  const reportDiagnostics = [
    ...aggregation.contributionReportDiagnostics,
    ...aggregation.expenditureReportDiagnostics,
  ];
  if (reportDiagnostics.length > 0) {
    const excludedRowCount = reportDiagnostics.reduce((sum, row) => sum + row.excludedRowCount, 0);
    const excludedAmountCents = reportDiagnostics.reduce((sum, row) => sum + row.excludedAmountCents, 0);
    throw new Error(
      `Missouri MEC in-cycle report lineage is not publishable: lineages=${reportDiagnostics.length}, rows=${excludedRowCount}, signedAmountCents=${excludedAmountCents}`
    );
  }
  if (aggregation.unrecognizedContributionKindRowCount > 0 || aggregation.unrecognizedExpenditureTypeRowCount > 0) {
    throw new Error(
      `Missouri MEC export has unrecognized transaction types: contributions=${aggregation.unrecognizedContributionKindRowCount}, expenditures=${aggregation.unrecognizedExpenditureTypeRowCount}`
    );
  }

  let summaryWritten = false;
  let directBreakdownsWritten = 0;
  if (!input.dryRun) {
    const write = await replaceMissouriCandidateFinanceSnapshot({
      db: input.db,
      link: {
        candidateId,
        electionId,
        electionYear: input.electionYear,
        candidateNameNormalized: normalizeMissouriCandidateNameForStorage(candidateName),
        officeName,
        district: input.district ?? null,
        committeeId,
        committeeName: requireText(input.committee.committeeName, "Missouri committee name"),
        linkStatus: "active",
        linkSource: input.committee.linkSource,
        sourceUrl: input.committee.sourceUrl ?? sourceUrl,
        lastVerifiedAt: now,
      },
      syncedAt: now,
      summary: {
        totalReceipts: null,
        directContributionTotal: aggregation.directContributionTotal,
        totalDisbursements: aggregation.totalDisbursements,
        cashOnHand: null,
        outsideSupportTotal: null,
        outsideOpposeTotal: null,
        sourceUrl,
      },
      directBreakdowns: aggregation.directBreakdowns,
    });
    summaryWritten = write.summaryWritten;
    directBreakdownsWritten = write.directBreakdownsWritten;
  }
  return {
    candidateId,
    electionId,
    electionYear: input.electionYear,
    committeeId,
    dryRun: input.dryRun === true,
    cycleStart: window.cycleStart,
    cycleEnd: window.cycleEnd,
    linkWritten: !input.dryRun,
    summaryWritten,
    directBreakdownsWritten,
    aggregation,
  };
}
