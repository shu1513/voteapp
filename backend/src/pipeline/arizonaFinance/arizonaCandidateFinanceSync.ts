import type { Pool, PoolClient } from "pg";

import {
  buildArizonaCandidateFinanceSnapshot,
  type ArizonaCandidateFinanceSnapshot,
  type ArizonaCandidateFinanceSnapshotClient,
} from "./arizonaCandidateFinanceSnapshot.js";
import {
  normalizeArizonaCandidateNameForStorage,
  resolveArizonaCandidateCommittee,
  type ArizonaCandidateCommitteeResolution,
} from "./arizonaCandidateCommitteeResolver.js";
import type { ArizonaSpotlightClientOptions } from "./arizonaSpotlightClient.js";
import {
  replaceArizonaCandidateFinanceSnapshot,
  type ArizonaFinanceDirectBreakdownInput,
  type ArizonaFinanceLinkInput,
  type ArizonaFinanceOutsideGroupBreakdownInput,
  type ArizonaFinanceOutsideGroupInput,
  type ArizonaFinanceSummaryInput,
} from "./arizonaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type ArizonaCandidateFinanceSyncInput = {
  db: Queryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  sourceUrl?: string | null;
  spotlightClientOptions?: ArizonaSpotlightClientOptions;
  spotlightClient?: Partial<ArizonaCandidateFinanceSnapshotClient>;
  now?: Date;
  dryRun?: boolean;
  directIncomeLimit?: number;
  independentExpenditureLimitPerPosition?: number;
  outsideGroupIncomeLimitPerGroup?: number;
  outsideMaxGroups?: number;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
  trustedCommittee?: {
    committeeId: string;
    committeeName: string;
    candidateFilerId?: string | null;
    sourceUrl?: string | null;
  };
};

type MatchedArizonaCommitteeResolution = Extract<ArizonaCandidateCommitteeResolution, { status: "matched" }>;

export type ArizonaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution:
    | ArizonaCandidateCommitteeResolution
    | ({
        status: "matched";
      } & MatchedArizonaCommitteeResolution);
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  matchedIncomeTransactionCount: number;
  includedIncomeTransactionCount: number;
  skippedIncomeTransactionCount: number;
  matchedIndependentExpenditureCount: number;
  includedIndependentExpenditureCount: number;
  skippedIndependentExpenditureCount: number;
  matchedOutsideIncomeTransactionCount: number;
  includedOutsideIncomeTransactionCount: number;
  skippedOutsideIncomeTransactionCount: number;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2002 || value > 2100) {
    throw new Error(`Invalid Arizona finance election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Arizona finance sync timestamp");
  }
  return normalized;
}

function trustedCommitteeResolution(
  input: NonNullable<ArizonaCandidateFinanceSyncInput["trustedCommittee"]>
): MatchedArizonaCommitteeResolution {
  return {
    status: "matched",
    committeeId: requireNonEmpty(input.committeeId, "trusted Arizona committee id"),
    committeeName: requireNonEmpty(input.committeeName, "trusted Arizona committee name"),
    confidence: "single_committee",
    source: "spotlight",
    sourceUrl: input.sourceUrl ?? null,
    matchedIncomeRowCount: 0,
    totalIncomeAmount: 0,
  };
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  resolution: MatchedArizonaCommitteeResolution;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): ArizonaFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeArizonaCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    committeeId: requireNonEmpty(input.resolution.committeeId, "Arizona committee id"),
    committeeName: requireNonEmpty(input.resolution.committeeName, "Arizona committee name"),
    linkStatus: "active",
    linkSource: "spotlight",
    sourceUrl: input.resolution.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function toSummary(snapshot: ArizonaCandidateFinanceSnapshot): ArizonaFinanceSummaryInput {
  return {
    totalReceipts: snapshot.directFinance.summary.totalReceipts,
    directContributionTotal: snapshot.directFinance.summary.directContributionTotal,
    totalDisbursements: null,
    cashOnHand: null,
    outsideSupportTotal: snapshot.outsideSpending.summary?.supportTotal ?? null,
    outsideOpposeTotal: snapshot.outsideSpending.summary?.opposeTotal ?? null,
    sourceUrl: snapshot.directFinance.summary.sourceUrl ?? snapshot.outsideSpending.summary?.sourceUrl ?? null,
  };
}

function toDirectBreakdowns(snapshot: ArizonaCandidateFinanceSnapshot): ArizonaFinanceDirectBreakdownInput[] {
  return snapshot.directFinance.directBreakdowns.map((breakdown) => ({
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount,
    sourceUrl: breakdown.sourceUrl,
  }));
}

function toOutsideGroups(snapshot: ArizonaCandidateFinanceSnapshot): ArizonaFinanceOutsideGroupInput[] {
  return (snapshot.outsideSpending.summary?.groups ?? []).map((group) => ({
    committeeId: group.committeeId,
    committeeName: group.committeeName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl,
  }));
}

function toOutsideGroupBreakdowns(snapshot: ArizonaCandidateFinanceSnapshot): ArizonaFinanceOutsideGroupBreakdownInput[] {
  return snapshot.outsideGroupContributions.outsideGroupBreakdowns.map((breakdown) => ({
    committeeId: breakdown.committeeId,
    supportOppose: breakdown.supportOppose,
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount,
    sourceUrl: breakdown.sourceUrl,
  }));
}

function emptyResult(input: {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: ArizonaCandidateCommitteeResolution;
}): ArizonaCandidateFinanceSyncResult {
  return {
    candidateId: input.candidateId,
    electionId: input.electionId,
    electionYear: input.electionYear,
    dryRun: input.dryRun,
    resolution: input.resolution,
    linkWritten: false,
    summaryWritten: false,
    directBreakdownsWritten: 0,
    outsideGroupsWritten: 0,
    outsideGroupBreakdownsWritten: 0,
    totalReceipts: null,
    directContributionTotal: null,
    outsideSupportTotal: null,
    outsideOpposeTotal: null,
    matchedIncomeTransactionCount: 0,
    includedIncomeTransactionCount: 0,
    skippedIncomeTransactionCount: 0,
    matchedIndependentExpenditureCount: 0,
    includedIndependentExpenditureCount: 0,
    skippedIndependentExpenditureCount: 0,
    matchedOutsideIncomeTransactionCount: 0,
    includedOutsideIncomeTransactionCount: 0,
    skippedOutsideIncomeTransactionCount: 0,
  };
}

export async function syncArizonaCandidateFinance(
  input: ArizonaCandidateFinanceSyncInput
): Promise<ArizonaCandidateFinanceSyncResult> {
  const now = normalizeTimestamp(input.now);
  const electionYear = normalizeElectionYear(input.electionYear);
  const dryRun = input.dryRun === true;
  const resolution = input.trustedCommittee
    ? trustedCommitteeResolution(input.trustedCommittee)
    : await resolveArizonaCandidateCommittee(
        {
          candidateName: input.candidateName,
          officeScope: input.officeScope,
          officeName: input.officeName,
          electionYear,
          district: input.district,
        },
        input.spotlightClientOptions,
        input.spotlightClient
          ? {
              searchCandidateCommittees: async (lookupInput, options) => {
                const rows = await input.spotlightClient?.searchIncomeTransactions?.(
                  { electionYear: lookupInput.electionYear, filerName: lookupInput.candidateName, limit: lookupInput.limit },
                  options
                );
                if (!rows) {
                  return [];
                }
                const matches = new Map<string, { committeeId: string; committeeName: string; amount: number; rowCount: number; sourceUrl: string | null }>();
                for (const row of rows) {
                  const existing = matches.get(row.committeeId);
                  if (!existing) {
                    matches.set(row.committeeId, {
                      committeeId: row.committeeId,
                      committeeName: row.committeeName,
                      amount: row.amount,
                      rowCount: 1,
                      sourceUrl: row.sourceUrl,
                    });
                  } else {
                    existing.amount = Math.round((existing.amount + row.amount) * 100) / 100;
                    existing.rowCount += 1;
                  }
                }
                return [...matches.values()];
              },
            }
          : undefined
      );

  if (resolution.status !== "matched") {
    return emptyResult({
      candidateId: input.candidateId,
      electionId: input.electionId,
      electionYear,
      dryRun,
      resolution,
    });
  }

  const snapshot = await buildArizonaCandidateFinanceSnapshot({
    candidateName: input.candidateName,
    candidateCommitteeId: resolution.committeeId,
    candidateFilerId: input.trustedCommittee?.candidateFilerId ?? resolution.committeeId,
    electionYear,
    spotlightClientOptions: input.spotlightClientOptions,
    spotlightClient: input.spotlightClient,
    directIncomeLimit: input.directIncomeLimit,
    independentExpenditureLimitPerPosition: input.independentExpenditureLimitPerPosition,
    outsideGroupIncomeLimitPerGroup: input.outsideGroupIncomeLimitPerGroup,
    outsideMaxGroups: input.outsideMaxGroups,
    directMaxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory,
    outsideMaxBreakdownsPerCategory: input.outsideMaxBreakdownsPerCategory,
    minIndustryAmount: input.minIndustryAmount,
  });

  const summary = toSummary(snapshot);
  const directBreakdowns = toDirectBreakdowns(snapshot);
  const outsideGroups = toOutsideGroups(snapshot);
  const outsideGroupBreakdowns = toOutsideGroupBreakdowns(snapshot);
  let writeResult = {
    linkId: "",
    summaryWritten: false,
    directBreakdownsWritten: 0,
    outsideGroupsWritten: 0,
    outsideGroupBreakdownsWritten: 0,
  };

  if (!dryRun) {
    writeResult = await replaceArizonaCandidateFinanceSnapshot({
      db: input.db,
      link: toFinanceLink({
        candidateId: input.candidateId,
        electionId: input.electionId,
        candidateName: input.candidateName,
        electionYear,
        officeName: input.officeName,
        district: input.district,
        resolution,
        sourceUrl: input.sourceUrl,
        verifiedAt: now,
      }),
      syncedAt: now,
      summary,
      directBreakdowns,
      outsideGroups,
      outsideGroupBreakdowns,
    });
  }

  return {
    candidateId: input.candidateId,
    electionId: input.electionId,
    electionYear,
    dryRun,
    resolution,
    linkWritten: !dryRun,
    summaryWritten: dryRun ? false : writeResult.summaryWritten,
    directBreakdownsWritten: dryRun ? 0 : writeResult.directBreakdownsWritten,
    outsideGroupsWritten: dryRun ? 0 : writeResult.outsideGroupsWritten,
    outsideGroupBreakdownsWritten: dryRun ? 0 : writeResult.outsideGroupBreakdownsWritten,
    totalReceipts: summary.totalReceipts ?? null,
    directContributionTotal: summary.directContributionTotal ?? null,
    outsideSupportTotal: summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? null,
    matchedIncomeTransactionCount: snapshot.directFinance.matchedIncomeTransactionCount,
    includedIncomeTransactionCount: snapshot.directFinance.includedIncomeTransactionCount,
    skippedIncomeTransactionCount: snapshot.directFinance.skippedIncomeTransactionCount,
    matchedIndependentExpenditureCount: snapshot.outsideSpending.matchedIndependentExpenditureCount,
    includedIndependentExpenditureCount: snapshot.outsideSpending.includedIndependentExpenditureCount,
    skippedIndependentExpenditureCount: snapshot.outsideSpending.skippedIndependentExpenditureCount,
    matchedOutsideIncomeTransactionCount: snapshot.outsideGroupContributions.matchedIncomeTransactionCount,
    includedOutsideIncomeTransactionCount: snapshot.outsideGroupContributions.includedIncomeTransactionCount,
    skippedOutsideIncomeTransactionCount: snapshot.outsideGroupContributions.skippedIncomeTransactionCount,
  };
}
