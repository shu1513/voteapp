import type { Pool, PoolClient } from "pg";

import type { LouisianaCampaignFinanceCsvRow } from "./louisianaCampaignFinanceArtifactReader.js";
import {
  normalizeLouisianaCandidateNameForStorage,
  resolveLouisianaCandidateCommittee,
  type LouisianaCandidateCommitteeMatch,
  type LouisianaCandidateCommitteeResolution,
} from "./louisianaCandidateCommitteeResolver.js";
import { aggregateLouisianaDirectContributions } from "./louisianaDirectContributionAggregator.js";
import { aggregateLouisianaOutsideGroupContributions } from "./louisianaOutsideGroupContributionAggregator.js";
import { aggregateLouisianaOutsideSupport } from "./louisianaOutsideSupportAggregator.js";
import {
  replaceLouisianaCandidateFinanceSnapshot,
  type LouisianaFinanceLinkInput,
  type LouisianaFinanceSummaryInput,
} from "./louisianaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type LouisianaCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
  expenditureRows?: readonly LouisianaCampaignFinanceCsvRow[];
  sourceUrl?: string | null;
  contributionSourceUrl?: string | null;
  expenditureSourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxBreakdownsPerCategory?: number;
  trustedCommittee?: {
    filerNumber: string;
    filerName: string;
    sourceUrl?: string | null;
  };
};

type MatchedLouisianaCommitteeResolution =
  | ({ status: "matched" } & LouisianaCandidateCommitteeMatch)
  | {
      status: "matched";
      filerNumber: string;
      filerName: string;
      candidateName: string | null;
      officeName: string;
      district: string | null;
      confidence: "exact";
      source: "la_ethics_search";
      sourceUrl: string | null;
      matchedCandidateRowCount: number;
    };

export type LouisianaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: LouisianaCandidateCommitteeResolution | MatchedLouisianaCommitteeResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
  matchedOutsideExpenditureRowCount: number;
  includedOutsideExpenditureRowCount: number;
  skippedOutsideExpenditureRowCount: number;
  matchedOutsideContributionRowCount: number;
  includedOutsideContributionRowCount: number;
  skippedOutsideContributionRowCount: number;
};

const LOUISIANA_CAMPAIGN_FINANCE_SOURCE_URL =
  "https://www.ethics.la.gov/campaignfinancesearch/ShowPremadereports.aspx";

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Louisiana finance election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Louisiana finance sync timestamp");
  }
  return normalized;
}

function resolveTrustedCommittee(input: {
  filerNumber: string;
  filerName: string;
  candidateName: string;
  officeName: string;
  district?: string | null;
  sourceUrl?: string | null;
}): MatchedLouisianaCommitteeResolution {
  return {
    status: "matched",
    filerNumber: requireNonEmpty(input.filerNumber, "trusted Louisiana filer number"),
    filerName: requireNonEmpty(input.filerName, "trusted Louisiana filer name"),
    candidateName: input.candidateName,
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    confidence: "exact",
    source: "la_ethics_search",
    sourceUrl: input.sourceUrl ?? null,
    matchedCandidateRowCount: 0,
  };
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  resolution: MatchedLouisianaCommitteeResolution;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): LouisianaFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeLouisianaCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    filerNumber: requireNonEmpty(input.resolution.filerNumber, "Louisiana filer number"),
    filerName: requireNonEmpty(input.resolution.filerName, "Louisiana filer name"),
    linkStatus: "active",
    linkSource: "la_ethics_search",
    sourceUrl: input.sourceUrl ?? input.resolution.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function mergeSummary(input: {
  directTotal: number;
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  sourceUrl: string | null;
}): LouisianaFinanceSummaryInput {
  return {
    totalReceipts: input.directTotal,
    directContributionTotal: input.directTotal,
    outsideSupportTotal: input.outsideSupportTotal,
    outsideOpposeTotal: input.outsideOpposeTotal,
    sourceUrl: input.sourceUrl,
  };
}

function emptyResult(input: {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: LouisianaCandidateCommitteeResolution;
}): LouisianaCandidateFinanceSyncResult {
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
    matchedContributionRowCount: 0,
    includedContributionRowCount: 0,
    skippedContributionRowCount: 0,
    matchedOutsideExpenditureRowCount: 0,
    includedOutsideExpenditureRowCount: 0,
    skippedOutsideExpenditureRowCount: 0,
    matchedOutsideContributionRowCount: 0,
    includedOutsideContributionRowCount: 0,
    skippedOutsideContributionRowCount: 0,
  };
}

export async function syncLouisianaCandidateFinance(
  input: LouisianaCandidateFinanceSyncInput
): Promise<LouisianaCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;

  const resolution = input.trustedCommittee
    ? resolveTrustedCommittee({
        ...input.trustedCommittee,
        candidateName,
        officeName,
        district: input.district,
      })
    : resolveLouisianaCandidateCommittee({
        candidateName,
        officeScope,
        officeName,
        electionYear,
        district: input.district,
        candidateRows: input.contributionRows,
        sourceUrl: input.sourceUrl ?? input.contributionSourceUrl ?? LOUISIANA_CAMPAIGN_FINANCE_SOURCE_URL,
      });

  if (resolution.status !== "matched") {
    return emptyResult({ candidateId, electionId, electionYear, dryRun, resolution });
  }

  const directFinance = aggregateLouisianaDirectContributions({
    filerNumber: resolution.filerNumber,
    electionYear,
    contributionRows: input.contributionRows,
    sourceUrl: input.contributionSourceUrl ?? input.sourceUrl ?? resolution.sourceUrl ?? null,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory,
  });

  const outsideFinance =
    input.expenditureRows !== undefined
      ? aggregateLouisianaOutsideSupport({
          candidateName,
          candidateFilerName: resolution.filerName,
          electionYear,
          expenditureRows: input.expenditureRows,
          sourceUrl: input.expenditureSourceUrl ?? input.sourceUrl ?? null,
          maxGroups: input.outsideMaxGroups,
        })
      : {
          summary: {
            outsideSupportTotal: 0,
            outsideOpposeTotal: 0,
            sourceUrl: null,
            groups: [],
          },
          matchedExpenditureRowCount: 0,
          includedExpenditureRowCount: 0,
          skippedExpenditureRowCount: 0,
        };

  const outsideGroupFinance = aggregateLouisianaOutsideGroupContributions({
    electionYear,
    outsideGroups: outsideFinance.summary.groups,
    contributionRows: input.contributionRows,
    sourceUrl: input.contributionSourceUrl ?? input.sourceUrl ?? null,
    maxBreakdownsPerCategory: input.outsideMaxBreakdownsPerCategory,
  });

  const summary = mergeSummary({
    directTotal: directFinance.summary.directContributionTotal,
    outsideSupportTotal: outsideFinance.summary.outsideSupportTotal,
    outsideOpposeTotal: outsideFinance.summary.outsideOpposeTotal,
    sourceUrl: input.sourceUrl ?? directFinance.summary.sourceUrl ?? outsideFinance.summary.sourceUrl,
  });

  if (!dryRun) {
    await replaceLouisianaCandidateFinanceSnapshot({
      db: input.db,
      link: toFinanceLink({
        candidateId,
        electionId,
        candidateName,
        electionYear,
        officeName,
        district: input.district,
        resolution,
        sourceUrl: summary.sourceUrl,
        verifiedAt: syncedAt,
      }),
      syncedAt,
      summary,
      directBreakdowns: directFinance.directBreakdowns,
      outsideGroups: outsideFinance.summary.groups,
      outsideGroupBreakdowns: outsideGroupFinance.outsideGroupBreakdowns,
      classifications: outsideGroupFinance.classifications,
    });
  }

  return {
    candidateId,
    electionId,
    electionYear,
    dryRun,
    resolution,
    linkWritten: !dryRun,
    summaryWritten: !dryRun,
    directBreakdownsWritten: dryRun ? 0 : directFinance.directBreakdowns.length,
    outsideGroupsWritten: dryRun ? 0 : outsideFinance.summary.groups.length,
    outsideGroupBreakdownsWritten: dryRun ? 0 : outsideGroupFinance.outsideGroupBreakdowns.length,
    totalReceipts: summary.totalReceipts ?? null,
    directContributionTotal: summary.directContributionTotal ?? null,
    outsideSupportTotal: summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? null,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
    matchedOutsideExpenditureRowCount: outsideFinance.matchedExpenditureRowCount,
    includedOutsideExpenditureRowCount: outsideFinance.includedExpenditureRowCount,
    skippedOutsideExpenditureRowCount: outsideFinance.skippedExpenditureRowCount,
    matchedOutsideContributionRowCount: outsideGroupFinance.matchedContributionRowCount,
    includedOutsideContributionRowCount: outsideGroupFinance.includedContributionRowCount,
    skippedOutsideContributionRowCount: outsideGroupFinance.skippedContributionRowCount,
  };
}
