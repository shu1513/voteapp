import type { Pool, PoolClient } from "pg";

import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  normalizeWisconsinCandidateNameKeys,
  searchAndResolveWisconsinCandidateCommittee,
  type WisconsinCandidateCommitteeMatch,
  type WisconsinCandidateCommitteeResolution,
} from "./wisconsinCandidateCommitteeResolver.js";
import { aggregateWisconsinDirectContributions } from "./wisconsinDirectContributionAggregator.js";
import { toWisconsinSunshineOfficeSearchInput } from "./wisconsinFinanceEligibleOffices.js";
import {
  replaceWisconsinCandidateFinanceSnapshot,
  type WisconsinFinanceLinkInput,
  type WisconsinFinanceSummaryInput,
} from "./wisconsinFinanceWriter.js";
import { aggregateWisconsinOutsideSpending } from "./wisconsinOutsideSpendingAggregator.js";
import {
  getWisconsinSunshineContributionSizeAggregates,
  getWisconsinSunshineDirectOccupationAggregates,
  getWisconsinSunshineIndependentExpenditureGroups,
  getWisconsinSunshineOutsideSpenderOrganizationFunders,
  type WisconsinSunshineAggregate,
  type WisconsinSunshineClientOptions,
  type WisconsinSunshineIndependentSpendingGroup,
} from "./wisconsinSunshineClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

type WisconsinSunshineDataClient = {
  searchAndResolveCandidateCommittee: (
    input: {
      candidateName: string;
      officeScope: string;
      officeName: string;
      electionYear: number;
      district?: string | null;
    },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinCandidateCommitteeResolution>;
  getDirectOccupationAggregates: (
    input: { entityId: string | number; electionYear: number; limit?: number },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinSunshineAggregate[]>;
  getContributionSizeAggregates: (
    input: { entityId: string | number; electionYear: number; limit?: number },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinSunshineAggregate[]>;
  getIndependentExpenditureGroups: (
    input: {
      candidateCommitteeName: string;
      electionYear: number;
      office?: string | null;
      district?: string | null;
      limit?: number;
    },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinSunshineIndependentSpendingGroup[]>;
  getOutsideSpenderOrganizationFunders: (
    input: { entityId: string | number; electionYear: number; limit?: number },
    options?: WisconsinSunshineClientOptions
  ) => Promise<WisconsinSunshineAggregate[]>;
};

export type WisconsinCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  sourceUrl?: string | null;
  sunshineClientOptions?: WisconsinSunshineClientOptions;
  sunshineClient?: Partial<WisconsinSunshineDataClient>;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxFundersPerGroup?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  trustedCommittee?: {
    entityId: string;
    committeeId: string;
    committeeName: string;
    assignedCommitteeId?: string | null;
    sourceUrl?: string | null;
  };
};

type MatchedWisconsinCommitteeResolution =
  | Extract<WisconsinCandidateCommitteeResolution, { status: "matched" }>
  | ({ status: "matched" } & WisconsinCandidateCommitteeMatch);

export type WisconsinCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: WisconsinCandidateCommitteeResolution | MatchedWisconsinCommitteeResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  directOccupationRowCount: number;
  directContributionSizeRowCount: number;
  outsideGroupCount: number;
  outsideFunderRowCount: number;
  skippedOutsideGroupFunderLookupCount: number;
};

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;
const DEFAULT_OUTSIDE_MAX_GROUPS = 20;
const DEFAULT_OUTSIDE_MAX_FUNDERS_PER_GROUP = 20;

const DEFAULT_SUNSHINE_CLIENT: WisconsinSunshineDataClient = {
  searchAndResolveCandidateCommittee: searchAndResolveWisconsinCandidateCommittee,
  getDirectOccupationAggregates: getWisconsinSunshineDirectOccupationAggregates,
  getContributionSizeAggregates: getWisconsinSunshineContributionSizeAggregates,
  getIndependentExpenditureGroups: getWisconsinSunshineIndependentExpenditureGroups,
  getOutsideSpenderOrganizationFunders: getWisconsinSunshineOutsideSpenderOrganizationFunders,
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Wisconsin finance election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Wisconsin finance sync timestamp");
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Wisconsin finance AI classification minimum amount: ${value}`);
  }
  return normalized;
}

function normalizeCandidateNameForStorage(value: string): string {
  return [...normalizeWisconsinCandidateNameKeys(value)][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function mergeSunshineClient(client: Partial<WisconsinSunshineDataClient> | undefined): WisconsinSunshineDataClient {
  return { ...DEFAULT_SUNSHINE_CLIENT, ...(client ?? {}) };
}

function toMatchedTrustedCommittee(input: NonNullable<WisconsinCandidateFinanceSyncInput["trustedCommittee"]>): MatchedWisconsinCommitteeResolution {
  return {
    status: "matched",
    entityId: requireNonEmpty(input.entityId, "trusted Wisconsin entity id"),
    committeeId: requireNonEmpty(input.committeeId, "trusted Wisconsin committee id"),
    ...(input.assignedCommitteeId?.trim() ? { assignedCommitteeId: input.assignedCommitteeId.trim() } : {}),
    committeeName: requireNonEmpty(input.committeeName, "trusted Wisconsin committee name"),
    confidence: "exact",
    source: "sunshine_api",
    sourceUrl: input.sourceUrl ?? null,
    matchedCommitteeRowCount: 0,
  };
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  resolution: MatchedWisconsinCommitteeResolution;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): WisconsinFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    entityId: requireNonEmpty(input.resolution.entityId, "Wisconsin entity id"),
    committeeId: requireNonEmpty(input.resolution.committeeId, "Wisconsin committee id"),
    committeeName: requireNonEmpty(input.resolution.committeeName, "Wisconsin committee name"),
    assignedCommitteeId: input.resolution.assignedCommitteeId ?? null,
    linkStatus: "active",
    linkSource: "sunshine_api",
    sourceUrl: input.resolution.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function combineSummary(input: {
  directSummary: WisconsinFinanceSummaryInput;
  outsideSummary: WisconsinFinanceSummaryInput;
  fallbackSourceUrl?: string | null;
}): WisconsinFinanceSummaryInput {
  return {
    totalReceipts: input.directSummary.totalReceipts ?? null,
    directContributionTotal: input.directSummary.directContributionTotal ?? null,
    totalDisbursements: input.directSummary.totalDisbursements ?? null,
    cashOnHand: input.directSummary.cashOnHand ?? null,
    outsideSupportTotal: input.outsideSummary.outsideSupportTotal ?? 0,
    outsideOpposeTotal: input.outsideSummary.outsideOpposeTotal ?? 0,
    sourceUrl: input.directSummary.sourceUrl ?? input.outsideSummary.sourceUrl ?? input.fallbackSourceUrl ?? null,
  };
}

function emptyResult(input: {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: WisconsinCandidateCommitteeResolution;
}): WisconsinCandidateFinanceSyncResult {
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
    totalDisbursements: null,
    outsideSupportTotal: null,
    outsideOpposeTotal: null,
    directOccupationRowCount: 0,
    directContributionSizeRowCount: 0,
    outsideGroupCount: 0,
    outsideFunderRowCount: 0,
    skippedOutsideGroupFunderLookupCount: 0,
  };
}

export async function syncWisconsinCandidateFinance(
  input: WisconsinCandidateFinanceSyncInput
): Promise<WisconsinCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const sunshineClient = mergeSunshineClient(input.sunshineClient);
  const aiClassificationMinAmount = normalizeAiClassificationMinAmount(input.aiClassificationMinAmount);
  const officeSearch = toWisconsinSunshineOfficeSearchInput({
    officeScope,
    officeCanonicalName: officeName,
    district: input.district,
  });

  const resolution = input.trustedCommittee
    ? toMatchedTrustedCommittee(input.trustedCommittee)
    : await sunshineClient.searchAndResolveCandidateCommittee(
        {
          candidateName,
          officeScope,
          officeName,
          electionYear,
          district: input.district,
        },
        input.sunshineClientOptions
      );

  if (resolution.status !== "matched") {
    return emptyResult({ candidateId, electionId, electionYear, dryRun, resolution });
  }

  const [directFinance, outsideFinance] = await Promise.all([
    aggregateWisconsinDirectContributions({
      entityId: resolution.entityId,
      electionYear,
      maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
      sunshineClientOptions: input.sunshineClientOptions,
      sunshineClient,
    }),
    officeSearch
      ? aggregateWisconsinOutsideSpending({
          candidateCommitteeName: resolution.committeeName,
          electionYear,
          office: officeSearch.sunshineOffice,
          district: officeSearch.district,
          maxGroups: input.outsideMaxGroups ?? DEFAULT_OUTSIDE_MAX_GROUPS,
          maxFundersPerGroup: input.outsideMaxFundersPerGroup ?? DEFAULT_OUTSIDE_MAX_FUNDERS_PER_GROUP,
          aiClassificationMinAmount,
          db: input.db,
          financeIndustryClassifier: input.financeIndustryClassifier,
          dryRun,
          sunshineClientOptions: input.sunshineClientOptions,
          sunshineClient,
        })
      : Promise.resolve({
          summary: {
            totalReceipts: null,
            directContributionTotal: null,
            totalDisbursements: null,
            cashOnHand: null,
            outsideSupportTotal: 0,
            outsideOpposeTotal: 0,
            sourceUrl: null,
          },
          outsideGroups: [],
          outsideGroupBreakdowns: [],
          classifications: [],
          outsideSupportTotal: 0,
          outsideOpposeTotal: 0,
          outsideGroupCount: 0,
          outsideFunderRowCount: 0,
          skippedOutsideGroupFunderLookupCount: 0,
        }),
  ]);

  const summary = combineSummary({
    directSummary: directFinance.summary,
    outsideSummary: outsideFinance.summary,
    fallbackSourceUrl: input.sourceUrl,
  });
  const link = toFinanceLink({
    candidateId,
    electionId,
    candidateName,
    electionYear,
    officeName,
    district: input.district,
    resolution,
    sourceUrl: input.sourceUrl,
    verifiedAt: syncedAt,
  });

  if (!dryRun) {
    await replaceWisconsinCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary,
      directBreakdowns: directFinance.directBreakdowns,
      outsideGroups: outsideFinance.outsideGroups,
      outsideGroupBreakdowns: outsideFinance.outsideGroupBreakdowns,
      classifications: outsideFinance.classifications,
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
    outsideGroupsWritten: dryRun ? 0 : outsideFinance.outsideGroups.length,
    outsideGroupBreakdownsWritten: dryRun ? 0 : outsideFinance.outsideGroupBreakdowns.length,
    totalReceipts: summary.totalReceipts ?? null,
    directContributionTotal: summary.directContributionTotal ?? null,
    totalDisbursements: summary.totalDisbursements ?? null,
    outsideSupportTotal: summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? null,
    directOccupationRowCount: directFinance.directOccupationRowCount,
    directContributionSizeRowCount: directFinance.directContributionSizeRowCount,
    outsideGroupCount: outsideFinance.outsideGroupCount,
    outsideFunderRowCount: outsideFinance.outsideFunderRowCount,
    skippedOutsideGroupFunderLookupCount: outsideFinance.skippedOutsideGroupFunderLookupCount,
  };
}
