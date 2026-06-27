import type { Pool, PoolClient } from "pg";

import { aggregateKentuckyDirectContributions } from "./kentuckyDirectContributionAggregator.js";
import {
  buildKentuckyKrefContributionExportUrl,
  buildKentuckyKrefIndependentExpenditureExportUrl,
  downloadKentuckyKrefCandidateContributions,
  downloadKentuckyKrefIeOnlyCommitteeContributions,
  downloadKentuckyKrefIndependentExpenditures,
  KENTUCKY_KREF_IE_ONLY_ORGANIZATION_TYPE,
  type KentuckyKrefClientOptions,
  type KentuckyKrefContributionExportInput,
  type KentuckyKrefContributionRecord,
  type KentuckyKrefIndependentExpenditureExportInput,
  type KentuckyKrefIndependentExpenditureRecord,
} from "./kentuckyKrefClient.js";
import {
  replaceKentuckyCandidateFinanceSnapshot,
  type KentuckyFinanceDirectBreakdownInput,
  type KentuckyFinanceLinkInput,
  type KentuckyFinanceOutsideGroupBreakdownInput,
  type KentuckyFinanceOutsideGroupInput,
  type KentuckyFinanceSummaryInput,
} from "./kentuckyFinanceWriter.js";
import { aggregateKentuckyOutsideGroupContributions } from "./kentuckyOutsideGroupContributionAggregator.js";
import {
  aggregateKentuckyOutsideSpending,
  normalizeKentuckyCandidateNameKeys,
  type KentuckyOutsideSpendingGroup,
} from "./kentuckyOutsideSpendingAggregator.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type KentuckyKrefDataClient = {
  downloadCandidateContributions: (
    input: Omit<KentuckyKrefContributionExportInput, "contributionSearchType">,
    options?: KentuckyKrefClientOptions
  ) => Promise<KentuckyKrefContributionRecord[]>;
  downloadIndependentExpenditures: (
    input: KentuckyKrefIndependentExpenditureExportInput,
    options?: KentuckyKrefClientOptions
  ) => Promise<KentuckyKrefIndependentExpenditureRecord[]>;
  downloadIeOnlyCommitteeContributions: (
    input?: Omit<KentuckyKrefContributionExportInput, "contributionSearchType" | "organizationType">,
    options?: KentuckyKrefClientOptions
  ) => Promise<KentuckyKrefContributionRecord[]>;
};

export type KentuckyCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionDate: string;
  officeName: string;
  location?: string | null;
  district?: string | null;
  sourceUrl?: string | null;
  krefClientOptions?: KentuckyKrefClientOptions;
  krefClient?: Partial<KentuckyKrefDataClient>;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxBreakdownsPerCategory?: number;
  outsideGroupIndustryMinAmount?: number;
  trustedLink: {
    candidateKey: string;
    committeeKey: string;
    committeeName: string;
    sourceUrl?: string | null;
  };
};

export type KentuckyCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number;
  directContributionTotal: number;
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
  matchedOutsideContributionRowCount: number;
  includedOutsideContributionRowCount: number;
  skippedOutsideContributionRowCount: number;
  candidateContributionRowCount: number;
  independentExpenditureRowCount: number;
  outsideContributionRowCount: number;
};

const DEFAULT_KREF_CLIENT: KentuckyKrefDataClient = {
  downloadCandidateContributions: downloadKentuckyKrefCandidateContributions,
  downloadIndependentExpenditures: downloadKentuckyKrefIndependentExpenditures,
  downloadIeOnlyCommitteeContributions: downloadKentuckyKrefIeOnlyCommitteeContributions,
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;
const DEFAULT_OUTSIDE_MAX_GROUPS = 50;
const DEFAULT_OUTSIDE_GROUP_INDUSTRY_MIN_AMOUNT = 25_000;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Kentucky finance election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Kentucky finance sync timestamp");
  }
  return normalized;
}

function normalizeCandidateNameForStorage(value: string): string {
  if (value.includes(",")) {
    const parts = splitCandidateName(value);
    return `${parts.firstName} ${parts.lastName}`.replace(/\s+/g, " ").toUpperCase();
  }
  return [...normalizeKentuckyCandidateNameKeys(value)][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function mergeKrefClient(client: Partial<KentuckyKrefDataClient> | undefined): KentuckyKrefDataClient {
  return { ...DEFAULT_KREF_CLIENT, ...(client ?? {}) };
}

function splitCandidateName(value: string): { firstName: string; lastName: string } {
  const trimmed = requireNonEmpty(value, "candidate name").replace(/\s+/g, " ");
  const commaParts = trimmed.split(",").map((part) => part.trim()).filter(Boolean);
  if (commaParts.length >= 2) {
    return {
      firstName: requireNonEmpty(commaParts.slice(1).join(" "), "candidate first name"),
      lastName: requireNonEmpty(commaParts[0] ?? "", "candidate last name"),
    };
  }

  const parts = trimmed.split(" ").filter(Boolean);
  return {
    firstName: requireNonEmpty(parts.slice(0, -1).join(" ") || parts[0] || "", "candidate first name"),
    lastName: requireNonEmpty(parts.length > 1 ? parts[parts.length - 1] ?? "" : parts[0] ?? "", "candidate last name"),
  };
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  location?: string | null;
  trustedLink: KentuckyCandidateFinanceSyncInput["trustedLink"];
  sourceUrl?: string | null;
  verifiedAt: Date;
}): KentuckyFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? input.location ?? null,
    candidateKey: requireNonEmpty(input.trustedLink.candidateKey, "trusted Kentucky candidate key"),
    committeeKey: requireNonEmpty(input.trustedLink.committeeKey, "trusted Kentucky committee key"),
    committeeName: requireNonEmpty(input.trustedLink.committeeName, "trusted Kentucky committee name"),
    linkStatus: "active",
    linkSource: "kref_public_search",
    sourceUrl: input.trustedLink.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function toSummary(input: {
  totalReceipts: number;
  directContributionTotal: number;
  outsideSupportTotal: number;
  outsideOpposeTotal: number;
  sourceUrl?: string | null;
}): KentuckyFinanceSummaryInput {
  return {
    totalReceipts: input.totalReceipts,
    directContributionTotal: input.directContributionTotal,
    outsideSupportTotal: input.outsideSupportTotal,
    outsideOpposeTotal: input.outsideOpposeTotal,
    sourceUrl: input.sourceUrl ?? null,
  };
}

function toDirectBreakdowns(
  breakdowns: readonly KentuckyFinanceDirectBreakdownInput[]
): KentuckyFinanceDirectBreakdownInput[] {
  return breakdowns.map((breakdown) => ({
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount ?? null,
    sourceUrl: breakdown.sourceUrl ?? null,
  }));
}

function toOutsideGroups(groups: readonly KentuckyOutsideSpendingGroup[]): KentuckyFinanceOutsideGroupInput[] {
  return groups.map((group) => ({
    committeeKey: group.committeeKey,
    committeeName: group.committeeName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl,
  }));
}

function toOutsideGroupBreakdowns(
  breakdowns: readonly KentuckyFinanceOutsideGroupBreakdownInput[]
): KentuckyFinanceOutsideGroupBreakdownInput[] {
  return breakdowns.map((breakdown) => ({
    committeeKey: breakdown.committeeKey,
    supportOppose: breakdown.supportOppose,
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount ?? null,
    sourceUrl: breakdown.sourceUrl ?? null,
  }));
}

async function downloadOutsideGroupContributions(input: {
  groups: readonly KentuckyOutsideSpendingGroup[];
  client: KentuckyKrefDataClient;
  options?: KentuckyKrefClientOptions;
}): Promise<KentuckyKrefContributionRecord[]> {
  const records: KentuckyKrefContributionRecord[] = [];
  const seenCommitteeKeys = new Set<string>();
  for (const group of input.groups) {
    if (seenCommitteeKeys.has(group.committeeKey)) {
      continue;
    }
    seenCommitteeKeys.add(group.committeeKey);
    records.push(
      ...(await input.client.downloadIeOnlyCommitteeContributions({ organizationName: group.committeeName }, input.options))
    );
  }
  return records;
}

export async function syncKentuckyCandidateFinance(
  input: KentuckyCandidateFinanceSyncInput
): Promise<KentuckyCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionDate = requireNonEmpty(input.electionDate, "Kentucky election date");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  requireNonEmpty(input.trustedLink.candidateKey, "trusted Kentucky candidate key");
  requireNonEmpty(input.trustedLink.committeeKey, "trusted Kentucky committee key");
  requireNonEmpty(input.trustedLink.committeeName, "trusted Kentucky committee name");

  const krefClient = mergeKrefClient(input.krefClient);
  const nameParts = splitCandidateName(candidateName);
  const candidateContributionExportInput = {
    candidateFirstName: nameParts.firstName,
    candidateLastName: nameParts.lastName,
  };
  const independentExpenditureExportInput = {
    candidateFirstName: nameParts.firstName,
    candidateLastName: nameParts.lastName,
  };
  const candidateContributionSourceUrl = buildKentuckyKrefContributionExportUrl({
    ...candidateContributionExportInput,
    contributionSearchType: "Candidate",
  });
  const independentExpenditureSourceUrl = buildKentuckyKrefIndependentExpenditureExportUrl(independentExpenditureExportInput);
  const outsideContributionSourceUrl = buildKentuckyKrefContributionExportUrl({
    contributionSearchType: "Organization",
    organizationType: KENTUCKY_KREF_IE_ONLY_ORGANIZATION_TYPE,
  });

  const [candidateContributionRecords, independentExpenditureRecords] = await Promise.all([
    krefClient.downloadCandidateContributions(candidateContributionExportInput, input.krefClientOptions),
    krefClient.downloadIndependentExpenditures(independentExpenditureExportInput, input.krefClientOptions),
  ]);

  const directFinance = aggregateKentuckyDirectContributions({
    candidateName,
    electionDate,
    officeName,
    location: input.location,
    contributionRecords: candidateContributionRecords,
    sourceUrl: candidateContributionSourceUrl,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
  });
  const outsideFinance = aggregateKentuckyOutsideSpending({
    candidateName,
    electionDate,
    officeOrBallotMeasure: officeName,
    expenditureRecords: independentExpenditureRecords,
    sourceUrl: independentExpenditureSourceUrl,
    maxGroups: input.outsideMaxGroups ?? DEFAULT_OUTSIDE_MAX_GROUPS,
  });
  const outsideGroups = outsideFinance.summary?.groups ?? [];
  const outsideContributionRecords = await downloadOutsideGroupContributions({
    groups: outsideGroups,
    client: krefClient,
    options: input.krefClientOptions,
  });
  const outsideGroupFinance = aggregateKentuckyOutsideGroupContributions({
    electionYear,
    outsideGroups,
    contributionRecords: outsideContributionRecords,
    sourceUrl: outsideContributionSourceUrl,
    maxBreakdownsPerCategory: input.outsideMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    minIndustryAmount: input.outsideGroupIndustryMinAmount ?? DEFAULT_OUTSIDE_GROUP_INDUSTRY_MIN_AMOUNT,
  });

  const summary = toSummary({
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    outsideSupportTotal: outsideFinance.summary?.supportTotal ?? 0,
    outsideOpposeTotal: outsideFinance.summary?.opposeTotal ?? 0,
    sourceUrl: directFinance.summary.sourceUrl ?? outsideFinance.summary?.sourceUrl ?? input.sourceUrl ?? null,
  });
  const directBreakdowns = toDirectBreakdowns(directFinance.directBreakdowns);
  const outsideGroupRows = toOutsideGroups(outsideGroups);
  const outsideGroupBreakdowns = toOutsideGroupBreakdowns(outsideGroupFinance.outsideGroupBreakdowns);

  if (!input.dryRun) {
    await replaceKentuckyCandidateFinanceSnapshot({
      db: input.db,
      link: toFinanceLink({
        candidateId,
        electionId,
        candidateName,
        electionYear,
        officeName,
        district: input.district,
        location: input.location,
        trustedLink: input.trustedLink,
        sourceUrl: input.sourceUrl ?? candidateContributionSourceUrl,
        verifiedAt: syncedAt,
      }),
      syncedAt,
      summary,
      directBreakdowns,
      outsideGroups: outsideGroupRows,
      outsideGroupBreakdowns,
    });
  }

  return {
    candidateId,
    electionId,
    electionYear,
    dryRun: input.dryRun === true,
    linkWritten: !input.dryRun,
    summaryWritten: !input.dryRun,
    directBreakdownsWritten: input.dryRun ? 0 : directBreakdowns.length,
    outsideGroupsWritten: input.dryRun ? 0 : outsideGroupRows.length,
    outsideGroupBreakdownsWritten: input.dryRun ? 0 : outsideGroupBreakdowns.length,
    totalReceipts: summary.totalReceipts ?? 0,
    directContributionTotal: summary.directContributionTotal ?? 0,
    outsideSupportTotal: summary.outsideSupportTotal ?? 0,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? 0,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
    matchedExpenditureRowCount: outsideFinance.matchedExpenditureRowCount,
    includedExpenditureRowCount: outsideFinance.includedExpenditureRowCount,
    skippedExpenditureRowCount: outsideFinance.skippedExpenditureRowCount,
    matchedOutsideContributionRowCount: outsideGroupFinance.matchedContributionRowCount,
    includedOutsideContributionRowCount: outsideGroupFinance.includedContributionRowCount,
    skippedOutsideContributionRowCount: outsideGroupFinance.skippedContributionRowCount,
    candidateContributionRowCount: candidateContributionRecords.length,
    independentExpenditureRowCount: independentExpenditureRecords.length,
    outsideContributionRowCount: outsideContributionRecords.length,
  };
}
