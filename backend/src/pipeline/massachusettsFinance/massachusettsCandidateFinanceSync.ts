import type { Pool, PoolClient } from "pg";

import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import {
  normalizeMassachusettsCandidateNameKeys,
  searchAndResolveMassachusettsCandidateCommittee,
  type MassachusettsCandidateCommitteeMatch,
  type MassachusettsCandidateCommitteeResolution,
} from "./massachusettsCandidateCommitteeResolver.js";
import { aggregateMassachusettsDirectContributions } from "./massachusettsDirectContributionAggregator.js";
import {
  getMassachusettsOcpfContributionItems,
  getMassachusettsOcpfIepacReportSummaries,
  getMassachusettsOcpfReportDetail,
  buildMassachusettsOcpfContributionItemsUrl,
  buildMassachusettsOcpfIepacReportSummariesUrl,
  type MassachusettsOcpfClientOptions,
  type MassachusettsOcpfContributionItem,
  type MassachusettsOcpfIepacReportSummary,
  type MassachusettsOcpfReportDetail,
} from "./massachusettsOcpfClient.js";
import { aggregateMassachusettsOutsideGroupContributions } from "./massachusettsOutsideGroupContributionAggregator.js";
import { aggregateMassachusettsOutsideSpending } from "./massachusettsOutsideSpendingAggregator.js";
import {
  replaceMassachusettsCandidateFinanceSnapshot,
  type MassachusettsFinanceDirectBreakdownInput,
  type MassachusettsFinanceLinkInput,
  type MassachusettsFinanceOutsideGroupBreakdownInput,
  type MassachusettsFinanceOutsideGroupInput,
  type MassachusettsFinanceSummaryInput,
} from "./massachusettsFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

type MassachusettsOcpfDataClient = {
  searchAndResolveCandidateCommittee: (
    input: {
      candidateName: string;
      officeScope: string;
      officeName: string;
      electionYear: number;
      district?: string | null;
    },
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsCandidateCommitteeResolution>;
  getContributionItems: (
    input: { candidateCpfId: string; electionYear: number; limit?: number },
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsOcpfContributionItem[]>;
  getIepacReportSummaries: (
    electionYear: number,
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsOcpfIepacReportSummary[]>;
  getReportDetail: (
    input: { reportId: number },
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsOcpfReportDetail>;
};

export type MassachusettsCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  sourceUrl?: string | null;
  ocpfClientOptions?: MassachusettsOcpfClientOptions;
  ocpfClient?: Partial<MassachusettsOcpfDataClient>;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  contributionItemLimit?: number;
  iepacReportLimit?: number;
  outsideMaxGroups?: number;
  outsideMaxBreakdownsPerCategory?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  trustedCommittee?: {
    candidateCpfId: string;
    filerName: string;
    committeeName: string;
    sourceUrl?: string | null;
  };
};

type MassachusettsCandidateFinanceSyncResolution =
  | MassachusettsCandidateCommitteeResolution
  | ({ status: "matched" } & MassachusettsCandidateCommitteeMatch);

export type MassachusettsCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: MassachusettsCandidateFinanceSyncResolution;
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
  matchedExpenditureRowCount: number;
  includedExpenditureRowCount: number;
  skippedExpenditureRowCount: number;
  matchedReceiptRowCount: number;
  includedReceiptRowCount: number;
  skippedReceiptRowCount: number;
  iepacReportCount: number;
  iepacReportDetailCount: number;
};

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;
const DEFAULT_IEPAC_REPORT_LIMIT = 1_000;
const DEFAULT_REPORT_DETAIL_CONCURRENCY = 8;

const DEFAULT_OCPF_CLIENT: MassachusettsOcpfDataClient = {
  searchAndResolveCandidateCommittee: searchAndResolveMassachusettsCandidateCommittee,
  getContributionItems: getMassachusettsOcpfContributionItems,
  getIepacReportSummaries: getMassachusettsOcpfIepacReportSummaries,
  getReportDetail: getMassachusettsOcpfReportDetail,
};

type MatchedMassachusettsCommitteeResolution = Extract<MassachusettsCandidateFinanceSyncResolution, { status: "matched" }>;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Massachusetts finance election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Massachusetts finance sync timestamp");
  }
  return normalized;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Massachusetts finance ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Massachusetts finance AI classification minimum amount: ${value}`);
  }
  return normalized;
}

function normalizeCandidateNameForStorage(value: string): string {
  return [...normalizeMassachusettsCandidateNameKeys(value)][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function mergeOcpfClient(client: Partial<MassachusettsOcpfDataClient> | undefined): MassachusettsOcpfDataClient {
  return { ...DEFAULT_OCPF_CLIENT, ...(client ?? {}) };
}

function toFailureReason(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function fetchReportDetailsBounded(input: {
  reports: readonly MassachusettsOcpfIepacReportSummary[];
  ocpfClient: MassachusettsOcpfDataClient;
  ocpfClientOptions?: MassachusettsOcpfClientOptions;
  concurrency?: number;
}): Promise<MassachusettsOcpfReportDetail[]> {
  const workerCount = Math.max(
    1,
    Math.min(input.concurrency ?? DEFAULT_REPORT_DETAIL_CONCURRENCY, input.reports.length)
  );
  const details: MassachusettsOcpfReportDetail[] = [];
  let nextIndex = 0;

  async function worker(): Promise<void> {
    while (nextIndex < input.reports.length) {
      const report = input.reports[nextIndex];
      nextIndex += 1;
      if (!report) {
        continue;
      }
      try {
        details.push(await input.ocpfClient.getReportDetail({ reportId: report.reportId }, input.ocpfClientOptions));
      } catch (error) {
        console.warn(
          `Massachusetts finance sync skipped OCPF report detail reportId=${report.reportId}: ${toFailureReason(error)}`
        );
      }
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return details;
}

function toMatchedTrustedCommittee(
  input: NonNullable<MassachusettsCandidateFinanceSyncInput["trustedCommittee"]>
): MatchedMassachusettsCommitteeResolution {
  const filerName = requireNonEmpty(input.filerName, "trusted Massachusetts filer name");
  return {
    status: "matched",
    candidateCpfId: requireNonEmpty(input.candidateCpfId, "trusted Massachusetts candidate CPF ID"),
    filerName,
    committeeName: requireNonEmpty(input.committeeName, "trusted Massachusetts committee name"),
    officeSought: null,
    confidence: "exact",
    source: "ocpf_api",
    sourceUrl: input.sourceUrl ?? null,
    matchedFilerRowCount: 0,
  };
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  resolution: MatchedMassachusettsCommitteeResolution;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): MassachusettsFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    candidateCpfId: requireNonEmpty(input.resolution.candidateCpfId, "Massachusetts candidate CPF ID"),
    filerName: requireNonEmpty(input.resolution.filerName, "Massachusetts filer name"),
    committeeName: requireNonEmpty(input.resolution.committeeName ?? input.resolution.filerName, "Massachusetts committee name"),
    linkStatus: "active",
    linkSource: "ocpf_api",
    sourceUrl: input.resolution.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function toDirectBreakdowns(breakdowns: readonly MassachusettsFinanceDirectBreakdownInput[]): MassachusettsFinanceDirectBreakdownInput[] {
  return breakdowns.map((breakdown) => ({
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount ?? null,
    sourceUrl: breakdown.sourceUrl ?? null,
  }));
}

function toOutsideGroups(groups: readonly MassachusettsFinanceOutsideGroupInput[]): MassachusettsFinanceOutsideGroupInput[] {
  return groups.map((group) => ({
    iepacCpfId: group.iepacCpfId,
    iepacName: group.iepacName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl ?? null,
  }));
}

function outsideBreakdownKey(input: MassachusettsFinanceOutsideGroupBreakdownInput): string {
  const categoryKey =
    input.categoryType === "donor"
      ? normalizeFinanceLabel(input.categoryName, "donor")
      : input.categoryName.trim().toUpperCase();
  return `${input.iepacCpfId.trim()}\u0000${input.supportOppose}\u0000${input.categoryType}\u0000${categoryKey}`;
}

function addOutsideBreakdown(
  breakdowns: Map<string, MassachusettsFinanceOutsideGroupBreakdownInput>,
  breakdown: MassachusettsFinanceOutsideGroupBreakdownInput
): void {
  const key = outsideBreakdownKey(breakdown);
  const existing = breakdowns.get(key);
  if (!existing) {
    breakdowns.set(key, breakdown);
    return;
  }
  breakdowns.set(key, {
    ...existing,
    amount: Math.round((existing.amount + breakdown.amount) * 100) / 100,
    contributorCount:
      existing.contributorCount === null ||
      existing.contributorCount === undefined ||
      breakdown.contributorCount === null ||
      breakdown.contributorCount === undefined
        ? existing.contributorCount ?? breakdown.contributorCount ?? null
        : existing.contributorCount + breakdown.contributorCount,
    sourceUrl: existing.sourceUrl ?? breakdown.sourceUrl,
  });
}

function collectOutsideClassifications(
  breakdowns: Iterable<MassachusettsFinanceOutsideGroupBreakdownInput>,
  minAmount: number
): Map<string, FinanceLabelClassification> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor" || breakdown.amount < minAmount) {
      continue;
    }
    mergeFinanceLabelClassification(
      classifications,
      classifyFinanceLabel({ rawLabel: breakdown.categoryName, labelType: "donor" })
    );
  }
  return classifications;
}

function asClassifiableOutsideBreakdowns(breakdowns: Iterable<MassachusettsFinanceOutsideGroupBreakdownInput>) {
  return [...breakdowns].map((breakdown) => ({
    committeeId: breakdown.iepacCpfId,
    supportOppose: breakdown.supportOppose,
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount,
    sourceUrl: breakdown.sourceUrl,
  }));
}

async function enrichOutsideGroupIndustryBreakdowns(input: {
  db: Queryable;
  outsideGroupBreakdowns: readonly MassachusettsFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: MassachusettsFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.outsideGroupBreakdowns) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const breakdowns = new Map<string, MassachusettsFinanceOutsideGroupBreakdownInput>();
  for (const breakdown of input.outsideGroupBreakdowns) {
    if (breakdown.categoryType !== "industry") {
      addOutsideBreakdown(breakdowns, breakdown);
    }
  }

  const classifiableOutsideBreakdowns = asClassifiableOutsideBreakdowns(breakdowns.values());
  const classifications = collectOutsideClassifications(breakdowns.values(), input.aiClassificationMinAmount);
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: [],
    outsideBreakdowns: classifiableOutsideBreakdowns,
    classifications,
    classifier: input.classifier,
    minAmount: input.aiClassificationMinAmount,
    dryRun: input.dryRun,
  });

  const industryBreakdowns = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: [],
    outsideBreakdowns: classifiableOutsideBreakdowns,
    classifications,
  });
  for (const breakdown of industryBreakdowns.outsideIndustryBreakdowns) {
    addOutsideBreakdown(breakdowns, {
      iepacCpfId: breakdown.committeeId,
      supportOppose: breakdown.supportOppose,
      categoryType: "industry",
      categoryName: breakdown.categoryName,
      amount: breakdown.amount,
      contributorCount: breakdown.contributorCount,
      sourceUrl: breakdown.sourceUrl,
    });
  }

  return {
    outsideGroupBreakdowns: [...breakdowns.values()],
    classifications: [...classifications.values()],
  };
}

function toSummary(input: {
  directSummary: { totalReceipts: number; directContributionTotal: number; sourceUrl: string | null };
  outsideSummary: { supportTotal: number; opposeTotal: number; sourceUrl: string | null } | null;
  fallbackSourceUrl?: string | null;
}): MassachusettsFinanceSummaryInput {
  return {
    totalReceipts: input.directSummary.totalReceipts,
    directContributionTotal: input.directSummary.directContributionTotal,
    outsideSupportTotal: input.outsideSummary?.supportTotal ?? 0,
    outsideOpposeTotal: input.outsideSummary?.opposeTotal ?? 0,
    sourceUrl: input.directSummary.sourceUrl ?? input.outsideSummary?.sourceUrl ?? input.fallbackSourceUrl ?? null,
  };
}

function emptyResult(input: {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: MassachusettsCandidateFinanceSyncResolution;
}): MassachusettsCandidateFinanceSyncResult {
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
    matchedExpenditureRowCount: 0,
    includedExpenditureRowCount: 0,
    skippedExpenditureRowCount: 0,
    matchedReceiptRowCount: 0,
    includedReceiptRowCount: 0,
    skippedReceiptRowCount: 0,
    iepacReportCount: 0,
    iepacReportDetailCount: 0,
  };
}

export async function syncMassachusettsCandidateFinance(
  input: MassachusettsCandidateFinanceSyncInput
): Promise<MassachusettsCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const ocpfClient = mergeOcpfClient(input.ocpfClient);
  const aiClassificationMinAmount = normalizeAiClassificationMinAmount(input.aiClassificationMinAmount);
  const iepacReportLimit = normalizePositiveInteger(input.iepacReportLimit, DEFAULT_IEPAC_REPORT_LIMIT, "iepacReportLimit");

  const resolution = input.trustedCommittee
    ? toMatchedTrustedCommittee(input.trustedCommittee)
    : await ocpfClient.searchAndResolveCandidateCommittee(
        {
          candidateName,
          officeScope,
          officeName,
          electionYear,
          district: input.district,
        },
        input.ocpfClientOptions
      );

  if (resolution.status !== "matched") {
    return emptyResult({ candidateId, electionId, electionYear, dryRun, resolution });
  }

  const contributionSourceUrl = buildMassachusettsOcpfContributionItemsUrl({
    candidateCpfId: resolution.candidateCpfId,
    electionYear,
    limit: input.contributionItemLimit,
  });
  const iepacSourceUrl = buildMassachusettsOcpfIepacReportSummariesUrl(electionYear);
  const [contributionItems, iepacReports] = await Promise.all([
    ocpfClient.getContributionItems(
      { candidateCpfId: resolution.candidateCpfId, electionYear, limit: input.contributionItemLimit },
      input.ocpfClientOptions
    ),
    ocpfClient.getIepacReportSummaries(electionYear, input.ocpfClientOptions),
  ]);

  const boundedIepacReports = iepacReports.slice(0, iepacReportLimit);
  const reportDetails = await fetchReportDetailsBounded({
    reports: boundedIepacReports,
    ocpfClient,
    ocpfClientOptions: input.ocpfClientOptions,
  });

  const directFinance = aggregateMassachusettsDirectContributions({
    candidateCpfId: resolution.candidateCpfId,
    electionYear,
    contributionItems,
    sourceUrl: contributionSourceUrl,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
  });
  const outsideFinance = aggregateMassachusettsOutsideSpending({
    candidateCpfId: resolution.candidateCpfId,
    electionYear,
    reportDetails,
    sourceUrl: iepacSourceUrl,
    maxGroups: input.outsideMaxGroups ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
  });
  const outsideGroupBreakdowns = aggregateMassachusettsOutsideGroupContributions({
    electionYear,
    outsideGroups: outsideFinance.summary?.groups ?? [],
    reportDetails,
    sourceUrl: iepacSourceUrl,
    maxBreakdownsPerCategory: input.outsideMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    minIndustryAmount: aiClassificationMinAmount,
  });
  const outsideIndustryFinance = await enrichOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns: outsideGroupBreakdowns.outsideGroupBreakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    dryRun,
  });
  const summary = toSummary({
    directSummary: directFinance.summary,
    outsideSummary: outsideFinance.summary,
    fallbackSourceUrl: input.sourceUrl,
  });
  const directBreakdowns = toDirectBreakdowns(directFinance.directBreakdowns);
  const outsideGroups = toOutsideGroups(outsideFinance.summary?.groups ?? []);
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
    await replaceMassachusettsCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary,
      directBreakdowns,
      outsideGroups,
      outsideGroupBreakdowns: outsideIndustryFinance.outsideGroupBreakdowns,
      classifications: outsideIndustryFinance.classifications,
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
    directBreakdownsWritten: dryRun ? 0 : directBreakdowns.length,
    outsideGroupsWritten: dryRun ? 0 : outsideGroups.length,
    outsideGroupBreakdownsWritten: dryRun ? 0 : outsideIndustryFinance.outsideGroupBreakdowns?.length ?? 0,
    totalReceipts: summary.totalReceipts ?? null,
    directContributionTotal: summary.directContributionTotal ?? null,
    outsideSupportTotal: summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? null,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
    matchedExpenditureRowCount: outsideFinance.matchedExpenditureRowCount,
    includedExpenditureRowCount: outsideFinance.includedExpenditureRowCount,
    skippedExpenditureRowCount: outsideFinance.skippedExpenditureRowCount,
    matchedReceiptRowCount: outsideGroupBreakdowns.matchedReceiptRowCount,
    includedReceiptRowCount: outsideGroupBreakdowns.includedReceiptRowCount,
    skippedReceiptRowCount: outsideGroupBreakdowns.skippedReceiptRowCount,
    iepacReportCount: iepacReports.length,
    iepacReportDetailCount: reportDetails.length,
  };
}
