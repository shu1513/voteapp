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
  getMassachusettsOcpfCityCouncilCandidateReports,
  getMassachusettsOcpfContributionItems,
  getMassachusettsOcpfIepacReportSummaries,
  getMassachusettsOcpfLegislativeCandidateReports,
  getMassachusettsOcpfMayoralCandidateReports,
  getMassachusettsOcpfReportDetail,
  getMassachusettsOcpfStatewideCandidateReports,
  buildMassachusettsOcpfContributionItemsUrl,
  buildMassachusettsOcpfIepacReportSummariesUrl,
  type MassachusettsOcpfCandidateReport,
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

// OCPF office class for the bank-report YTD feeds; totals (raised/spent/cash)
// come from these report-cover numbers, never from itemized sums (Georgia
// lesson: transaction sums diverge from official totals — Wu 2025 items ran
// +3.6% over the bank YTD).
export type MassachusettsOcpfYtdOfficeClass = "statewide" | "legislative" | "mayoral" | "city_council";

export function massachusettsOcpfYtdOfficeClass(input: {
  officeScope: string;
  officeName: string;
}): MassachusettsOcpfYtdOfficeClass | null {
  const officeScope = input.officeScope.trim();
  if (officeScope === "statewide") {
    return "statewide";
  }
  if (officeScope === "state_upper" || officeScope === "state_lower") {
    return "legislative";
  }
  if (officeScope === "place") {
    const officeName = input.officeName.trim();
    if (officeName === "Mayor") {
      return "mayoral";
    }
    if (officeName === "City Council Member") {
      return "city_council";
    }
  }
  return null;
}

async function defaultGetCandidateYtdReports(
  input: { officeClass: MassachusettsOcpfYtdOfficeClass; electionYear: number },
  options?: MassachusettsOcpfClientOptions
): Promise<MassachusettsOcpfCandidateReport[]> {
  switch (input.officeClass) {
    case "statewide":
      // onBallot=false returns every statewide filer; the caller matches the
      // already-resolved CPF ID, so the broader list only improves coverage.
      return getMassachusettsOcpfStatewideCandidateReports(
        { electionYear: input.electionYear, onBallot: false },
        options
      );
    case "legislative":
      return getMassachusettsOcpfLegislativeCandidateReports({ electionYear: input.electionYear }, options);
    case "mayoral":
      return getMassachusettsOcpfMayoralCandidateReports({ electionYear: input.electionYear }, options);
    case "city_council":
      return getMassachusettsOcpfCityCouncilCandidateReports({ electionYear: input.electionYear }, options);
  }
}

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
  getCandidateYtdReports: (
    input: { officeClass: MassachusettsOcpfYtdOfficeClass; electionYear: number },
    options?: MassachusettsOcpfClientOptions
  ) => Promise<MassachusettsOcpfCandidateReport[]>;
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
  // Display cap on persisted donor rows per (IE PAC, direction);
  // classification always sees every donor.
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
  totalDisbursements: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  ytdReportMatched: boolean;
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
  getCandidateYtdReports: defaultGetCandidateYtdReports,
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

// Display cap on PERSISTED donor rows per (IE PAC, direction), applied AFTER
// classification so a >cap-donor group still gets industry totals built from
// every donor. Industry rows are naturally bounded by the slug set and are
// never capped.
function capDonorBreakdowns(
  breakdowns: readonly MassachusettsFinanceOutsideGroupBreakdownInput[],
  maxDonorsPerGroup: number
): MassachusettsFinanceOutsideGroupBreakdownInput[] {
  const donorsByGroup = new Map<string, MassachusettsFinanceOutsideGroupBreakdownInput[]>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor") {
      continue;
    }
    const key = `${breakdown.iepacCpfId.trim()}\u0000${breakdown.supportOppose}`;
    const list = donorsByGroup.get(key) ?? [];
    list.push(breakdown);
    donorsByGroup.set(key, list);
  }
  const kept = new Set<MassachusettsFinanceOutsideGroupBreakdownInput>();
  for (const list of donorsByGroup.values()) {
    for (const donor of list
      .sort((left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName))
      .slice(0, maxDonorsPerGroup)) {
      kept.add(donor);
    }
  }
  return breakdowns.filter((breakdown) => breakdown.categoryType !== "donor" || kept.has(breakdown));
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
  maxDonorBreakdownsPerGroup: number;
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
    // Capped only HERE, after every donor fed the classifications and the
    // rebuilt industry rows above.
    outsideGroupBreakdowns: capDonorBreakdowns([...breakdowns.values()], input.maxDonorBreakdownsPerGroup),
    classifications: [...classifications.values()],
  };
}

// Flow totals (raised/spent) must be nonnegative; malformed or negative
// values in a matched bank row become null rather than failing the writer.
function nonNegativeYtdAmount(value: number | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

// Cash on hand is signed: OCPF bank rows report legitimately overdrawn
// balances and the schema accepts them (migration 231).
function signedYtdAmount(value: number | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

// A YTD feed request failure throws so the whole candidate sync fails and the
// previous snapshot stays intact (stalest-first ordering retries it next
// run). Only a fetched feed with no row for this CPF returns null — that is
// the one case where OCPF genuinely publishes no cover totals.
async function fetchMatchingYtdReport(input: {
  ocpfClient: MassachusettsOcpfDataClient;
  ocpfClientOptions?: MassachusettsOcpfClientOptions;
  officeScope: string;
  officeName: string;
  electionYear: number;
  candidateCpfId: string;
}): Promise<MassachusettsOcpfCandidateReport | null> {
  const officeClass = massachusettsOcpfYtdOfficeClass({
    officeScope: input.officeScope,
    officeName: input.officeName,
  });
  if (!officeClass) {
    return null;
  }
  const reports = await input.ocpfClient.getCandidateYtdReports(
    { officeClass, electionYear: input.electionYear },
    input.ocpfClientOptions
  );
  return reports.find((report) => report.cpfId.trim() === input.candidateCpfId) ?? null;
}

function toSummary(input: {
  directSummary: { totalReceipts: number; directContributionTotal: number; sourceUrl: string | null };
  outsideSummary: { supportTotal: number; opposeTotal: number; sourceUrl: string | null } | null;
  fallbackSourceUrl?: string | null;
  ytdReport: MassachusettsOcpfCandidateReport | null;
}): MassachusettsFinanceSummaryInput {
  // Raised/spent/cash come from the OCPF bank-report YTD cover numbers. The
  // itemized receipt sum backfills raised ONLY when the feed has no row for
  // this candidate (pre-YTD behavior); a matched row with an invalid raised
  // value stays null rather than silently switching to the itemized sum.
  // Spent and cash are never derived from items.
  return {
    totalReceipts: input.ytdReport
      ? nonNegativeYtdAmount(input.ytdReport.receiptsYtd)
      : input.directSummary.totalReceipts,
    directContributionTotal: input.directSummary.directContributionTotal,
    totalDisbursements: nonNegativeYtdAmount(input.ytdReport?.expendituresYtd),
    cashOnHand: signedYtdAmount(input.ytdReport?.cashOnHand),
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
    totalDisbursements: null,
    outsideSupportTotal: null,
    outsideOpposeTotal: null,
    ytdReportMatched: false,
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
  const [contributionItems, iepacReports, ytdReport] = await Promise.all([
    ocpfClient.getContributionItems(
      { candidateCpfId: resolution.candidateCpfId, electionYear, limit: input.contributionItemLimit },
      input.ocpfClientOptions
    ),
    ocpfClient.getIepacReportSummaries(electionYear, input.ocpfClientOptions),
    fetchMatchingYtdReport({
      ocpfClient,
      ocpfClientOptions: input.ocpfClientOptions,
      officeScope,
      officeName,
      electionYear,
      candidateCpfId: resolution.candidateCpfId,
    }),
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
    minIndustryAmount: aiClassificationMinAmount,
  });
  const outsideIndustryFinance = await enrichOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns: outsideGroupBreakdowns.outsideGroupBreakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    maxDonorBreakdownsPerGroup: normalizePositiveInteger(
      input.outsideMaxBreakdownsPerCategory,
      DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
      "outsideMaxBreakdownsPerCategory"
    ),
    dryRun,
  });
  const summary = toSummary({
    directSummary: directFinance.summary,
    outsideSummary: outsideFinance.summary,
    fallbackSourceUrl: input.sourceUrl,
    ytdReport,
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
    totalDisbursements: summary.totalDisbursements ?? null,
    outsideSupportTotal: summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? null,
    ytdReportMatched: ytdReport !== null,
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
