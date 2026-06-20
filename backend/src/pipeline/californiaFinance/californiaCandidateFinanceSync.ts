import type { Pool, PoolClient } from "pg";

import {
  aggregateCaliforniaDirectContributions,
  type CalAccessReceiptRow,
} from "./californiaDirectContributionAggregator.js";
import {
  type CaliforniaIndependentSpendingSummary,
  type CaliforniaPowerSearchClientOptions,
  summarizeCaliforniaIndependentSpendingByCandidate,
  toCaliforniaElectionCycle,
} from "./californiaPowerSearchClient.js";
import {
  type CaliforniaFinanceDirectBreakdownInput,
  type CaliforniaFinanceOutsideGroupBreakdownInput,
  type CaliforniaFinanceSummaryInput,
  type CaliforniaFinanceLinkInput,
  replaceCaliforniaCandidateFinanceSnapshot,
} from "./californiaFinanceWriter.js";
import {
  type FinanceLabelClassification,
  type FinanceLabelType,
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";

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
  directReceiptRows?: readonly CalAccessReceiptRow[];
  controlledCommitteeFilingIds?: readonly string[];
  directSourceUrl?: string | null;
  directMaxBreakdownsPerCategory?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  outsideReceiptRowsByCommitteeId?: ReadonlyMap<string, readonly CalAccessReceiptRow[]>;
  outsideCommitteeFilingIdsByCommitteeId?: ReadonlyMap<string, readonly string[]>;
  outsideReceiptSourceUrl?: string | null;
  loadOutsideReceiptRowsForCommittees?: (
    committeeIds: readonly string[]
  ) => Promise<{
    receiptRowsByCommitteeId: ReadonlyMap<string, readonly CalAccessReceiptRow[]>;
    controlledCommitteeFilingIdsByCommitteeId?: ReadonlyMap<string, readonly string[]>;
    sourceUrl?: string | null;
  } | null>;
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
const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 100_000;

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

function normalizeCommitteeId(value: string): string {
  return value.trim().toUpperCase();
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid California finance sync timestamp");
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error("California finance aiClassificationMinAmount must be a nonnegative number");
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

function rowValue(row: CalAccessReceiptRow, key: string): string {
  return row[key]?.trim() ?? "";
}

function parseReceiptAmount(raw: string): number | null {
  const parsed = Number(raw.replace(/[$,]/g, "").trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function parseReceiptDateYear(raw: string): number | null {
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(raw.trim());
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(raw.trim());
  return isoMatch ? Number(isoMatch[1]) : null;
}

function isReceiptInElectionCycle(row: CalAccessReceiptRow, electionYear: number): boolean {
  const year = parseReceiptDateYear(rowValue(row, "RCPT_DATE"));
  if (year === null) {
    return false;
  }
  const cycleStartYear = toCaliforniaElectionCycle(electionYear);
  return year >= cycleStartYear && year <= electionYear;
}

function isReceiptForCommittee(input: {
  row: CalAccessReceiptRow;
  committeeId: string;
  filingIds: Set<string>;
}): boolean {
  const filingId = normalizeCommitteeId(rowValue(input.row, "FILING_ID"));
  if (input.filingIds.size > 0) {
    return filingId.length > 0 && input.filingIds.has(filingId);
  }

  const rowCommitteeId = normalizeCommitteeId(rowValue(input.row, "CMTE_ID"));
  if (rowCommitteeId && rowCommitteeId === input.committeeId) {
    return true;
  }
  return false;
}

function toSummaryInput(input: {
  directSummary: CaliforniaFinanceSummaryInput | null;
  outsideSummary: CaliforniaIndependentSpendingSummary | null;
  fallbackSourceUrl: string | null | undefined;
}): CaliforniaFinanceSummaryInput | undefined {
  if (input.directSummary || input.outsideSummary) {
    const summary: CaliforniaFinanceSummaryInput = {
      sourceUrl: input.directSummary?.sourceUrl ?? input.outsideSummary?.sourceUrl ?? input.fallbackSourceUrl ?? null,
    };
    if (input.directSummary) {
      summary.totalReceipts = input.directSummary.totalReceipts;
      summary.totalDisbursements = input.directSummary.totalDisbursements;
      summary.cashOnHand = input.directSummary.cashOnHand;
      summary.debtsOwed = input.directSummary.debtsOwed;
    }
    if (input.outsideSummary) {
      summary.outsideSupportTotal = input.outsideSummary.supportTotal;
      summary.outsideOpposeTotal = input.outsideSummary.opposeTotal;
    }
    return summary;
  }
  return {
    sourceUrl: input.fallbackSourceUrl ?? null,
  };
}

function directBreakdownKey(input: CaliforniaFinanceDirectBreakdownInput): string {
  if (input.categoryType === "employer" || input.categoryType === "occupation") {
    return `${input.categoryType}\u0000${normalizeFinanceLabel(input.categoryName, input.categoryType)}`;
  }
  return `${input.categoryType}\u0000${input.categoryName.trim().toUpperCase()}`;
}

function outsideBreakdownKey(input: CaliforniaFinanceOutsideGroupBreakdownInput): string {
  const categoryKey =
    input.categoryType === "employer" || input.categoryType === "occupation" || input.categoryType === "donor"
      ? normalizeFinanceLabel(input.categoryName, input.categoryType)
      : input.categoryName.trim().toUpperCase();
  return `${input.committeeId.trim().toUpperCase()}\u0000${input.supportOppose}\u0000${input.categoryType}\u0000${categoryKey}`;
}

function combineNullableCounts(left: number | null | undefined, right: number | null | undefined): number | null {
  if (left === null || left === undefined) {
    return right ?? null;
  }
  if (right === null || right === undefined) {
    return left;
  }
  return left + right;
}

function addDirectBreakdown(
  breakdowns: Map<string, CaliforniaFinanceDirectBreakdownInput>,
  input: CaliforniaFinanceDirectBreakdownInput
): void {
  const categoryName = input.categoryName.trim();
  if (categoryName.length === 0 || input.amount < 0) {
    return;
  }
  const normalized = { ...input, categoryName };
  const key = directBreakdownKey(normalized);
  const existing = breakdowns.get(key);
  if (!existing) {
    breakdowns.set(key, normalized);
    return;
  }
  breakdowns.set(key, {
    ...existing,
    amount: existing.amount + normalized.amount,
    contributorCount: combineNullableCounts(existing.contributorCount, normalized.contributorCount),
    sourceUrl: existing.sourceUrl ?? normalized.sourceUrl,
  });
}

function toDirectBreakdownMap(
  breakdowns: readonly CaliforniaFinanceDirectBreakdownInput[]
): Map<string, CaliforniaFinanceDirectBreakdownInput> {
  const map = new Map<string, CaliforniaFinanceDirectBreakdownInput>();
  for (const breakdown of breakdowns) {
    addDirectBreakdown(map, breakdown);
  }
  return map;
}

function addOutsideBreakdown(
  breakdowns: Map<string, CaliforniaFinanceOutsideGroupBreakdownInput>,
  input: CaliforniaFinanceOutsideGroupBreakdownInput
): void {
  const committeeId = input.committeeId.trim();
  const categoryName = input.categoryName.trim();
  if (committeeId.length === 0 || categoryName.length === 0 || input.amount < 0) {
    return;
  }
  const normalized = { ...input, committeeId, categoryName };
  const key = outsideBreakdownKey(normalized);
  const existing = breakdowns.get(key);
  if (!existing) {
    breakdowns.set(key, normalized);
    return;
  }
  breakdowns.set(key, {
    ...existing,
    amount: existing.amount + normalized.amount,
    contributorCount: combineNullableCounts(existing.contributorCount, normalized.contributorCount),
    sourceUrl: existing.sourceUrl ?? normalized.sourceUrl,
  });
}

function collectOutsideClassifications(
  breakdowns: Iterable<CaliforniaFinanceOutsideGroupBreakdownInput>
): Map<string, FinanceLabelClassification> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "employer" && breakdown.categoryType !== "occupation") {
      continue;
    }
    const labelType: Extract<FinanceLabelType, "employer" | "occupation"> = breakdown.categoryType;
    const classification = classifyFinanceLabel({ rawLabel: breakdown.categoryName, labelType });
    if (classification.normalizedLabel.length === 0) {
      continue;
    }
    mergeFinanceLabelClassification(classifications, classification);
  }
  return classifications;
}

function collectDirectClassifications(
  breakdowns: Iterable<CaliforniaFinanceDirectBreakdownInput>
): Map<string, FinanceLabelClassification> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "employer" && breakdown.categoryType !== "occupation") {
      continue;
    }
    const labelType: Extract<FinanceLabelType, "employer" | "occupation"> = breakdown.categoryType;
    const classification = classifyFinanceLabel({ rawLabel: breakdown.categoryName, labelType });
    if (classification.normalizedLabel.length === 0) {
      continue;
    }
    mergeFinanceLabelClassification(classifications, classification);
  }
  return classifications;
}

async function buildDirectFinanceSnapshot(input: {
  db: Queryable;
  link: CaliforniaFinanceLinkInput;
  electionYear: number;
  receiptRows: readonly CalAccessReceiptRow[] | undefined;
  controlledCommitteeFilingIds: readonly string[] | undefined;
  sourceUrl: string | null | undefined;
  maxBreakdownsPerCategory: number | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  dryRun: boolean;
}): Promise<{
  summary: CaliforniaFinanceSummaryInput | null;
  directBreakdowns: CaliforniaFinanceDirectBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.receiptRows) {
    return { summary: null, directBreakdowns: undefined, classifications: [] };
  }

  const directAggregation = aggregateCaliforniaDirectContributions({
    controlledCommitteeId: input.link.controlledCommitteeId,
    controlledCommitteeFilingIds: input.controlledCommitteeFilingIds,
    electionYear: input.electionYear,
    receiptRows: input.receiptRows,
    sourceUrl: input.sourceUrl ?? input.link.sourceUrl ?? null,
    maxBreakdownsPerCategory: input.maxBreakdownsPerCategory,
  });
  const directBreakdowns = toDirectBreakdownMap(directAggregation.directBreakdowns);
  const classifications = collectDirectClassifications(directBreakdowns.values());

  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: directBreakdowns.values(),
    outsideBreakdowns: [],
    classifications,
    classifier: input.classifier,
    minAmount: input.aiClassificationMinAmount,
    dryRun: input.dryRun,
  });

  const industryBreakdowns = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: directBreakdowns.values(),
    outsideBreakdowns: [],
    classifications,
  });
  for (const breakdown of industryBreakdowns.directIndustryBreakdowns) {
    addDirectBreakdown(directBreakdowns, breakdown);
  }

  return {
    summary: directAggregation.summary,
    directBreakdowns: [...directBreakdowns.values()],
    classifications: [...classifications.values()],
  };
}

async function loadOutsideReceiptData(input: {
  summary: CaliforniaIndependentSpendingSummary | null;
  receiptRowsByCommitteeId: ReadonlyMap<string, readonly CalAccessReceiptRow[]> | undefined;
  committeeFilingIdsByCommitteeId: ReadonlyMap<string, readonly string[]> | undefined;
  sourceUrl: string | null | undefined;
  loader: CaliforniaCandidateFinanceSyncInput["loadOutsideReceiptRowsForCommittees"];
}): Promise<{
  receiptRowsByCommitteeId: ReadonlyMap<string, readonly CalAccessReceiptRow[]>;
  committeeFilingIdsByCommitteeId: ReadonlyMap<string, readonly string[]>;
  sourceUrl: string | null | undefined;
} | null> {
  if (!input.summary || input.summary.groups.length === 0) {
    return null;
  }
  if (input.receiptRowsByCommitteeId) {
    return {
      receiptRowsByCommitteeId: input.receiptRowsByCommitteeId,
      committeeFilingIdsByCommitteeId: input.committeeFilingIdsByCommitteeId ?? new Map(),
      sourceUrl: input.sourceUrl,
    };
  }
  if (!input.loader) {
    return null;
  }
  const loaded = await input.loader(input.summary.groups.map((group) => group.expenderId));
  if (!loaded) {
    return null;
  }
  return {
    receiptRowsByCommitteeId: loaded.receiptRowsByCommitteeId,
    committeeFilingIdsByCommitteeId: loaded.controlledCommitteeFilingIdsByCommitteeId ?? new Map(),
    sourceUrl: loaded.sourceUrl,
  };
}

function addOutsideReceiptBreakdowns(input: {
  breakdowns: Map<string, CaliforniaFinanceOutsideGroupBreakdownInput>;
  group: CaliforniaIndependentSpendingSummary["groups"][number];
  rows: readonly CalAccessReceiptRow[];
  filingIds: Set<string>;
  electionYear: number;
  sourceUrl: string | null;
}): void {
  const committeeId = normalizeCommitteeId(input.group.expenderId);
  for (const row of input.rows) {
    if (!isReceiptForCommittee({ row, committeeId, filingIds: input.filingIds })) {
      continue;
    }
    const amount = parseReceiptAmount(rowValue(row, "AMOUNT"));
    if (amount === null || amount <= 0 || !isReceiptInElectionCycle(row, input.electionYear)) {
      continue;
    }
    addOutsideBreakdown(input.breakdowns, {
      committeeId: input.group.expenderId,
      supportOppose: input.group.supportOppose,
      categoryType: "employer",
      categoryName: rowValue(row, "CTRIB_EMP"),
      amount,
      contributorCount: 1,
      sourceUrl: input.sourceUrl,
    });
    addOutsideBreakdown(input.breakdowns, {
      committeeId: input.group.expenderId,
      supportOppose: input.group.supportOppose,
      categoryType: "occupation",
      categoryName: rowValue(row, "CTRIB_OCC"),
      amount,
      contributorCount: 1,
      sourceUrl: input.sourceUrl,
    });
  }
}

async function buildOutsideGroupBreakdowns(input: {
  db: Queryable;
  summary: CaliforniaIndependentSpendingSummary | null;
  receiptData: Awaited<ReturnType<typeof loadOutsideReceiptData>>;
  electionYear: number;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: CaliforniaFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.summary || !input.receiptData) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const outsideBreakdowns = new Map<string, CaliforniaFinanceOutsideGroupBreakdownInput>();
  const sourceUrl = input.receiptData.sourceUrl ?? input.summary.sourceUrl ?? null;
  for (const group of input.summary.groups) {
    const committeeKey = normalizeCommitteeId(group.expenderId);
    addOutsideReceiptBreakdowns({
      breakdowns: outsideBreakdowns,
      group,
      rows: input.receiptData.receiptRowsByCommitteeId.get(committeeKey) ?? [],
      filingIds: new Set((input.receiptData.committeeFilingIdsByCommitteeId.get(committeeKey) ?? []).map(normalizeCommitteeId)),
      electionYear: input.electionYear,
      sourceUrl,
    });
  }

  const classifications = collectOutsideClassifications(outsideBreakdowns.values());
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: [],
    outsideBreakdowns: outsideBreakdowns.values(),
    classifications,
    classifier: input.classifier,
    minAmount: input.aiClassificationMinAmount,
    dryRun: input.dryRun,
  });

  const industryBreakdowns = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: [],
    outsideBreakdowns: outsideBreakdowns.values(),
    classifications,
  });
  for (const breakdown of industryBreakdowns.outsideIndustryBreakdowns) {
    addOutsideBreakdown(outsideBreakdowns, breakdown);
  }

  return {
    outsideGroupBreakdowns: [...outsideBreakdowns.values()],
    classifications: [...classifications.values()],
  };
}

export async function syncCaliforniaCandidateFinance(
  input: CaliforniaCandidateFinanceSyncInput
): Promise<CaliforniaCandidateFinanceSyncResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const includeOutside = input.includeOutside !== false;
  const aiClassificationMinAmount = normalizeAiClassificationMinAmount(input.aiClassificationMinAmount);
  const powerSearchClient = input.powerSearchClient ?? DEFAULT_POWER_SEARCH_CLIENT;
  const link = toFinanceLink({ ...input, electionYear, now: syncedAt });

  const directFinance = await buildDirectFinanceSnapshot({
    db: input.db,
    link,
    electionYear,
    receiptRows: input.directReceiptRows,
    controlledCommitteeFilingIds: input.controlledCommitteeFilingIds,
    sourceUrl: input.directSourceUrl,
    maxBreakdownsPerCategory: input.directMaxBreakdownsPerCategory,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    dryRun: input.dryRun === true,
  });

  const outsideSummary = includeOutside
    ? await powerSearchClient.summarizeIndependentSpendingByCandidate(
        { candidateName: input.candidateName, electionYear },
        input.powerSearchOptions ?? {}
      )
    : null;
  const outsideReceiptData = await loadOutsideReceiptData({
    summary: outsideSummary,
    receiptRowsByCommitteeId: input.outsideReceiptRowsByCommitteeId,
    committeeFilingIdsByCommitteeId: input.outsideCommitteeFilingIdsByCommitteeId,
    sourceUrl: input.outsideReceiptSourceUrl,
    loader: input.loadOutsideReceiptRowsForCommittees,
  });
  const outsideFinance = await buildOutsideGroupBreakdowns({
    db: input.db,
    summary: outsideSummary,
    receiptData: outsideReceiptData,
    electionYear,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    dryRun: input.dryRun === true,
  });
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const classification of directFinance.classifications) {
    mergeFinanceLabelClassification(classifications, classification);
  }
  for (const classification of outsideFinance.classifications) {
    mergeFinanceLabelClassification(classifications, classification);
  }
  const summaryInput = toSummaryInput({
    directSummary: directFinance.summary,
    outsideSummary,
    fallbackSourceUrl: link.sourceUrl,
  });

  if (!input.dryRun) {
    await replaceCaliforniaCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary: summaryInput,
      directBreakdowns: directFinance.directBreakdowns,
      outsideGroups: outsideSummary ? toOutsideGroups(outsideSummary) : undefined,
      outsideGroupBreakdowns: outsideFinance.outsideGroupBreakdowns,
      classifications: [...classifications.values()],
    });
  }

  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear,
    dryRun: input.dryRun === true,
    outsideIncluded: includeOutside,
    linkWritten: !input.dryRun,
    summaryWritten: !input.dryRun && Boolean(summaryInput),
    directBreakdownsWritten: input.dryRun ? 0 : directFinance.directBreakdowns?.length ?? 0,
    outsideGroupsWritten: input.dryRun ? 0 : outsideSummary?.groups.length ?? 0,
    outsideGroupBreakdownsWritten: input.dryRun ? 0 : outsideFinance.outsideGroupBreakdowns?.length ?? 0,
    outsideSupportTotal: outsideSummary?.supportTotal ?? null,
    outsideOpposeTotal: outsideSummary?.opposeTotal ?? null,
  };
}
