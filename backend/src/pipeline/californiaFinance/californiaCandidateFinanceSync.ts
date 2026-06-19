import type { Pool, PoolClient } from "pg";

import {
  aggregateCaliforniaDirectContributions,
  type CalAccessReceiptRow,
} from "./californiaDirectContributionAggregator.js";
import {
  type CaliforniaIndependentSpendingSummary,
  type CaliforniaPowerSearchClientOptions,
  summarizeCaliforniaIndependentSpendingByCandidate,
} from "./californiaPowerSearchClient.js";
import {
  type CaliforniaFinanceDirectBreakdownInput,
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

function toSummaryInput(input: {
  directSummary: CaliforniaFinanceSummaryInput | null;
  outsideSummary: CaliforniaIndependentSpendingSummary | null;
  includeOutside: boolean;
  fallbackSourceUrl: string | null | undefined;
}): CaliforniaFinanceSummaryInput | undefined {
  if (input.directSummary || input.outsideSummary) {
    return {
      totalReceipts: input.directSummary?.totalReceipts ?? null,
      totalDisbursements: input.directSummary?.totalDisbursements ?? null,
      cashOnHand: input.directSummary?.cashOnHand ?? null,
      debtsOwed: input.directSummary?.debtsOwed ?? null,
      outsideSupportTotal: input.outsideSummary?.supportTotal ?? null,
      outsideOpposeTotal: input.outsideSummary?.opposeTotal ?? null,
      sourceUrl: input.directSummary?.sourceUrl ?? input.outsideSummary?.sourceUrl ?? input.fallbackSourceUrl ?? null,
    };
  }
  if (!input.includeOutside) {
    return {
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      sourceUrl: input.fallbackSourceUrl ?? null,
    };
  }
  return undefined;
}

function directBreakdownKey(input: CaliforniaFinanceDirectBreakdownInput): string {
  if (input.categoryType === "employer" || input.categoryType === "occupation") {
    return `${input.categoryType}\u0000${normalizeFinanceLabel(input.categoryName, input.categoryType)}`;
  }
  return `${input.categoryType}\u0000${input.categoryName.trim().toUpperCase()}`;
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

  if (!input.dryRun) {
    await replaceCaliforniaCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary: toSummaryInput({
        directSummary: directFinance.summary,
        outsideSummary,
        includeOutside,
        fallbackSourceUrl: link.sourceUrl,
      }),
      directBreakdowns: directFinance.directBreakdowns,
      outsideGroups: outsideSummary ? toOutsideGroups(outsideSummary) : undefined,
      classifications: directFinance.classifications,
    });
  }

  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear,
    dryRun: input.dryRun === true,
    outsideIncluded: includeOutside,
    linkWritten: !input.dryRun,
    summaryWritten: !input.dryRun && (outsideSummary !== null || !includeOutside),
    directBreakdownsWritten: input.dryRun ? 0 : directFinance.directBreakdowns?.length ?? 0,
    outsideGroupsWritten: input.dryRun ? 0 : outsideSummary?.groups.length ?? 0,
    outsideGroupBreakdownsWritten: 0,
    outsideSupportTotal: outsideSummary?.supportTotal ?? null,
    outsideOpposeTotal: outsideSummary?.opposeTotal ?? null,
  };
}
