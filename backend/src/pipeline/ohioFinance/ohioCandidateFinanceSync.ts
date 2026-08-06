import type { Pool, PoolClient } from "pg";

import {
  classifyFinanceLabel,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import { normalizeOhioCandidateNameForStorage } from "./ohioCandidateCommitteeResolver.js";
import type { OhioDirectContributionAggregationResult } from "./ohioDirectContributionAggregator.js";
import type { OhioFinanceOutsideGroup } from "./ohioOutsideSpendingAggregator.js";
import {
  replaceOhioCandidateFinanceSnapshot,
  type OhioFinanceLinkInput,
  type OhioFinanceLinkSource,
  type OhioFinanceOutsideGroupBreakdownInput,
  type OhioFinanceOutsideGroupInput,
  type OhioFinanceSummaryInput,
} from "./ohioFinanceWriter.js";

// Per-candidate write step for Ohio finance (ohio_plan.md PR 7). Unlike the
// maryland sibling this takes aggregation RESULTS, not raw rows: the ~90 MB
// CAC_CON files must be streamed exactly once for every open accumulator
// (decision 10), so the batch layer owns loading and aggregation and this
// module only turns one candidate's results into a snapshot write.
//
// The committee identity comes from the active oh_candidate_finance_links
// row (written by the PR 5 auto-linker or manually), so there is no resolver
// call here — the due list only returns linked candidates.

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

// One candidate's slice of the year's outside-group funder aggregation
// (ohio_plan.md PR 8, #3): raw donor/industry breakdowns from the batch
// layer plus its row counters, pre industry enrichment.
export type OhioCandidateOutsideFundersInput = {
  breakdowns: readonly OhioFinanceOutsideGroupBreakdownInput[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

// One candidate's slice of the year's outside-spending aggregation. Null
// totals never occur here — a candidate with no attributed rows gets zeros
// (the aggregation ran; the answer really is zero).
export type OhioCandidateOutsideFinanceInput = {
  supportTotal: number;
  opposeTotal: number;
  groups: readonly OhioFinanceOutsideGroup[];
  // Null/undefined when the funder contribution artifacts were unavailable
  // this run: outsideGroupBreakdowns is passed undefined so the writer
  // keeps the stored breakdown rows. The outside totals leg is unaffected —
  // funders are enrichment on top of it.
  funders?: OhioCandidateOutsideFundersInput | null;
};

export type OhioCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  // The linked committee from the due row — trusted, not re-resolved. The
  // link's original provenance is written back as-is: a manual link must
  // stay "manual" (defaulted for auto flows), or it would lose provenance
  // and become eligible for auto-link supersession.
  committee: {
    committeeId: string;
    committeeName: string;
    linkSource?: OhioFinanceLinkSource;
    sourceUrl?: string | null;
  };
  directFinance: OhioDirectContributionAggregationResult;
  // Null when Form 31-U data was unavailable this run: the summary's outside
  // totals are written as NULL (the writer's preserveWhenNull policy keeps
  // the stored values) and the outside-group rows are left untouched.
  outsideFinance: OhioCandidateOutsideFinanceInput | null;
  sourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  // Optional AI industry classifier (never constructed by default — Ohio
  // syncs run rule/cached-only and persist 'unknown' classification rows
  // for the manual industry-label queue).
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  // Display cap on persisted donor rows per (committee, direction);
  // classification always sees every donor.
  outsideMaxDonorBreakdownsPerGroup?: number;
};

export type OhioCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  committeeId: string;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  matchedOutsideContributionRowCount: number;
  includedOutsideContributionRowCount: number;
  skippedOutsideContributionRowCount: number;
  totalReceipts: number;
  directContributionTotal: number;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  // Direct-aggregation diagnostics passed through for batch reporting.
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
  unknownShortDescriptionRowCount: number;
  coverReportCount: number;
  blankCoverRowCount: number;
  negativeBalanceOnHand: boolean;
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
    throw new Error(`Invalid Ohio finance sync election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Ohio finance sync timestamp");
  }
  return normalized;
}

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
// Every donor is rule-classified regardless of size (maryland parity).
const STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT = 0;
// Display cap on PERSISTED donor rows per (committee, direction), applied
// AFTER classification so a >cap-donor group still gets industry totals
// built from every donor. Industry rows are naturally bounded by the slug
// set and are never capped.
const DEFAULT_MAX_DONOR_BREAKDOWNS_PER_GROUP = 50;

function normalizeMaxDonorBreakdowns(value: number | undefined): number {
  const normalized = value ?? DEFAULT_MAX_DONOR_BREAKDOWNS_PER_GROUP;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Ohio finance outsideMaxDonorBreakdownsPerGroup: ${value}`);
  }
  return normalized;
}

function capDonorBreakdowns(
  breakdowns: readonly OhioFinanceOutsideGroupBreakdownInput[],
  maxDonorsPerGroup: number
): OhioFinanceOutsideGroupBreakdownInput[] {
  const donorsByGroup = new Map<string, OhioFinanceOutsideGroupBreakdownInput[]>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor") {
      continue;
    }
    const key = [breakdown.committeeId.trim(), breakdown.supportOppose].join(" | ");
    const list = donorsByGroup.get(key) ?? [];
    list.push(breakdown);
    donorsByGroup.set(key, list);
  }
  const kept = new Set<OhioFinanceOutsideGroupBreakdownInput>();
  for (const list of donorsByGroup.values()) {
    for (const donor of list
      .sort(
        (left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName)
      )
      .slice(0, maxDonorsPerGroup)) {
      kept.add(donor);
    }
  }
  return breakdowns.filter((breakdown) => breakdown.categoryType !== "donor" || kept.has(breakdown));
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Ohio finance AI classification minimum amount: ${value}`);
  }
  return normalized;
}

function outsideBreakdownKey(breakdown: OhioFinanceOutsideGroupBreakdownInput): string {
  return [
    breakdown.committeeId.trim(),
    breakdown.supportOppose,
    breakdown.categoryType,
    breakdown.categoryName.trim().toUpperCase(),
  ].join(" | ");
}

function addOutsideBreakdown(
  breakdowns: Map<string, OhioFinanceOutsideGroupBreakdownInput>,
  breakdown: OhioFinanceOutsideGroupBreakdownInput
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
        ? null
        : existing.contributorCount + breakdown.contributorCount,
    sourceUrl: existing.sourceUrl ?? breakdown.sourceUrl,
  });
}

function collectOutsideClassifications(
  breakdowns: Iterable<OhioFinanceOutsideGroupBreakdownInput>,
  minIndustryAmount: number
): Map<string, FinanceLabelClassification> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor" || breakdown.amount < minIndustryAmount) {
      continue;
    }
    mergeFinanceLabelClassification(
      classifications,
      classifyFinanceLabel({ rawLabel: breakdown.categoryName, labelType: "donor" })
    );
  }
  return classifications;
}

// Maryland pattern: the aggregator's static industry rows are discarded and
// rebuilt from the merged classification state (rules + cached DB rows +
// manual verdicts), so a manual industry label always wins and every
// unresolved donor persists an 'unknown' row for the manual queue.
async function enrichOutsideGroupIndustryBreakdowns(input: {
  db: Pick<Pool | PoolClient, "query">;
  outsideGroupBreakdowns: readonly OhioFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  maxDonorBreakdownsPerGroup: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: OhioFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.outsideGroupBreakdowns) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const breakdowns = new Map<string, OhioFinanceOutsideGroupBreakdownInput>();
  for (const breakdown of input.outsideGroupBreakdowns) {
    if (breakdown.categoryType !== "industry") {
      addOutsideBreakdown(breakdowns, breakdown);
    }
  }

  const classifications = collectOutsideClassifications(
    breakdowns.values(),
    STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT
  );
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: [],
    outsideBreakdowns: breakdowns.values(),
    classifications,
    classifier: input.classifier,
    minAmount: input.aiClassificationMinAmount,
    dryRun: input.dryRun,
  });

  const industryBreakdowns = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: [],
    outsideBreakdowns: breakdowns.values(),
    classifications,
  });
  for (const breakdown of industryBreakdowns.outsideIndustryBreakdowns) {
    addOutsideBreakdown(breakdowns, breakdown);
  }

  return {
    // Capped only HERE, after every donor fed the classifications and the
    // rebuilt industry rows above.
    outsideGroupBreakdowns: capDonorBreakdowns([...breakdowns.values()], input.maxDonorBreakdownsPerGroup),
    classifications: [...classifications.values()],
  };
}

export async function syncOhioCandidateFinance(
  input: OhioCandidateFinanceSyncInput
): Promise<OhioCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const committeeId = requireNonEmpty(input.committee.committeeId, "Ohio committee id");
  const committeeName = requireNonEmpty(input.committee.committeeName, "Ohio committee name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const { directFinance, outsideFinance } = input;

  const link: OhioFinanceLinkInput = {
    candidateId,
    electionId,
    electionYear,
    candidateNameNormalized: normalizeOhioCandidateNameForStorage(candidateName),
    officeName,
    district: input.district ?? null,
    committeeId,
    committeeName,
    linkStatus: "active",
    linkSource: input.committee.linkSource ?? "sos_bulk_export",
    sourceUrl: input.committee.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: syncedAt,
  };

  const summary: OhioFinanceSummaryInput = {
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    totalDisbursements: directFinance.summary.totalDisbursements,
    cashOnHand: directFinance.summary.cashOnHand,
    outsideSupportTotal: outsideFinance === null ? null : outsideFinance.supportTotal,
    outsideOpposeTotal: outsideFinance === null ? null : outsideFinance.opposeTotal,
    sourceUrl: directFinance.summary.sourceUrl ?? input.committee.sourceUrl ?? input.sourceUrl ?? null,
  };

  // Undefined (not []) when unavailable, so the writer leaves the stored
  // outside-group rows alone instead of deleting them.
  const outsideGroups: OhioFinanceOutsideGroupInput[] | undefined =
    outsideFinance === null ? undefined : [...outsideFinance.groups];

  // Funders (PR 8): same undefined-vs-[] contract one level down — a null
  // funders slice keeps the stored breakdown rows while the groups and
  // totals still refresh.
  const funders = outsideFinance?.funders ?? null;
  const outsideIndustryFinance = await enrichOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns: funders === null ? undefined : funders.breakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount: normalizeAiClassificationMinAmount(input.aiClassificationMinAmount),
    maxDonorBreakdownsPerGroup: normalizeMaxDonorBreakdowns(input.outsideMaxDonorBreakdownsPerGroup),
    dryRun,
  });

  if (!dryRun) {
    await replaceOhioCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary,
      directBreakdowns: directFinance.directBreakdowns,
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
    committeeId,
    linkWritten: !dryRun,
    summaryWritten: !dryRun,
    directBreakdownsWritten: dryRun ? 0 : directFinance.directBreakdowns.length,
    outsideGroupsWritten: dryRun ? 0 : outsideGroups?.length ?? 0,
    outsideGroupBreakdownsWritten: dryRun ? 0 : outsideIndustryFinance.outsideGroupBreakdowns?.length ?? 0,
    matchedOutsideContributionRowCount: funders?.matchedContributionRowCount ?? 0,
    includedOutsideContributionRowCount: funders?.includedContributionRowCount ?? 0,
    skippedOutsideContributionRowCount: funders?.skippedContributionRowCount ?? 0,
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    totalDisbursements: directFinance.summary.totalDisbursements,
    cashOnHand: directFinance.summary.cashOnHand,
    outsideSupportTotal: outsideFinance === null ? null : outsideFinance.supportTotal,
    outsideOpposeTotal: outsideFinance === null ? null : outsideFinance.opposeTotal,
    matchedContributionRowCount: directFinance.matchedContributionRowCount,
    includedContributionRowCount: directFinance.includedContributionRowCount,
    skippedContributionRowCount: directFinance.skippedContributionRowCount,
    unknownShortDescriptionRowCount: directFinance.unknownShortDescriptionRowCount,
    coverReportCount: directFinance.coverReportCount,
    blankCoverRowCount: directFinance.blankCoverRowCount,
    negativeBalanceOnHand: directFinance.negativeBalanceOnHand,
  };
}
