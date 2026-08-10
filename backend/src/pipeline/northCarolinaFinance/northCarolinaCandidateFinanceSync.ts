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
import { normalizeNorthCarolinaCandidateNameForStorage } from "./northCarolinaCandidateCommitteeResolver.js";
import type { NorthCarolinaDirectAggregationResult } from "./northCarolinaDirectContributionAggregator.js";
import type { NorthCarolinaFinanceOutsideGroup } from "./northCarolinaOutsideSpendingAggregator.js";
import {
  replaceNorthCarolinaCandidateFinanceSnapshot,
  type NorthCarolinaFinanceLinkInput,
  type NorthCarolinaFinanceLinkSource,
  type NorthCarolinaFinanceOutsideGroupBreakdownInput,
  type NorthCarolinaFinanceOutsideGroupInput,
  type NorthCarolinaFinanceSummaryInput,
} from "./northCarolinaFinanceWriter.js";

// Per-candidate write step for North Carolina finance (north_carolina_plan.md
// PR 7), ohio shape: this module takes aggregation RESULTS, not artifacts —
// the batch layer owns the artifact cache and the aggregators, and this
// module turns one candidate's results into one snapshot write.
//
// It enforces the direct aggregator's three-status write contract (PR 6):
// - "ok"                   → write the aggregated snapshot.
// - "honest_null"          → write it too: the aggregator already produced
//                            the honest snapshot (null summary fields, empty
//                            direct breakdowns) because the portal PROVES a
//                            required period is superseded-unavailable or its
//                            lineage is ambiguous. The writer's
//                            preserve-when-null policy keeps outside totals.
// - "incomplete_artifacts" → REFUSE to write (throw): the cache, not the
//                            portal, is suspect — a missing cached report or
//                            a mispaired cover must keep the previous valid
//                            snapshot and be re-acquired, never become
//                            writable money or a fake honest-null.
//
// The committee identity comes from the active nc_candidate_finance_links row
// (auto-linker or manual) via the due row — trusted, not re-resolved. The
// link's original provenance is written back as-is: a manual link must stay
// "manual" or it would lose provenance and become eligible for auto-link
// supersession.

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

// One candidate's slice of the year's outside-group funder aggregation
// (north_carolina_plan.md PR 8, #3): raw donor/industry breakdowns from the
// batch layer plus its row counters, pre industry enrichment.
export type NorthCarolinaCandidateOutsideFundersInput = {
  breakdowns: readonly NorthCarolinaFinanceOutsideGroupBreakdownInput[];
  matchedReceiptRowCount: number;
  includedReceiptRowCount: number;
  skippedReceiptRowCount: number;
};

// One candidate's slice of the year's outside-spending aggregation. Null
// totals never occur here — a candidate with no attributed rows gets zeros
// (the aggregation ran; the answer really is zero). The batch layer passes
// outsideFinance null instead when the IE artifacts were unavailable, and the
// writer's preserveWhenNull policy keeps the stored outside totals.
export type NorthCarolinaCandidateOutsideFinanceInput = {
  supportTotal: number;
  opposeTotal: number;
  groups: readonly NorthCarolinaFinanceOutsideGroup[];
  // Null/undefined when the funder receipt artifacts were unavailable this
  // run: outsideGroupBreakdowns is passed undefined so the writer keeps the
  // stored breakdown rows. The outside totals leg is unaffected — funders
  // are enrichment on top of it.
  funders?: NorthCarolinaCandidateOutsideFundersInput | null;
};

export type NorthCarolinaCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  committee: {
    committeeId: string;
    committeeName: string;
    linkSource?: NorthCarolinaFinanceLinkSource;
    sourceUrl?: string | null;
  };
  directFinance: NorthCarolinaDirectAggregationResult;
  outsideFinance: NorthCarolinaCandidateOutsideFinanceInput | null;
  sourceUrl?: string | null;
  now?: Date;
  dryRun?: boolean;
  // Optional AI industry classifier (never constructed by default — North
  // Carolina syncs run rule/cached-only and persist 'unknown' classification
  // rows for the manual industry-label queue).
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  // Display cap on persisted donor rows per (committee, direction);
  // classification always sees every donor.
  outsideMaxDonorBreakdownsPerGroup?: number;
};

export type NorthCarolinaCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  committeeId: string;
  // Never "incomplete_artifacts" — that status throws instead of writing.
  directStatus: "ok" | "honest_null";
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  matchedOutsideReceiptRowCount: number;
  includedOutsideReceiptRowCount: number;
  skippedOutsideReceiptRowCount: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  // Direct-aggregation diagnostics passed through for batch reporting.
  selectedReportCount: number;
  supersededUnavailablePeriodCount: number;
  quarantinedGroupCount: number;
  derivedBreakdownsQuarantined: boolean;
  unknownReceiptTypeCodeCount: number;
  cycleChainMismatchCount: number;
  negativeCashOnHand: boolean;
  ieTypedRegularReportRowCount: number;
  ieTypedRegularReportCents: number;
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
    throw new Error(`Invalid North Carolina finance sync election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid North Carolina finance sync timestamp");
  }
  return normalized;
}

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
// Every donor is rule-classified regardless of size (maryland/ohio parity).
const STATE_MIN_OUTSIDE_INDUSTRY_AMOUNT = 0;
// Display cap on PERSISTED donor rows per (committee, direction), applied
// AFTER classification so a >cap-donor group still gets industry totals
// built from every donor. Industry rows are naturally bounded by the slug
// set and are never capped.
const DEFAULT_MAX_DONOR_BREAKDOWNS_PER_GROUP = 50;

function normalizeMaxDonorBreakdowns(value: number | undefined): number {
  const normalized = value ?? DEFAULT_MAX_DONOR_BREAKDOWNS_PER_GROUP;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid North Carolina finance outsideMaxDonorBreakdownsPerGroup: ${value}`);
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid North Carolina finance AI classification minimum amount: ${value}`);
  }
  return normalized;
}

function capDonorBreakdowns(
  breakdowns: readonly NorthCarolinaFinanceOutsideGroupBreakdownInput[],
  maxDonorsPerGroup: number
): NorthCarolinaFinanceOutsideGroupBreakdownInput[] {
  const donorsByGroup = new Map<string, NorthCarolinaFinanceOutsideGroupBreakdownInput[]>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor") {
      continue;
    }
    const key = [breakdown.committeeId.trim(), breakdown.supportOppose].join(" | ");
    const list = donorsByGroup.get(key) ?? [];
    list.push(breakdown);
    donorsByGroup.set(key, list);
  }
  const kept = new Set<NorthCarolinaFinanceOutsideGroupBreakdownInput>();
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

function outsideBreakdownKey(breakdown: NorthCarolinaFinanceOutsideGroupBreakdownInput): string {
  return [
    breakdown.committeeId.trim(),
    breakdown.supportOppose,
    breakdown.categoryType,
    breakdown.categoryName.trim().toUpperCase(),
  ].join(" | ");
}

function addOutsideBreakdown(
  breakdowns: Map<string, NorthCarolinaFinanceOutsideGroupBreakdownInput>,
  breakdown: NorthCarolinaFinanceOutsideGroupBreakdownInput
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
  breakdowns: Iterable<NorthCarolinaFinanceOutsideGroupBreakdownInput>,
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

// Maryland/ohio pattern: the aggregator's static industry rows are discarded
// and rebuilt from the merged classification state (rules + cached DB rows +
// manual verdicts), so a manual industry label always wins and every
// unresolved donor persists an 'unknown' row for the manual queue.
async function enrichOutsideGroupIndustryBreakdowns(input: {
  db: Queryable;
  outsideGroupBreakdowns: readonly NorthCarolinaFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  maxDonorBreakdownsPerGroup: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: NorthCarolinaFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.outsideGroupBreakdowns) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const breakdowns = new Map<string, NorthCarolinaFinanceOutsideGroupBreakdownInput>();
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

export async function syncNorthCarolinaCandidateFinance(
  input: NorthCarolinaCandidateFinanceSyncInput
): Promise<NorthCarolinaCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const committeeId = requireNonEmpty(input.committee.committeeId, "North Carolina committee id");
  const committeeName = requireNonEmpty(input.committee.committeeName, "North Carolina committee name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const { directFinance, outsideFinance } = input;

  // Three-status contract: incomplete artifacts never write. The message
  // carries the exact suspects so the caller (or a human) can re-acquire
  // precisely what is broken.
  if (directFinance.status === "incomplete_artifacts") {
    const suspects = [
      ...directFinance.missingReportIds.map((reportId) => `missing report ${reportId}`),
      ...directFinance.coverIdentityMismatchReportIds.map((reportId) => `mispaired cover for report ${reportId}`),
    ];
    throw new Error(
      "North Carolina finance artifacts are incomplete for this candidate; keeping the previous snapshot " +
        `and requiring re-acquisition (run north-carolina-candidates:finance:raw:refresh): ${suspects.join(", ")}`
    );
  }

  const link: NorthCarolinaFinanceLinkInput = {
    candidateId,
    electionId,
    electionYear,
    candidateNameNormalized: normalizeNorthCarolinaCandidateNameForStorage(candidateName),
    officeName,
    district: input.district ?? null,
    committeeId,
    committeeName,
    linkStatus: "active",
    linkSource: input.committee.linkSource ?? "ncsbe_portal",
    sourceUrl: input.committee.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: syncedAt,
  };

  const summary: NorthCarolinaFinanceSummaryInput = {
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    totalDisbursements: directFinance.summary.totalDisbursements,
    cashOnHand: directFinance.summary.cashOnHand,
    outsideSupportTotal: outsideFinance === null ? null : outsideFinance.supportTotal,
    outsideOpposeTotal: outsideFinance === null ? null : outsideFinance.opposeTotal,
    sourceUrl: directFinance.summary.sourceUrl ?? input.committee.sourceUrl ?? input.sourceUrl ?? null,
  };

  // Undefined (not []) when the outside leg was unavailable, so the writer
  // leaves the stored outside-group rows alone instead of deleting them. An
  // available leg with zero groups passes [] and legitimately clears rows.
  const outsideGroups: NorthCarolinaFinanceOutsideGroupInput[] | undefined =
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
    await replaceNorthCarolinaCandidateFinanceSnapshot({
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
    directStatus: directFinance.status,
    linkWritten: !dryRun,
    summaryWritten: !dryRun,
    directBreakdownsWritten: dryRun ? 0 : directFinance.directBreakdowns.length,
    outsideGroupsWritten: dryRun ? 0 : outsideGroups?.length ?? 0,
    outsideGroupBreakdownsWritten: dryRun ? 0 : outsideIndustryFinance.outsideGroupBreakdowns?.length ?? 0,
    matchedOutsideReceiptRowCount: funders?.matchedReceiptRowCount ?? 0,
    includedOutsideReceiptRowCount: funders?.includedReceiptRowCount ?? 0,
    skippedOutsideReceiptRowCount: funders?.skippedReceiptRowCount ?? 0,
    totalReceipts: directFinance.summary.totalReceipts,
    directContributionTotal: directFinance.summary.directContributionTotal,
    totalDisbursements: directFinance.summary.totalDisbursements,
    cashOnHand: directFinance.summary.cashOnHand,
    outsideSupportTotal: outsideFinance === null ? null : outsideFinance.supportTotal,
    outsideOpposeTotal: outsideFinance === null ? null : outsideFinance.opposeTotal,
    selectedReportCount: directFinance.selectedReportIds.length,
    supersededUnavailablePeriodCount: directFinance.supersededUnavailablePeriods.length,
    quarantinedGroupCount: directFinance.quarantinedGroups.length,
    derivedBreakdownsQuarantined: directFinance.derivedBreakdownsQuarantined,
    unknownReceiptTypeCodeCount: directFinance.unknownReceiptTypeCodes.length,
    cycleChainMismatchCount: directFinance.cycleChainMismatches.length,
    negativeCashOnHand: directFinance.negativeCashOnHand,
    ieTypedRegularReportRowCount: directFinance.ieTypedRegularReportRowCount,
    ieTypedRegularReportCents: directFinance.ieTypedRegularReportCents,
  };
}
