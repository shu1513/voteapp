import {
  selectRhodeIslandCf2CyclePeriods,
  ERTS_CF2_BEGINNING_CASH_LABEL,
  ERTS_CF2_ENDING_CASH_LABEL,
  ERTS_CF2_TOTAL_CASH_LABEL,
  type RhodeIslandCf2CycleSelection,
  type RhodeIslandCf2PeriodValues,
} from "./rhodeIslandCf2ReportSelector.js";
import { readErtsTextArtifact } from "./rhodeIslandErtsArtifactCache.js";
import { ertsContributionReportUrl } from "./rhodeIslandErtsClient.js";
import {
  classifyErtsSearchResult,
  parseErtsContributionExport,
  parseErtsSummaryGroupings,
  ERTS_CONTRIBUTION_RESULT_GRID_ID,
  ERTS_CONTRIBUTION_SUMMARY_GRID_ID,
  ERTS_CONTRIBUTION_TYPE_CODES,
  ERTS_EXPENDITURE_RESULT_GRID_ID,
  ERTS_EXPENDITURE_SUMMARY_GRID_ID,
  type ErtsContributionExportRow,
} from "./rhodeIslandErtsParsers.js";

// Direct-contribution aggregation for Rhode Island (rhode_island_plan.md
// decisions 1, 10, 13). Cache only, no portal traffic. Three jobs:
//
// 1. `direct_contribution_total` — donor money only, summed from the report
//    pages' SUMMARY GROUPINGS (never from summing the export — the georgia
//    cover-arithmetic lesson; the acquisition's gate (a) proved every
//    grouping is either reproduced cent-exact by the export or genuinely
//    summary-only, so the groupings are the complete official per-type
//    totals). Loans, interest, public funds, check-off and `Other Receipt`
//    stay in `total_receipts` (the CF-2 side) but out of the direct total.
//
// 2. Size buckets — itemized export rows only (georgia bucket boundaries,
//    unique-contributor counting). `Aggregate - *` rows are lawful sub-$200
//    roll-ups and never enter buckets (decision 10), which is why the
//    published coverage note says buckets reflect itemized contributions
//    only.
//
// 3. Per-period CF-2 reconciliation — the search-side groupings must agree
//    with the authoritative CF-2's own lines: cash receipts against
//    TotalCash - Beginning, the expenditure summary against TotalCash -
//    Ending, and each mapped CF-2 line against its SET of grouping labels
//    (spike result 5b: line 6 is every in-kind type; itemized + Aggregate
//    pairs roll up into one line). Any disagreement QUARANTINES the
//    organization — mismatched money is never published.
//
// The summary-groupings vocabulary is open (spike result 5b: `NSF Check`,
// `Refund of Contribution` render with no search code): labels outside the
// pinned vocabulary are reported as diagnostics and excluded from the direct
// total, never guessed into a bucket. They still participate in the cash-
// receipts reconciliation, which sums every non-in-kind grouping — that is
// what the CF-2's own arithmetic includes.

// --- Decision-13 partition of the pinned contribution-type vocabulary -------

/** Donor money: itemized + aggregate + in-kind, for the direct total. */
export const ERTS_DONOR_CONTRIBUTION_TYPES: ReadonlySet<string> = new Set([
  "Individual",
  "PAC",
  "Party",
  "Aggregate - Individual",
  "Aggregate - PAC",
  "Aggregate - Party",
  "In-Kind - Individual",
  "In-Kind - Party",
  "In-Kind - PAC",
  "In Kind - Aggregate",
]);

/** Itemized donor rows — the only rows that enter size buckets. */
export const ERTS_BUCKETED_CONTRIBUTION_TYPES: ReadonlySet<string> = new Set([
  "Individual",
  "PAC",
  "Party",
  "In-Kind - Individual",
  "In-Kind - Party",
  "In-Kind - PAC",
]);

/** Non-donor receipts: in total_receipts (CF-2 side), never in the direct total. */
export const ERTS_NON_DONOR_CONTRIBUTION_TYPES: ReadonlySet<string> = new Set([
  "Loan Proceeds",
  "Loan Proceeds - PAC",
  "Loan Proceeds - Party",
  "Interest Received",
  "Refund/Rebate",
  "State Check Off",
  "Matching Public Funds",
  "Other Receipt",
]);

/**
 * Party-building money on a candidate organization is a classification
 * defect (decision 13: quarantine + diagnostic) — it lawfully belongs to
 * party committees, so its appearance here means the organization's ledger
 * is not what the module believes it is.
 */
export const ERTS_PARTY_BUILDING_CONTRIBUTION_TYPES: ReadonlySet<string> = new Set([
  "Party Building - Individual",
  "Party Building - PAC",
  "Party Building - Party",
]);

// The partition must cover the pinned search vocabulary exactly — a type
// added to the portal shows up as an unknown-label diagnostic, but a type
// added to the PIN without a classification would silently fall through.
for (const type of Object.keys(ERTS_CONTRIBUTION_TYPE_CODES)) {
  const memberships = [
    ERTS_DONOR_CONTRIBUTION_TYPES.has(type),
    ERTS_NON_DONOR_CONTRIBUTION_TYPES.has(type),
    ERTS_PARTY_BUILDING_CONTRIBUTION_TYPES.has(type),
  ].filter(Boolean).length;
  if (memberships !== 1) {
    throw new Error(`ERTS contribution type ${JSON.stringify(type)} is classified ${memberships} times, expected exactly 1`);
  }
}

const IN_KIND_LABEL_PATTERN = /^In[- ]Kind/i;

// --- CF-2 line <-> grouping-set mapping (spike gate 6, proven live) ---------

export const ERTS_CF2_LINE_CHECKS: readonly {
  cf2Label: string;
  description: string;
  matches: (label: string) => boolean;
}[] = [
  {
    cf2Label: "2. Individuals",
    description: "Individual (+ Aggregate)",
    matches: (label) => label === "Individual" || label === "Aggregate - Individual",
  },
  {
    cf2Label: "3. Political Parties",
    description: "Party (+ Aggregate)",
    matches: (label) => label === "Party" || label === "Aggregate - Party",
  },
  {
    cf2Label: "4. Political Action Committees",
    description: "PAC (+ Aggregate)",
    matches: (label) => label === "PAC" || label === "Aggregate - PAC",
  },
  {
    cf2Label: "7. Interest Received",
    description: "Interest Received",
    matches: (label) => label === "Interest Received",
  },
  {
    cf2Label: "6. Report of In-Kind Contributions",
    description: "all In-Kind types",
    matches: (label) => IN_KIND_LABEL_PATTERN.test(label),
  },
];

export type RhodeIslandPeriodReconciliationFailure = {
  beginIso: string;
  endIso: string;
  detail: string;
};

function formatCents(cents: number): string {
  return `$${(cents / 100).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

/**
 * Reconcile one period's search-side evidence against its authoritative
 * CF-2. Every failure is a reason the organization's totals must not
 * publish this run.
 */
export function reconcileRhodeIslandPeriodAgainstCf2(input: {
  cf2: RhodeIslandCf2PeriodValues;
  contributionGroupings: ReadonlyMap<string, number>;
  expenditureGroupingsTotalCents: number;
}): RhodeIslandPeriodReconciliationFailure[] {
  const failures: RhodeIslandPeriodReconciliationFailure[] = [];
  const fail = (detail: string): void => {
    failures.push({ beginIso: input.cf2.beginIso, endIso: input.cf2.endIso, detail });
  };

  // Cash receipts: the CF-2's own arithmetic includes every cash grouping —
  // negative return lines too — and excludes in-kind (its own line 6).
  const cashReceiptsCents = [...input.contributionGroupings].reduce(
    (total, [label, cents]) => (IN_KIND_LABEL_PATTERN.test(label) ? total : total + cents),
    0
  );
  if (cashReceiptsCents !== input.cf2.cashReceiptsCents) {
    fail(
      `search cash receipts ${formatCents(cashReceiptsCents)} != CF-2 ` +
        `(${ERTS_CF2_TOTAL_CASH_LABEL} - ${ERTS_CF2_BEGINNING_CASH_LABEL}) ${formatCents(input.cf2.cashReceiptsCents)}`
    );
  }

  if (input.expenditureGroupingsTotalCents !== input.cf2.disbursementsCents) {
    fail(
      `search expenditures ${formatCents(input.expenditureGroupingsTotalCents)} != CF-2 ` +
        `(${ERTS_CF2_TOTAL_CASH_LABEL} - ${ERTS_CF2_ENDING_CASH_LABEL}) ${formatCents(input.cf2.disbursementsCents)}`
    );
  }

  for (const check of ERTS_CF2_LINE_CHECKS) {
    const cf2Cents = input.cf2.values.get(check.cf2Label);
    if (cf2Cents === undefined) {
      // The selector requires every pinned label before a period is usable.
      fail(`CF-2 value for ${JSON.stringify(check.cf2Label)} is missing`);
      continue;
    }
    const groupedCents = [...input.contributionGroupings].reduce(
      (total, [label, cents]) => (check.matches(label) ? total + cents : total),
      0
    );
    if (groupedCents !== cf2Cents) {
      fail(`${check.description} ${formatCents(groupedCents)} != CF-2 ${check.cf2Label} ${formatCents(cf2Cents)}`);
    }
  }
  return failures;
}

// --- Size buckets (georgia boundaries, verbatim) ----------------------------

export function rhodeIslandContributionSizeBucket(amountCents: number): string {
  const amount = amountCents / 100;
  if (amount < 100) return "$1-$99";
  if (amount < 250) return "$100-$249";
  if (amount < 500) return "$250-$499";
  if (amount < 1_000) return "$500-$999";
  if (amount < 5_000) return "$1,000-$4,999";
  return "$5,000+";
}

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function contributorIdentityKey(row: ErtsContributionExportRow): string {
  const name = normalizeTextKey(row.fullName) || normalizeTextKey(`${row.firstName} ${row.lastName}`);
  if (!name) {
    return `unknown-${row.contributionId}`;
  }
  const employer = normalizeTextKey(row.employerName);
  return employer ? `${name}\u0000${employer}` : name;
}

export type RhodeIslandFinanceDirectBreakdown = {
  categoryType: "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type RhodeIslandDirectBucketAggregation = {
  directBreakdowns: RhodeIslandFinanceDirectBreakdown[];
  totalRowCount: number;
  bucketedRowCount: number;
  aggregateRowCount: number;
  nonDonorRowCount: number;
  // Itemized donor rows skipped for a zero/negative amount — they carry no
  // bucket information (georgia parity).
  nonPositiveItemizedRowCount: number;
  partyBuildingRowCount: number;
  partyBuildingCents: number;
  unknownTypeRowCount: number;
  unknownTypeCents: number;
};

/**
 * Bucket the cycle's itemized export rows. Rows from every period are passed
 * together so a donor giving in two periods counts once per bucket
 * (contributor identity = normalized name + employer; georgia
 * `contributorKeys.size` pattern).
 */
export function aggregateRhodeIslandContributionSizeBuckets(input: {
  rows: readonly ErtsContributionExportRow[];
  sourceUrl?: string | null;
}): RhodeIslandDirectBucketAggregation {
  const sourceUrl = input.sourceUrl ?? null;
  const buckets = new Map<string, { amountCents: number; contributorKeys: Set<string> }>();
  let bucketedRowCount = 0;
  let aggregateRowCount = 0;
  let nonDonorRowCount = 0;
  let nonPositiveItemizedRowCount = 0;
  let partyBuildingRowCount = 0;
  let partyBuildingCents = 0;
  let unknownTypeRowCount = 0;
  let unknownTypeCents = 0;

  for (const row of input.rows) {
    const type = row.contributionType;
    if (ERTS_PARTY_BUILDING_CONTRIBUTION_TYPES.has(type)) {
      partyBuildingRowCount += 1;
      partyBuildingCents += row.amountCents;
      continue;
    }
    if (ERTS_BUCKETED_CONTRIBUTION_TYPES.has(type)) {
      if (row.amountCents <= 0) {
        nonPositiveItemizedRowCount += 1;
        continue;
      }
      bucketedRowCount += 1;
      const bucketName = rhodeIslandContributionSizeBucket(row.amountCents);
      const bucket = buckets.get(bucketName) ?? { amountCents: 0, contributorKeys: new Set<string>() };
      bucket.amountCents += row.amountCents;
      bucket.contributorKeys.add(contributorIdentityKey(row));
      buckets.set(bucketName, bucket);
      continue;
    }
    if (ERTS_DONOR_CONTRIBUTION_TYPES.has(type)) {
      // Aggregate roll-ups: in the direct total (via the groupings side),
      // never in buckets (decision 10).
      aggregateRowCount += 1;
      continue;
    }
    if (ERTS_NON_DONOR_CONTRIBUTION_TYPES.has(type)) {
      nonDonorRowCount += 1;
      continue;
    }
    unknownTypeRowCount += 1;
    unknownTypeCents += row.amountCents;
  }

  const directBreakdowns: RhodeIslandFinanceDirectBreakdown[] = [...buckets.entries()]
    .sort((left, right) => right[1].amountCents - left[1].amountCents || left[0].localeCompare(right[0]))
    .map(([categoryName, bucket]) => ({
      categoryType: "contribution_size",
      categoryName,
      amount: bucket.amountCents / 100,
      contributorCount: bucket.contributorKeys.size,
      sourceUrl,
    }));

  return {
    directBreakdowns,
    totalRowCount: input.rows.length,
    bucketedRowCount,
    aggregateRowCount,
    nonDonorRowCount,
    nonPositiveItemizedRowCount,
    partyBuildingRowCount,
    partyBuildingCents,
    unknownTypeRowCount,
    unknownTypeCents,
  };
}

// --- Cycle orchestration over the artifact cache ----------------------------

export type RhodeIslandCycleQuarantineReason = {
  reason:
    | "missing_cf2_label"
    | "duplicate_period_window"
    | "overlapping_periods"
    | "period_outside_cycle"
    | "cf2_reconciliation_mismatch"
    | "party_building_receipts";
  detail: string;
};

export type RhodeIslandCycleFinanceSummary = {
  // Dollars, writer-aligned (StandardStateFinanceSummaryInput).
  totalReceipts: number;
  directContributionTotal: number;
  totalDisbursements: number;
  cashOnHand: number;
  cashOnHandAsOfIso: string;
  sourceUrl: string;
};

export type RhodeIslandCycleFinanceAggregation = {
  orgId: string;
  cycleBeginIso: string;
  cycleEndIso: string;
  // True only when at least one authoritative CF-2 period exists AND nothing
  // quarantined. False with zero periods is decision 12's CF-5 deferral —
  // a valid link with no summary published, not a defect.
  publishable: boolean;
  hasCf2Periods: boolean;
  quarantineReasons: RhodeIslandCycleQuarantineReason[];
  // Null unless publishable.
  summary: RhodeIslandCycleFinanceSummary | null;
  directBreakdowns: RhodeIslandFinanceDirectBreakdown[];
  cf2Selection: RhodeIslandCf2CycleSelection;
  buckets: RhodeIslandDirectBucketAggregation;
  // Summary-grouping labels outside the pinned search vocabulary, summed
  // across periods: reported, excluded from the direct total, never guessed
  // (spike result 5b).
  unknownSummaryLabels: { label: string; cents: number }[];
  directContributionCents: number;
  exportRowCount: number;
  periodCount: number;
};

function expenditureGroupingsTotalCents(html: string): number {
  if (classifyErtsSearchResult(html, ERTS_EXPENDITURE_RESULT_GRID_ID) === "no_rows") {
    return 0;
  }
  return [...parseErtsSummaryGroupings(html, ERTS_EXPENDITURE_SUMMARY_GRID_ID).values()].reduce(
    (total, cents) => total + cents,
    0
  );
}

function isoToUsDate(iso: string): string {
  const [year, month, day] = iso.split("-");
  return `${month}/${day}/${year}`;
}

/**
 * Aggregate one organization's cycle finance from cached artifacts: CF-2
 * period selection, per-period reconciliation, direct total and size
 * buckets. This is PR 7's per-candidate entry point. A missing or stale
 * artifact throws (cache-incomplete is an operational error, isolated per
 * candidate by the sync); a data defect quarantines with reasons instead.
 */
export async function aggregateRhodeIslandOrganizationCycleFinance(input: {
  cacheDir: string;
  orgId: string;
  cycleBeginIso: string;
  cycleEndIso: string;
}): Promise<RhodeIslandCycleFinanceAggregation> {
  const cf2Selection = await selectRhodeIslandCf2CyclePeriods(input);
  const quarantineReasons: RhodeIslandCycleQuarantineReason[] = [...cf2Selection.quarantineReasons];

  const exportRows: ErtsContributionExportRow[] = [];
  const unknownLabelTotals = new Map<string, number>();
  let directContributionCents = 0;
  let partyBuildingGroupingCents = 0;

  for (const period of cf2Selection.periods) {
    const window = { orgId: input.orgId, beginIso: period.beginIso, endIso: period.endIso };
    const report = await readErtsTextArtifact({
      cacheDir: input.cacheDir,
      key: { type: "contribution_report", ...window },
    });
    // Re-classify from the verified bytes rather than trusting manifest
    // metadata: a no-rows window has no groupings and no export artifact.
    const hasRows = classifyErtsSearchResult(report.text, ERTS_CONTRIBUTION_RESULT_GRID_ID) === "rows";
    const groupings = hasRows
      ? parseErtsSummaryGroupings(report.text, ERTS_CONTRIBUTION_SUMMARY_GRID_ID)
      : new Map<string, number>();
    if (hasRows) {
      const exported = await readErtsTextArtifact({
        cacheDir: input.cacheDir,
        key: { type: "contribution_export", ...window },
      });
      exportRows.push(...parseErtsContributionExport(exported.text));
    }

    for (const [label, cents] of groupings) {
      if (ERTS_DONOR_CONTRIBUTION_TYPES.has(label)) {
        directContributionCents += cents;
      } else if (ERTS_PARTY_BUILDING_CONTRIBUTION_TYPES.has(label)) {
        partyBuildingGroupingCents += cents;
      } else if (!ERTS_NON_DONOR_CONTRIBUTION_TYPES.has(label)) {
        unknownLabelTotals.set(label, (unknownLabelTotals.get(label) ?? 0) + cents);
      }
    }

    const expenditureReport = await readErtsTextArtifact({
      cacheDir: input.cacheDir,
      key: { type: "expenditure_report", ...window },
    });
    for (const failure of reconcileRhodeIslandPeriodAgainstCf2({
      cf2: period,
      contributionGroupings: groupings,
      expenditureGroupingsTotalCents: expenditureGroupingsTotalCents(expenditureReport.text),
    })) {
      quarantineReasons.push({
        reason: "cf2_reconciliation_mismatch",
        detail: `${failure.beginIso}..${failure.endIso}: ${failure.detail}`,
      });
    }
  }

  const sourceUrl = ertsContributionReportUrl({
    orgId: input.orgId,
    begin: isoToUsDate(cf2Selection.cycleBeginIso),
    end: isoToUsDate(cf2Selection.cycleEndIso),
  });
  const buckets = aggregateRhodeIslandContributionSizeBuckets({ rows: exportRows, sourceUrl });
  if (partyBuildingGroupingCents !== 0 || buckets.partyBuildingRowCount > 0) {
    quarantineReasons.push({
      reason: "party_building_receipts",
      detail:
        `party-building money on a candidate organization: groupings ${formatCents(partyBuildingGroupingCents)}, ` +
        `${buckets.partyBuildingRowCount} export rows ${formatCents(buckets.partyBuildingCents)}`,
    });
  }

  const hasCf2Periods = cf2Selection.periods.length > 0;
  const publishable = hasCf2Periods && quarantineReasons.length === 0;
  const cycleTotals = cf2Selection.cycleTotals;
  return {
    orgId: input.orgId,
    cycleBeginIso: cf2Selection.cycleBeginIso,
    cycleEndIso: cf2Selection.cycleEndIso,
    publishable,
    hasCf2Periods,
    quarantineReasons,
    summary:
      publishable && cycleTotals
        ? {
            totalReceipts: cycleTotals.totalReceiptsCents / 100,
            directContributionTotal: directContributionCents / 100,
            totalDisbursements: cycleTotals.totalDisbursementsCents / 100,
            cashOnHand: cycleTotals.cashOnHandCents / 100,
            cashOnHandAsOfIso: cycleTotals.cashOnHandAsOfIso,
            sourceUrl,
          }
        : null,
    directBreakdowns: publishable ? buckets.directBreakdowns : [],
    cf2Selection,
    buckets,
    unknownSummaryLabels: [...unknownLabelTotals.entries()]
      .map(([label, cents]) => ({ label, cents }))
      .sort((left, right) => left.label.localeCompare(right.label)),
    directContributionCents,
    exportRowCount: exportRows.length,
    periodCount: cf2Selection.periods.length,
  };
}
