import type { OhioSosContributionRow, OhioSosCoverPageRow } from "./ohioSosBulkFiles.js";

// Direct-money aggregation for one Ohio candidate committee over one
// election cycle (ohio_plan.md PR 6). Itemized receipts come from the
// CAC_CON_{Y-1,Y} bulk files; disbursements and cash on hand come from the
// candidate cover pages, whose reports chain per period with no amendment
// duplicates (verified on the real 2026-cycle files: AMT_FORWARD equals the
// previous report's BALANCE_ON_HAND, zero duplicate report keys, zero
// AMENDED descriptions).
//
// Decision 6: Ohio ships WITHOUT occupation breakdowns — the combined
// EMP_OCCUPATION field is ~80% non-occupations. Contribution-size buckets
// are the only direct breakdown, using the shared bucket boundaries.
//
// The ~90 MB contribution files must never be materialized (decision 10),
// so this is an accumulator: the caller streams each file once and feeds
// every row to every open accumulator; rows for other committees are
// ignored cheaply.

export type OhioDirectFinanceSummary = {
  // Cover-page receipts (TOTAL_CONTRIBUTIONS + TOTAL_OTHER_INCOME summed
  // over the cycle's reports) when the committee has cover rows, else the
  // itemized sum. Cover is authoritative: non-itemized income is real and
  // large (Ramaswamy's $25.4M federal-account transfer appears only on a
  // cover page), and the cover totals satisfy the accounting identity
  // receipts − disbursements + forward = cash exactly on the real files.
  totalReceipts: number;
  directContributionTotal: number;
  // Null when the committee has no cover rows in the cycle window, or (cash
  // only) when the latest report's balance is negative (decision 1).
  totalDisbursements: number | null;
  cashOnHand: number | null;
  sourceUrl: string | null;
};

export type OhioFinanceDirectBreakdown = {
  categoryType: "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type OhioDirectContributionAggregationResult = {
  summary: OhioDirectFinanceSummary;
  directBreakdowns: OhioFinanceDirectBreakdown[];
  // Rows carrying this committee's MASTER_KEY.
  matchedContributionRowCount: number;
  // Matched rows that entered the direct-contribution total and buckets.
  includedContributionRowCount: number;
  // Matched rows that did not: matched = included + skipped, and the five
  // reason counters below partition skipped (each skipped row is counted in
  // exactly one of them).
  skippedContributionRowCount: number;
  missingAmountRowCount: number;
  nonPositiveAmountRowCount: number;
  outOfCycleRowCount: number;
  // The last two skip reasons still count toward itemized receipts — the
  // row's money is real, it is only excluded from the direct total and the
  // buckets. otherIncomeRowCount is 31-A-2 Other Income;
  // unknownShortDescriptionRowCount is a SHORT_DESCRIPTION outside the
  // pinned vocabulary, so a growing count means the portal added a form
  // type.
  otherIncomeRowCount: number;
  unknownShortDescriptionRowCount: number;
  coverReportCount: number;
  // Cycle cover rows with every money column blank (real: 3 of 1,673 rows
  // on the 2026-cycle file, e-filing damage). They are excluded from the
  // sums, the latest-report pick, and coverReportCount — a committee with
  // only blank rows falls back to itemized receipts and NULL
  // disbursements/cash instead of publishing fabricated zeroes.
  blankCoverRowCount: number;
  // Latest cover report's balance was negative → cashOnHand null.
  negativeBalanceOnHand: boolean;
  // Advisory reconciliation pair: the itemized in-cycle receipts sum vs the
  // cover-page receipts. In-kind receipts are itemized but absent from the
  // cover totals while non-itemized other income is only on the cover, so
  // modest differences in either direction are normal; a large gap means
  // artifact damage. coverReceiptsTotal is null without cover rows.
  itemizedReceiptsTotal: number;
  coverReceiptsTotal: number | null;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

// The pinned CAC_CON SHORT_DESCRIPTION vocabulary, from the real 2026-cycle
// files. Donor support feeds the direct total and size buckets; other
// income counts toward receipts only. Anything new fails closed into
// unknownShortDescriptionRowCount.
const DIRECT_DONOR_SUPPORT_SHORT_DESCRIPTIONS = new Set([
  "31-A STMT OF CONTRIBUTION",
  "31-E FR CONTRIBUTIONS",
  "31-J-1 IN-KIND CONT RCVD",
]);
const OTHER_INCOME_SHORT_DESCRIPTIONS = new Set(["31-A-2 OTHER INCOME"]);

function normalizeShortDescription(value: string | null): string {
  return (value ?? "").toUpperCase().replace(/\s+/g, " ").trim();
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function requireOhioMasterKey(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!/^[0-9]+$/.test(trimmed)) {
    throw new Error(`${fieldName} must be a numeric Ohio SOS master key`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Ohio direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Ohio direct contribution aggregation ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

// Shared bucket boundaries (maryland parity — the UI renders both).
function contributionSizeBucket(amountCents: number): string {
  if (amountCents < 100_00) {
    return "$1-$99";
  }
  if (amountCents < 250_00) {
    return "$100-$249";
  }
  if (amountCents < 500_00) {
    return "$250-$499";
  }
  if (amountCents < 1_000_00) {
    return "$500-$999";
  }
  if (amountCents < 5_000_00) {
    return "$1,000-$4,999";
  }
  return "$5,000+";
}

// A cover row whose every money column is blank carries no data at all
// (observed as rare e-filing damage). A PARTIALLY blank row is different:
// on the real 2026-cycle file all 583 rows with a blank TOTAL_CONTRIBUTIONS
// satisfy TOTAL_FUNDS = AMT_FORWARD + TOTAL_OTHER_INCOME exactly, so a
// blank money cell on an otherwise-filled row provably means zero and the
// `?? 0` sums below are correct.
function isBlankCoverRow(row: OhioSosCoverPageRow): boolean {
  return (
    row.amountForwardCents === null &&
    row.totalContributionsCents === null &&
    row.totalOtherIncomeCents === null &&
    row.totalFundsCents === null &&
    row.totalExpendituresCents === null &&
    row.balanceOnHandCents === null &&
    row.valueInkindReceivedCents === null &&
    row.valueInkindMadeCents === null &&
    row.outstandingLoansOwedCents === null &&
    row.outstandingDebtOwedCents === null &&
    row.outstandingLoansToCents === null &&
    row.valueIndependentExpendituresCents === null
  );
}

function contributorIdentityKey(row: OhioSosContributionRow, rowIndex: number): string {
  const parts = [
    row.nonIndividual,
    row.contributorLastName,
    row.contributorFirstName,
    row.contributorMiddleName,
    row.contributorSuffix,
    row.address,
    row.city,
    row.state,
    row.zip,
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : `unknown-row-${rowIndex}`;
}

type BucketAccumulator = {
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

export type OhioDirectContributionAccumulator = {
  add(row: OhioSosContributionRow): void;
  finish(input: { coverRows: readonly OhioSosCoverPageRow[] }): OhioDirectContributionAggregationResult;
};

export function createOhioDirectContributionAccumulator(input: {
  committeeId: string;
  electionYear: number;
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
}): OhioDirectContributionAccumulator {
  const committeeId = requireOhioMasterKey(input.committeeId, "Ohio committee id");
  const electionYear = normalizeElectionYear(input.electionYear);
  const cycleStartYear = electionYear - 1;
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;

  const buckets = new Map<string, BucketAccumulator>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;
  let missingAmountRowCount = 0;
  let nonPositiveAmountRowCount = 0;
  let outOfCycleRowCount = 0;
  let otherIncomeRowCount = 0;
  let unknownShortDescriptionRowCount = 0;
  let totalReceiptsCents = 0;
  let directContributionTotalCents = 0;
  let rowIndex = 0;
  let finished = false;

  function add(row: OhioSosContributionRow): void {
    if (finished) {
      throw new Error("Ohio direct contribution accumulator already finished");
    }
    rowIndex += 1;
    if (row.masterKey.trim() !== committeeId) {
      return;
    }
    matchedContributionRowCount += 1;

    if (row.amountCents === null) {
      missingAmountRowCount += 1;
      skippedContributionRowCount += 1;
      return;
    }
    if (row.amountCents <= 0) {
      nonPositiveAmountRowCount += 1;
      skippedContributionRowCount += 1;
      return;
    }
    // The bulk files are already RPT_YEAR-scoped; a row outside the cycle
    // window means the caller fed the wrong file (or portal damage), so it
    // is skipped and counted rather than silently included.
    if (row.reportYear === null || row.reportYear < cycleStartYear || row.reportYear > electionYear) {
      outOfCycleRowCount += 1;
      skippedContributionRowCount += 1;
      return;
    }

    totalReceiptsCents += row.amountCents;
    const shortDescription = normalizeShortDescription(row.shortDescription);
    if (!DIRECT_DONOR_SUPPORT_SHORT_DESCRIPTIONS.has(shortDescription)) {
      if (OTHER_INCOME_SHORT_DESCRIPTIONS.has(shortDescription)) {
        otherIncomeRowCount += 1;
      } else {
        unknownShortDescriptionRowCount += 1;
      }
      skippedContributionRowCount += 1;
      return;
    }

    includedContributionRowCount += 1;
    directContributionTotalCents += row.amountCents;
    const categoryName = contributionSizeBucket(row.amountCents);
    const bucket = buckets.get(categoryName);
    const contributorKey = contributorIdentityKey(row, rowIndex);
    if (bucket) {
      bucket.amountCents += row.amountCents;
      bucket.contributorKeys.add(contributorKey);
    } else {
      buckets.set(categoryName, {
        categoryName,
        amountCents: row.amountCents,
        contributorKeys: new Set([contributorKey]),
      });
    }
  }

  function finish(finishInput: {
    coverRows: readonly OhioSosCoverPageRow[];
  }): OhioDirectContributionAggregationResult {
    if (finished) {
      throw new Error("Ohio direct contribution accumulator already finished");
    }
    finished = true;

    const cycleRows = finishInput.coverRows.filter(
      (row) =>
        row.masterKey.trim() === committeeId &&
        row.reportYear !== null &&
        row.reportYear >= cycleStartYear &&
        row.reportYear <= electionYear
    );
    const blankCoverRowCount = cycleRows.filter(isBlankCoverRow).length;
    const cycleCoverRows = cycleRows.filter((row) => !isBlankCoverRow(row));

    let totalDisbursementsCents: number | null = null;
    let coverReceiptsCents: number | null = null;
    let latestCoverRow: OhioSosCoverPageRow | null = null;
    for (const row of cycleCoverRows) {
      totalDisbursementsCents = (totalDisbursementsCents ?? 0) + (row.totalExpendituresCents ?? 0);
      coverReceiptsCents =
        (coverReceiptsCents ?? 0) + (row.totalContributionsCents ?? 0) + (row.totalOtherIncomeCents ?? 0);
      if (
        latestCoverRow === null ||
        (row.dateReportFiledIso ?? "") > (latestCoverRow.dateReportFiledIso ?? "") ||
        ((row.dateReportFiledIso ?? "") === (latestCoverRow.dateReportFiledIso ?? "") &&
          Number(row.reportKey) > Number(latestCoverRow.reportKey))
      ) {
        latestCoverRow = row;
      }
    }

    // Decision 1: the canonical schema rejects negative cash, so a negative
    // balance is written as NULL and surfaced in diagnostics — never
    // clamped.
    const latestBalanceCents = latestCoverRow?.balanceOnHandCents ?? null;
    const negativeBalanceOnHand = latestBalanceCents !== null && latestBalanceCents < 0;
    const cashOnHandCents = latestBalanceCents !== null && latestBalanceCents >= 0 ? latestBalanceCents : null;

    const directBreakdowns: OhioFinanceDirectBreakdown[] = [...buckets.values()]
      .sort(
        (left, right) =>
          right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName)
      )
      .slice(0, maxBreakdownsPerCategory)
      .map((bucket) => ({
        categoryType: "contribution_size" as const,
        categoryName: bucket.categoryName,
        amount: centsToDollars(bucket.amountCents),
        contributorCount: bucket.contributorKeys.size,
        sourceUrl,
      }));

    return {
      summary: {
        totalReceipts: centsToDollars(coverReceiptsCents ?? totalReceiptsCents),
        directContributionTotal: centsToDollars(directContributionTotalCents),
        totalDisbursements: totalDisbursementsCents === null ? null : centsToDollars(totalDisbursementsCents),
        cashOnHand: cashOnHandCents === null ? null : centsToDollars(cashOnHandCents),
        sourceUrl,
      },
      directBreakdowns,
      matchedContributionRowCount,
      includedContributionRowCount,
      skippedContributionRowCount,
      missingAmountRowCount,
      nonPositiveAmountRowCount,
      outOfCycleRowCount,
      otherIncomeRowCount,
      unknownShortDescriptionRowCount,
      coverReportCount: cycleCoverRows.length,
      blankCoverRowCount,
      negativeBalanceOnHand,
      itemizedReceiptsTotal: centsToDollars(totalReceiptsCents),
      coverReceiptsTotal: coverReceiptsCents === null ? null : centsToDollars(coverReceiptsCents),
    };
  }

  return { add, finish };
}
