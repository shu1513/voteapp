// Alabama direct-finance aggregation: contribution-size buckets from the
// cached cash extracts, gated by the cash coverage ratio.
//
// Money model (plan-alabama-finance.md, Summary mapping): the SUMMARY comes
// from the live race row and is never touched here. This module only builds
// the size buckets and the coverage evidence that gates them:
// - coverage cash = signed sum of every non-in-kind cash row for the FCPA
//   committee across the transaction-date-year window (itemized +
//   non-itemized + payroll; the cash file embeds in-kind rows that must not
//   be counted — gotcha 3).
// - buckets = positive "Cash (Itemized)" rows only — no in-kind, no
//   non-itemized, no `Returned (Cash Only)` rows, and negative rows are
//   excluded from buckets while still counting in coverage cash (signed,
//   never abs()'d).
// - coverage ratio = coverage cash ÷ race MONETARYCONTRIB, and buckets are
//   only trustworthy inside [0.97, 1.01] (extracts lag one day and can
//   permanently omit filed rows; above 1.01, or a zero race total with
//   nonzero extract cash, means a bad committee join). Outside the band the
//   sync writes the summary with NO buckets plus a diagnostic — coverage
//   gates buckets only, never the summary.

import type { AlabamaCashRow } from "./alabamaFcpaCsv.js";

export const ALABAMA_CASH_COVERAGE_MIN = 0.97;
export const ALABAMA_CASH_COVERAGE_MAX = 1.01;

export const ALABAMA_DIRECT_COVERAGE_NOTE =
  "Contribution-size amounts cover itemized cash contributions only; the raised total also includes non-itemized cash, in-kind contributions, and other receipts.";

const ITEMIZED_CASH_TYPE = "Cash (Itemized)";
const RETURNED_CONTRIBUTOR_TYPE = "Returned (Cash Only)";

function isInKindType(contributionType: string): boolean {
  return contributionType.toUpperCase().startsWith("IN-KIND");
}

/** Delaware bucket edges — the fleet's contribution-size vocabulary. */
function sizeBucket(amountCents: number): string {
  if (amountCents < 10_000) return "$1-$99";
  if (amountCents < 25_000) return "$100-$249";
  if (amountCents < 50_000) return "$250-$499";
  if (amountCents < 100_000) return "$500-$999";
  if (amountCents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

export type AlabamaFinanceDirectBreakdown = {
  categoryType: "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
};

export type AlabamaDirectFinanceAggregationResult = {
  /** Signed non-in-kind cash for the committee across the window. */
  coverageCashCents: number;
  /** coverageCashCents ÷ race MONETARYCONTRIB; null when the race total is 0. */
  coverageRatio: number | null;
  /** True only when the buckets may be shown. */
  bucketsUsable: boolean;
  /** Why bucketsUsable is false; empty when usable. */
  bucketDiagnostics: string[];
  breakdowns: AlabamaFinanceDirectBreakdown[];
  committeeRowCount: number;
  inKindRowCount: number;
  nonItemizedCashCents: number;
  returnedRowCount: number;
  negativeOrZeroItemizedRowCount: number;
};

export function aggregateAlabamaDirectFinance(input: {
  /** Parsed cash rows from every window-year artifact, all committees. */
  cashRows: readonly AlabamaCashRow[];
  /** Extract CommitteeId (the FCPA committee number) to aggregate. */
  fcpaCommitteeNumber: string;
  /** Race-row MONETARYCONTRIB in dollars (the authoritative cash total). */
  authoritativeCashContrib: number;
}): AlabamaDirectFinanceAggregationResult {
  let coverageCashCents = 0;
  let committeeRowCount = 0;
  let inKindRowCount = 0;
  let nonItemizedCashCents = 0;
  let returnedRowCount = 0;
  let negativeOrZeroItemizedRowCount = 0;
  const bucketTotals = new Map<string, { cents: number; contributors: Set<string> }>();

  for (const row of input.cashRows) {
    if (row.committeeId !== input.fcpaCommitteeNumber) {
      continue;
    }
    committeeRowCount += 1;
    if (isInKindType(row.contributionType)) {
      inKindRowCount += 1;
      continue;
    }
    coverageCashCents += row.amountCents;
    if (row.contributionType !== ITEMIZED_CASH_TYPE) {
      nonItemizedCashCents += row.amountCents;
      continue;
    }
    if (row.contributorType === RETURNED_CONTRIBUTOR_TYPE) {
      returnedRowCount += 1;
      continue;
    }
    if (row.amountCents <= 0) {
      negativeOrZeroItemizedRowCount += 1;
      continue;
    }
    const contributorKey = `${row.lastName}|${row.firstName}`.toUpperCase();
    const bucket = sizeBucket(row.amountCents);
    const entry = bucketTotals.get(bucket) ?? { cents: 0, contributors: new Set<string>() };
    entry.cents += row.amountCents;
    entry.contributors.add(contributorKey);
    bucketTotals.set(bucket, entry);
  }

  const bucketDiagnostics: string[] = [];
  let coverageRatio: number | null = null;
  if (input.authoritativeCashContrib > 0) {
    coverageRatio = coverageCashCents / 100 / input.authoritativeCashContrib;
    if (coverageRatio < ALABAMA_CASH_COVERAGE_MIN || coverageRatio > ALABAMA_CASH_COVERAGE_MAX) {
      bucketDiagnostics.push(
        `cash_coverage_out_of_tolerance: ${coverageRatio.toFixed(4)} outside [${ALABAMA_CASH_COVERAGE_MIN}, ${ALABAMA_CASH_COVERAGE_MAX}]`
      );
    }
  } else if (coverageCashCents !== 0) {
    // Zero authoritative cash with nonzero extract cash = the FCPA number
    // aggregates a different committee than the linked one (bad join). Never
    // show buckets.
    bucketDiagnostics.push(
      `zero_authoritative_cash_nonzero_extract_cash: ${(coverageCashCents / 100).toFixed(2)}`
    );
  }

  const bucketsUsable = bucketDiagnostics.length === 0;
  const breakdowns: AlabamaFinanceDirectBreakdown[] = bucketsUsable
    ? [...bucketTotals.entries()]
        .sort((left, right) => right[1].cents - left[1].cents || left[0].localeCompare(right[0]))
        .map(([name, entry]) => ({
          categoryType: "contribution_size" as const,
          categoryName: name,
          amount: entry.cents / 100,
          contributorCount: entry.contributors.size,
        }))
    : [];

  return {
    coverageCashCents,
    coverageRatio,
    bucketsUsable,
    bucketDiagnostics,
    breakdowns,
    committeeRowCount,
    inKindRowCount,
    nonItemizedCashCents,
    returnedRowCount,
    negativeOrZeroItemizedRowCount,
  };
}
