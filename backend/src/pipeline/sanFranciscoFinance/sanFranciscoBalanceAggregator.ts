// Cash, debt, and loan figures from a committee's Form 460 summary filings
// (plan Phase 4). Ending balances are point-in-time lines, so they come from
// the LATEST filing only; loans received are per-period, so they sum across
// filings. The no-duplicate-filing_nid source guarantee these sums rely on
// is asserted where the rows are fetched (getSanFranciscoCommitteeSummaryRows
// throws on a duplicate), so a broken guarantee can never reach this math.
//
// Deliberately NOT election-windowed: SF committees are strictly
// per-election — the same candidates running in both the June and November
// 2026 D4 races appear in the SFEC manifests with DIFFERENT committee FPPC
// ids (Gee 1484806/1490199, Wong 1485709/1489126, verified 2026-08-08) — so
// one committee's full filing history IS one election. Post-election
// filings are that campaign's wind-down, and cutting them with a date
// window would report stale balances: the gate committee's final filing
// (period end 2025-01-08, two months after election day) is the one that
// carries the real ending cash ($0) and the $199,970.62 loan forgiveness.
import type { SanFranciscoSummaryRow } from "./sanFranciscoOpenDataClient.js";

export type SanFranciscoBalanceAggregate = {
  /** Form 460 line 16 of the latest filing, cents; null without a 460. */
  cashOnHandCents: number | null;
  /** Form 460 line 19 of the latest filing, cents; null without a 460. */
  debtsOwedCents: number | null;
  /**
   * Schedule B1 line 1 summed across all Form 460 filings, cents — gross new
   * borrowing. NEVER part of total_raised: the Phase 4 gate proved loan
   * principal is absent from the line-5 contributions the dashboard funds
   * figure builds on (shared-contract loans_received semantics).
   */
  loansReceivedCents: number;
  /** Period end of the filing the balances came from (raw source string). */
  latestFilingPeriodEnd: string | null;
  form460Filings: number;
};

export function aggregateSanFranciscoBalances(
  summaryRows: readonly SanFranciscoSummaryRow[],
): SanFranciscoBalanceAggregate {
  // Only Form 460 filings carry the summary-page balance lines; the dataset
  // also holds FPPC450/461/465 rows whose line columns are empty.
  const form460Rows = summaryRows.filter((row) => row.formType === "FPPC460");
  // Latest filing by period end (ISO strings compare correctly), start date
  // and filing id as deterministic tiebreakers. Overlapping periods are real
  // upstream (a committee re-filed Jan-Sep over a Jan-Jun original, verified
  // live on committee 1467508) and the max period end picks the filing whose
  // beginning-cash chain continues.
  const sortKey = (row: SanFranciscoSummaryRow): [string, string, string] => [
    row.periodEnd ?? "",
    row.periodStart ?? "",
    // Numeric ids padded so "99" never outranks "100" lexicographically.
    row.filingIdNumber.padStart(20, "0"),
  ];
  const isLater = (a: SanFranciscoSummaryRow, b: SanFranciscoSummaryRow) => {
    const keyA = sortKey(a);
    const keyB = sortKey(b);
    for (let index = 0; index < keyA.length; index += 1) {
      if (keyA[index]! !== keyB[index]!) return keyA[index]! > keyB[index]!;
    }
    return false;
  };
  let latest: SanFranciscoSummaryRow | null = null;
  for (const row of form460Rows) {
    if (latest === null || isLater(row, latest)) latest = row;
  }
  return {
    cashOnHandCents: latest?.endingCashCents ?? null,
    debtsOwedCents: latest?.outstandingDebtsCents ?? null,
    loansReceivedCents: form460Rows.reduce(
      (sum, row) => sum + (row.loansReceivedCents ?? 0),
      0,
    ),
    latestFilingPeriodEnd: latest?.periodEnd ?? null,
    form460Filings: form460Rows.length,
  };
}
