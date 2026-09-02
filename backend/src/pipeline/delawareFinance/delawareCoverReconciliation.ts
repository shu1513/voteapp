// Delaware cover reconciliation (plan-delaware-finance.md fact 6).
//
// Two identities, both mandatory before anything is published:
// - Per-report cash identity (beginning + receipts − expenditures = ending)
//   is enforced by the PDF cover extractor itself, and chain continuity by
//   the inventory builder.
// - PER-PERIOD transaction reconciliation: every canonical filing period's
//   CSV receipt/expense rows must sum cent-exact to that period's cover
//   subtotals (2E / 3J). Per-period — never one grand total, where
//   cross-period differences could cancel — this is what proves the
//   transaction search returned current-version rows only for THIS
//   committee on THIS run (the probe proved the portal behavior; the sync
//   re-proves it every run and fails closed if a mid-amendment fetch window
//   ever breaks it).

import {
  parseDelawareAmountCents,
  type DelawareExpenseCsvRow,
  type DelawareReceiptCsvRow,
} from "./delawareCfrsParsers.js";
import { delawareFilingPeriodKey, type DelawareCanonicalReport } from "./delawareReportInventory.js";

export type DelawarePeriodReconciliation = {
  periodKey: string;
  csvReceiptsCents: number;
  coverReceiptsCents: number | null;
  csvExpensesCents: number;
  coverExpendituresCents: number | null;
  matches: boolean;
};

export type DelawareCoverReconciliationResult = {
  periods: DelawarePeriodReconciliation[];
  mismatchedPeriods: DelawarePeriodReconciliation[];
  ok: boolean;
};

function sumByPeriodKey(rows: readonly { period: string; amountCents: number }[]): Map<string, number> {
  const sums = new Map<string, number>();
  for (const row of rows) {
    const key = delawareFilingPeriodKey(row.period);
    sums.set(key, (sums.get(key) ?? 0) + row.amountCents);
  }
  return sums;
}

/**
 * Reconciles the full CSV row sets against the full canonical inventory,
 * period by period. A period present on either side must match cent-exact
 * on both receipts and expenditures; a CSV period without a canonical cover
 * (or vice versa, with nonzero cover money and no rows summing to it) is a
 * mismatch. Callers fail closed on `ok === false`.
 */
export function reconcileDelawareCoversPerPeriod(input: {
  canonicalReports: readonly DelawareCanonicalReport[];
  receiptRows: readonly DelawareReceiptCsvRow[];
  expenseRows: readonly DelawareExpenseCsvRow[];
}): DelawareCoverReconciliationResult {
  const receiptSums = sumByPeriodKey(
    input.receiptRows.map((row) => ({
      period: row["Filing Period"],
      amountCents: parseDelawareAmountCents(row["Contribution Amount"]),
    }))
  );
  const expenseSums = sumByPeriodKey(
    input.expenseRows.map((row) => ({
      period: row["Filing Period"],
      amountCents: parseDelawareAmountCents(row["Amount($)"]),
    }))
  );
  const coverByKey = new Map(input.canonicalReports.map((report) => [report.periodKey, report]));
  const allKeys = [...new Set([...coverByKey.keys(), ...receiptSums.keys(), ...expenseSums.keys()])].sort();

  const periods: DelawarePeriodReconciliation[] = allKeys.map((periodKey) => {
    const cover = coverByKey.get(periodKey);
    const csvReceiptsCents = receiptSums.get(periodKey) ?? 0;
    const csvExpensesCents = expenseSums.get(periodKey) ?? 0;
    const matches =
      cover !== undefined &&
      cover.receiptsCents === csvReceiptsCents &&
      cover.expendituresCents === csvExpensesCents;
    return {
      periodKey,
      csvReceiptsCents,
      coverReceiptsCents: cover?.receiptsCents ?? null,
      csvExpensesCents,
      coverExpendituresCents: cover?.expendituresCents ?? null,
      matches,
    };
  });
  const mismatchedPeriods = periods.filter((period) => !period.matches);
  return { periods, mismatchedPeriods, ok: mismatchedPeriods.length === 0 };
}
