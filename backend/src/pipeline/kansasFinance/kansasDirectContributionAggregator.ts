// Kansas direct-side aggregation (plan-kansas-finance.md, Phase 4).
//
// Step 1 — cover-sourced totals. The Receipts and Expenditures cover
// (K.S.A. 25-4148 form; parseKansasReportCover) carries seven lines:
//   1 cash on hand at beginning of period
//   2 total contributions and other receipts (Schedule A)
//   3 cash available this period (1 + 2)
//   4 total expenditures and other disbursements (Schedule C)
//   5 cash on hand at close of period (3 - 4)
//   6 in-kind (non-monetary) contributions (Schedule B)
//   7 other transactions (Schedule D)
// Totals are the sum of lines 2, 4 and 6 over the CANONICAL version of
// every period in the cycle ledger (kansasReportInventory.ts), and cash on
// hand is line 5 of the latest canonical report. Line 6 stays separate
// from line 2: the form keeps in-kind out of receipts, and so does this.
//
// Fail closed, per candidate: a cover that fails its own arithmetic, a
// canonical version with no opened cover (a paper scan — its totals come
// from OCR in a later step), or a ledger that is not complete leaves the
// candidate unpublishable. Last-minute reports duplicate into the next
// regular report and never count; prior-cycle filings are ignored.
//
// Pure. All arithmetic in integer cents.

import { reconcileKansasCoverArithmetic, type KansasReportCover } from "./kansasCfrViewerParsers.js";
import { kansasFilingHeaderKey, type KansasFilingHeader, type KansasLedger, type KansasLedgerEntry } from "./kansasReportInventory.js";

export type KansasOpenedCover = {
  /** The header the ledger was built from (kansasCandidateLedger `reports[].header`). */
  header: KansasFilingHeader;
  cover: KansasReportCover;
};

export type KansasCoverTotalsPeriod = {
  key: string;
  status: KansasLedgerEntry["status"];
  /** The canonical version's cover; null when the period has no counted filing. */
  cover: KansasReportCover | null;
};

export type KansasCoverTotals =
  | {
      status: "unpublishable";
      /** One line per blocker ("2025-annual: cover arithmetic failed"). */
      reasons: string[];
      periods: KansasCoverTotalsPeriod[];
    }
  | {
      status: "ok";
      /** Sum of line 2 over canonical covers. */
      totalReceiptsCents: number;
      /** Sum of line 4. */
      totalDisbursementsCents: number;
      /** Sum of line 6. */
      inKindCents: number;
      /**
       * Line 5 of the latest canonical report (by period end); null when no
       * period was filed (affidavit / not-required cycle) or the figure is
       * negative (the summaries table rejects it — "not reported" is honest).
       */
      cashOnHandCents: number | null;
      /** Non-blocking observations (a period's line 1 not equal to the prior filed period's line 5). */
      diagnostics: string[];
      periods: KansasCoverTotalsPeriod[];
    };

const COUNTED: ReadonlySet<KansasLedgerEntry["status"]> = new Set(["report_filed", "amended"]);

export function aggregateKansasCoverTotals(input: { ledger: KansasLedger; covers: readonly KansasOpenedCover[] }): KansasCoverTotals {
  const coversByKey = new Map<string, KansasReportCover[]>();
  for (const opened of input.covers) {
    const key = kansasFilingHeaderKey(opened.header);
    coversByKey.set(key, [...(coversByKey.get(key) ?? []), opened.cover]);
  }

  const reasons: string[] = [];
  const diagnostics: string[] = [];
  const periods: KansasCoverTotalsPeriod[] = [];
  let totalReceiptsCents = 0;
  let totalDisbursementsCents = 0;
  let inKindCents = 0;
  let latest: { periodEnd: string; cover: KansasReportCover } | null = null;
  let previousClose: { key: string; cents: number } | null = null;

  if (!input.ledger.complete) reasons.push("ledger incomplete");

  for (const entry of input.ledger.entries) {
    const { key } = entry.period;
    if (!COUNTED.has(entry.status)) {
      periods.push({ key, status: entry.status, cover: null });
      continue;
    }
    const canonical = entry.canonical;
    if (canonical === null) {
      // Unreachable by construction (report_filed/amended require a canonical); named so a regression surfaces.
      reasons.push(`${key}: counted period has no canonical version`);
      periods.push({ key, status: entry.status, cover: null });
      continue;
    }
    const matches = coversByKey.get(kansasFilingHeaderKey(canonical)) ?? [];
    if (matches.length !== 1) {
      reasons.push(
        matches.length === 0
          ? `${key}: no opened cover for the canonical ${canonical.channel} version`
          : `${key}: ${matches.length} opened covers match the canonical version`
      );
      periods.push({ key, status: entry.status, cover: null });
      continue;
    }
    const cover = matches[0]!;
    periods.push({ key, status: entry.status, cover });
    if (!reconcileKansasCoverArithmetic(cover)) {
      reasons.push(`${key}: cover arithmetic failed`);
      continue;
    }
    if (cover.inKindCents === null) {
      reasons.push(`${key}: cover line 6 (in-kind) unparsed`);
      continue;
    }
    // reconcileKansasCoverArithmetic proved these non-null.
    totalReceiptsCents += cover.totalContributionsCents!;
    totalDisbursementsCents += cover.totalExpendituresCents!;
    inKindCents += cover.inKindCents;
    if (previousClose !== null && cover.cashBeginningCents !== previousClose.cents) {
      diagnostics.push(`${key}: line 1 ${cover.cashBeginningCents} differs from ${previousClose.key} line 5 ${previousClose.cents}`);
    }
    previousClose = { key, cents: cover.cashCloseCents! };
    if (latest === null || entry.period.end > latest.periodEnd) latest = { periodEnd: entry.period.end, cover };
  }

  if (reasons.length > 0) return { status: "unpublishable", reasons, periods };

  const close = latest === null ? null : latest.cover.cashCloseCents!;
  if (close !== null && close < 0) diagnostics.push(`cash on hand ${close} is negative; reported as null`);
  return {
    status: "ok",
    totalReceiptsCents,
    totalDisbursementsCents,
    inKindCents,
    cashOnHandCents: close === null || close < 0 ? null : close,
    diagnostics,
    periods,
  };
}
