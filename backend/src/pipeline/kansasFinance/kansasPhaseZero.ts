// Phase 0 reconciliation logic for Kansas (plan-kansas-finance.md).
// Pure functions only — the probe script wires them to live pages/PDFs.

import type { KansasContributionExportRow, KansasReportCover, KansasScheduleATotals, KansasScheduleCTotals } from "./kansasCfrViewerParsers.js";
import { parseKansasOcrMoneyCents, type KansasOcrMoney } from "./kansasCfrViewerParsers.js";

// ---------------------------------------------------------------------------
// Contribution export summary (occupation coverage math).

export type KansasExportSummary = {
  rowCount: number;
  monetaryRowCount: number;
  monetaryTotalCents: number;
  inKindTotalCents: number;
  unparsedAmountRowCount: number;
  occupationFilledRowCount: number;
  /** Occupation-covered share of itemized monetary dollars (0..1). */
  occupationCoveredMonetaryShare: number | null;
};

export function summarizeKansasContributionExport(
  rows: readonly KansasContributionExportRow[]
): KansasExportSummary {
  let monetaryRowCount = 0;
  let monetaryTotalCents = 0;
  let inKindTotalCents = 0;
  let unparsedAmountRowCount = 0;
  let occupationFilledRowCount = 0;
  let occupationCoveredMonetaryCents = 0;
  for (const row of rows) {
    if (row.amountCents !== null) {
      monetaryRowCount += 1;
      monetaryTotalCents += row.amountCents;
      if (row.occupation !== "") occupationCoveredMonetaryCents += row.amountCents;
    }
    if (row.inKindAmountCents !== null) inKindTotalCents += row.inKindAmountCents;
    if (row.amountCents === null && row.inKindAmountCents === null) unparsedAmountRowCount += 1;
    if (row.occupation !== "") occupationFilledRowCount += 1;
  }
  return {
    rowCount: rows.length,
    monetaryRowCount,
    monetaryTotalCents,
    inKindTotalCents,
    unparsedAmountRowCount,
    occupationFilledRowCount,
    occupationCoveredMonetaryShare:
      monetaryTotalCents > 0 ? occupationCoveredMonetaryCents / monetaryTotalCents : null,
  };
}

// ---------------------------------------------------------------------------
// Cover vs schedule cross-checks (e-filed HTML report).

export type KansasCoverScheduleCheck = {
  coverArithmeticOk: boolean;
  scheduleAMatchesCover: boolean;
  scheduleCMatchesCover: boolean;
};

export function checkKansasCoverAgainstSchedules(
  cover: KansasReportCover,
  scheduleA: KansasScheduleATotals,
  scheduleC: KansasScheduleCTotals
): KansasCoverScheduleCheck {
  const coverArithmeticOk =
    cover.cashBeginningCents !== null &&
    cover.totalContributionsCents !== null &&
    cover.cashAvailableCents !== null &&
    cover.totalExpendituresCents !== null &&
    cover.cashCloseCents !== null &&
    cover.cashBeginningCents + cover.totalContributionsCents === cover.cashAvailableCents &&
    cover.cashAvailableCents - cover.totalExpendituresCents === cover.cashCloseCents;
  return {
    coverArithmeticOk,
    scheduleAMatchesCover:
      scheduleA.totalReceiptsCents !== null &&
      scheduleA.totalReceiptsCents === cover.totalContributionsCents,
    scheduleCMatchesCover:
      scheduleC.totalExpendituresCents !== null &&
      scheduleC.totalExpendituresCents === cover.totalExpendituresCents,
  };
}

// ---------------------------------------------------------------------------
// OCR cover recovery for scanned (paper-filed) reports.
//
// The OCR layer interleaves noise with the five SUMMARY amounts, so instead
// of trusting label adjacency the recovery searches the in-order money
// values for a 5-tuple (begin, receipts, available, spent, close) that
// satisfies both form identities begin+receipts=available and
// available-spent=close. Exactly one distinct value-tuple may match;
// anything else is a failed extraction (quarantine, never a guess).

export function extractKansasOcrMoneyValues(text: string): KansasOcrMoney[] {
  const values: KansasOcrMoney[] = [];
  for (const match of text.matchAll(/\$\s*[\d][\d,. ]*/g)) {
    const parsed = parseKansasOcrMoneyCents(match[0]);
    if (parsed) values.push(parsed);
  }
  return values;
}

export type KansasOcrCoverRecovery = {
  beginCents: number;
  receiptsCents: number;
  availableCents: number;
  expendituresCents: number;
  closeCents: number;
  usedUncertainRead: boolean;
};

/**
 * Primary OCR cover recovery: anchor each of the five SUMMARY amounts to its
 * label line (rotation-corrected extraction keeps label and amount on one
 * line; the amount may spill to the next line on a bad scan), then require
 * both form identities. Falls back to the in-order tuple search when labels
 * are too garbled — a report whose begin/spent are $0 makes the tuple search
 * inherently ambiguous (verified live on H003DP), so labels come first.
 */
const KANSAS_COVER_LABELS: { key: keyof Omit<KansasOcrCoverRecovery, "usedUncertainRead">; pattern: RegExp }[] = [
  { key: "beginCents", pattern: /CASH ON HAND AT BEGINNING/i },
  { key: "receiptsCents", pattern: /TOTAL CONTRIBUTIONS AND OTHER RECEIPTS/i },
  { key: "availableCents", pattern: /CASH AVAILABLE THIS PERIOD/i },
  { key: "expendituresCents", pattern: /TOTAL EXPENDITURES AND OTHER DISBURSEMENTS/i },
  { key: "closeCents", pattern: /CASH ON HAND AT CLOSE/i },
];

export function recoverKansasOcrCoverFromText(text: string): KansasOcrCoverRecovery | null {
  const lines = text.split("\n");
  const values: Partial<Record<string, KansasOcrMoney>> = {};
  for (const { key, pattern } of KANSAS_COVER_LABELS) {
    const labelIndex = lines.findIndex((line) => pattern.test(line));
    if (labelIndex < 0) break;
    for (let offset = 0; offset <= 2 && labelIndex + offset < lines.length; offset += 1) {
      const candidates = extractKansasOcrMoneyValues(lines[labelIndex + offset]!);
      if (candidates.length > 0) {
        values[key] = candidates[0];
        break;
      }
    }
  }
  const anchored = KANSAS_COVER_LABELS.every(({ key }) => values[key] !== undefined);
  if (anchored) {
    const recovery: KansasOcrCoverRecovery = {
      beginCents: values.beginCents!.cents,
      receiptsCents: values.receiptsCents!.cents,
      availableCents: values.availableCents!.cents,
      expendituresCents: values.expendituresCents!.cents,
      closeCents: values.closeCents!.cents,
      usedUncertainRead: KANSAS_COVER_LABELS.some(({ key }) => values[key]!.uncertain),
    };
    if (
      recovery.beginCents + recovery.receiptsCents === recovery.availableCents &&
      recovery.availableCents - recovery.expendituresCents === recovery.closeCents
    ) {
      return recovery;
    }
  }
  return recoverKansasOcrCover(extractKansasOcrMoneyValues(text));
}

/**
 * A long noisy document — especially one dense in $0.00 values, which satisfy
 * the identities in bulk — can make the 5-tuple search combinatorial. Past
 * this many money values the report goes to the manual queue instead.
 */
export const KANSAS_OCR_TUPLE_SEARCH_MAX_VALUES = 100;

export function recoverKansasOcrCover(moneyValues: readonly KansasOcrMoney[]): KansasOcrCoverRecovery | null {
  const n = moneyValues.length;
  if (n > KANSAS_OCR_TUPLE_SEARCH_MAX_VALUES) return null;
  const found = new Map<string, KansasOcrCoverRecovery>();
  for (let a = 0; a < n; a += 1) {
    for (let b = a + 1; b < n; b += 1) {
      for (let c = b + 1; c < n; c += 1) {
        if (moneyValues[a]!.cents + moneyValues[b]!.cents !== moneyValues[c]!.cents) continue;
        for (let d = c + 1; d < n; d += 1) {
          for (let e = d + 1; e < n; e += 1) {
            if (moneyValues[c]!.cents - moneyValues[d]!.cents !== moneyValues[e]!.cents) continue;
            const tuple: KansasOcrCoverRecovery = {
              beginCents: moneyValues[a]!.cents,
              receiptsCents: moneyValues[b]!.cents,
              availableCents: moneyValues[c]!.cents,
              expendituresCents: moneyValues[d]!.cents,
              closeCents: moneyValues[e]!.cents,
              usedUncertainRead: [a, b, c, d, e].some((index) => moneyValues[index]!.uncertain),
            };
            found.set(
              [tuple.beginCents, tuple.receiptsCents, tuple.availableCents, tuple.expendituresCents, tuple.closeCents].join("|"),
              tuple
            );
            // A second distinct tuple already means ambiguity — stop searching.
            if (found.size > 1) return null;
          }
        }
      }
    }
  }
  if (found.size !== 1) return null;
  return [...found.values()][0]!;
}

// ---------------------------------------------------------------------------
// Independent-expenditure statement reconciliation.
//
// "Total this Period" on IE statements is a cumulative control total WITHIN
// one reporting period that RESETS at period boundaries (verified live:
// Kansas Comeback 370,443.63 -> 378,943.63 -> 383,943.63 inside 1/1-7/23,
// then 138,270.00 as the first statement of 7/24-10/22).

export type KansasIeStatement = {
  label: string;
  periodKey: string;
  rowAmountsCents: number[];
  totalThisPeriodCents: number;
};

export type KansasIeReconciliation = {
  ok: boolean;
  failures: string[];
  totalRowCents: number;
};

export function reconcileKansasIeStatements(
  statements: readonly KansasIeStatement[]
): KansasIeReconciliation {
  const failures: string[] = [];
  let totalRowCents = 0;
  const runningByPeriod = new Map<string, number>();
  for (const statement of statements) {
    const rowsSum = statement.rowAmountsCents.reduce((sum, cents) => sum + cents, 0);
    totalRowCents += rowsSum;
    const running = (runningByPeriod.get(statement.periodKey) ?? 0) + rowsSum;
    runningByPeriod.set(statement.periodKey, running);
    if (running !== statement.totalThisPeriodCents) {
      failures.push(
        `${statement.label}: running total ${running} != stated Total this Period ${statement.totalThisPeriodCents}`
      );
    }
  }
  return { ok: failures.length === 0, failures, totalRowCents };
}
