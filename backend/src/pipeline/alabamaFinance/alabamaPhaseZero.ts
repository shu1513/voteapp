// Phase 0 pure functions: filing-detail cover parsing and race-vs-extract
// reconciliation. No network, no database (plan-alabama-finance.md, Phase 0).

import type { AlabamaRaceRow } from "./alabamaFcpaClient.js";
import type { AlabamaCashRow, AlabamaExpenditureRow } from "./alabamaFcpaCsv.js";

export function centsFromPortalNumber(value: number): number {
  return Math.round(value * 100);
}

/** "$358,862.76" / "$0.00" -> cents. */
export function parseAlabamaDollarsCents(raw: string): number {
  const match = /^-?\$?[\d,]+(?:\.\d{2})?$/.exec(raw.trim());
  if (!match) throw new Error(`Unparseable dollar amount: ${JSON.stringify(raw)}`);
  const negative = raw.trim().startsWith("-");
  const digits = raw.replace(/[^\d.]/g, "");
  const [whole, fraction = "0"] = digits.split(".");
  const cents = Number(whole) * 100 + Number(fraction.padEnd(2, "0"));
  return negative ? -cents : cents;
}

/**
 * Portal wall-clock stamps -> comparable milliseconds (interpreted as UTC;
 * both formats are Central wall clock, so comparisons between them are
 * consistent). Accepts "08/26/2026 08:41 PM" (filings FILEDDATE) and
 * "Aug 26, 2026, 2:32:00 AM" (extract catalog LASTUPDATEDRAW).
 */
export function parseAlabamaWallClockMs(raw: string): number {
  const trimmed = raw.trim();
  const months: Record<string, number> = {
    jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
    jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
  };
  let year: number, month: number, day: number, hour: number, minute: number, second: number, meridiem: string;
  const numeric = /^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{1,2}):(\d{2})\s+(AM|PM)$/i.exec(trimmed);
  const worded = /^([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4}),\s+(\d{1,2}):(\d{2}):(\d{2})\s+(AM|PM)$/i.exec(trimmed);
  if (numeric) {
    month = Number(numeric[1]);
    day = Number(numeric[2]);
    year = Number(numeric[3]);
    hour = Number(numeric[4]);
    minute = Number(numeric[5]);
    second = 0;
    meridiem = numeric[6]!.toUpperCase();
  } else if (worded) {
    const monthNumber = months[worded[1]!.toLowerCase()];
    if (!monthNumber) throw new Error(`Unparseable timestamp month: ${JSON.stringify(raw)}`);
    month = monthNumber;
    day = Number(worded[2]);
    year = Number(worded[3]);
    hour = Number(worded[4]);
    minute = Number(worded[5]);
    second = Number(worded[6]);
    meridiem = worded[7]!.toUpperCase();
  } else {
    throw new Error(`Unparseable timestamp: ${JSON.stringify(raw)}`);
  }
  if (meridiem === "PM" && hour !== 12) hour += 12;
  if (meridiem === "AM" && hour === 12) hour = 0;
  return Date.UTC(year, month - 1, day, hour, minute, second);
}

// ---------------------------------------------------------------------------
// Filing-detail cover parsing

export type AlabamaPeriodicFilingCover = {
  kind: "periodic";
  beginningBalanceCents: number;
  itemizedCashCents: number;
  nonItemizedCashCents: number;
  payrollCashCents: number;
  itemizedInKindCents: number;
  nonItemizedInKindCents: number;
  itemizedOtherCents: number;
  nonItemizedOtherCents: number;
  itemizedExpenditureCents: number;
  nonItemizedExpenditureCents: number;
  itemizedLocCents: number;
  nonItemizedLocCents: number;
  endingBalanceCents: number;
};

/** Major Contribution Reports carry only receipt totals, no expenditures. */
export type AlabamaMajorContributionFilingCover = {
  kind: "major_contribution";
  beginningBalanceCents: number;
  totalCashCents: number;
  totalInKindCents: number;
  totalOtherCents: number;
};

export type AlabamaFilingCover = AlabamaPeriodicFilingCover | AlabamaMajorContributionFilingCover;

type PeriodicCoverAmounts = Omit<AlabamaPeriodicFilingCover, "kind">;

const MAJOR_COVER_LABELS: ReadonlyArray<[keyof Omit<AlabamaMajorContributionFilingCover, "kind">, string]> = [
  ["beginningBalanceCents", "Beginning Balance"],
  ["totalCashCents", "Total Cash Contribution"],
  ["totalInKindCents", "Total In-Kind Contributions"],
  ["totalOtherCents", "Total Receipt from Other Sources"],
];

const COVER_LABELS: ReadonlyArray<[keyof PeriodicCoverAmounts, string]> = [
  ["beginningBalanceCents", "Beginning Balance"],
  ["itemizedCashCents", "Itemized cash contributions"],
  ["nonItemizedCashCents", "Non-itemized cash contributions"],
  ["payrollCashCents", "Non-itemized employee payroll contributions"],
  ["itemizedInKindCents", "Itemized in-kind contributions"],
  ["nonItemizedInKindCents", "Non-itemized in-kind contributions"],
  ["itemizedOtherCents", "Itemized receipts from other sources"],
  ["nonItemizedOtherCents", "Non-itemized receipts from other sources"],
  ["itemizedExpenditureCents", "Itemized Expenditures"],
  ["nonItemizedExpenditureCents", "Non-itemized Expenditures"],
  ["itemizedLocCents", "Itemized Line of Credit Expenditures"],
  ["nonItemizedLocCents", "Non-itemized Line of Credit Expenditures"],
  ["endingBalanceCents", "Ending Balance"],
];

function parseCoverAmounts<TKey extends string>(
  text: string,
  labels: ReadonlyArray<[TKey, string]>
): Record<TKey, number> {
  const amounts = {} as Record<TKey, number>;
  let cursor = 0;
  for (const [key, label] of labels) {
    // Labels appear in document order; "Itemized Expenditures" would otherwise
    // also match inside "...Line of Credit Expenditures", so search forward
    // from the previous label only.
    // Negative balances render accounting-style, "($220.23)" (live 2026-09-01,
    // nine committees), alongside the "-$" form.
    const candidates = [`${label} $`, `${label} -$`, `${label} ($`]
      .map((needle) => text.indexOf(needle, cursor))
      .filter((index) => index >= 0);
    if (candidates.length === 0) throw new Error(`Filing detail cover is missing ${JSON.stringify(label)}`);
    const start = Math.min(...candidates);
    const amountMatch = /^(?:-?\$[\d,]+\.\d{2}|\(\$[\d,]+\.\d{2}\))/.exec(text.slice(start + label.length + 1));
    if (!amountMatch) throw new Error(`Filing detail cover has no amount for ${JSON.stringify(label)}`);
    const raw = amountMatch[0];
    amounts[key] = raw.startsWith("(")
      ? -parseAlabamaDollarsCents(raw.slice(1, -1))
      : parseAlabamaDollarsCents(raw);
    cursor = start + label.length;
  }
  return amounts;
}

/**
 * Parse the structured cover totals from a public filing-detail page. Handles
 * both layouts: full periodic reports (weekly/monthly/daily/annual) and the
 * reduced Major Contribution Report cover.
 */
export function parseAlabamaFilingDetailCover(html: string): AlabamaFilingCover {
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ");
  if (text.includes("Total Cash Contribution $") || text.includes("Total Cash Contribution -$")) {
    return { kind: "major_contribution", ...parseCoverAmounts(text, MAJOR_COVER_LABELS) };
  }
  return { kind: "periodic", ...parseCoverAmounts(text, COVER_LABELS) };
}

export function alabamaCoverCashCents(cover: AlabamaFilingCover): number {
  if (cover.kind === "major_contribution") return cover.totalCashCents;
  return cover.itemizedCashCents + cover.nonItemizedCashCents + cover.payrollCashCents;
}

export function alabamaCoverInKindCents(cover: AlabamaFilingCover): number {
  if (cover.kind === "major_contribution") return cover.totalInKindCents;
  return cover.itemizedInKindCents + cover.nonItemizedInKindCents;
}

export function alabamaCoverOtherCents(cover: AlabamaFilingCover): number {
  if (cover.kind === "major_contribution") return cover.totalOtherCents;
  return cover.itemizedOtherCents + cover.nonItemizedOtherCents;
}

/** Ordinary expenditures only; line-of-credit stays out of state cash accounting. */
export function alabamaCoverExpenditureCents(cover: AlabamaFilingCover): number {
  if (cover.kind === "major_contribution") return 0;
  return cover.itemizedExpenditureCents + cover.nonItemizedExpenditureCents;
}

// ---------------------------------------------------------------------------
// Extract summaries per committee

const CASH_TYPES = new Set([
  "Cash (Itemized)",
  "Cash (Non-Itemized)",
  "Non-Itemized Employee Payroll Contribution",
]);

export type AlabamaMoneySummary = { rowCount: number; amountCents: number };

export type AlabamaCashExtractSummary = {
  cash: AlabamaMoneySummary;
  inKind: AlabamaMoneySummary;
  returnedRows: AlabamaMoneySummary;
  negativeRows: AlabamaMoneySummary;
  amendedRowCount: number;
};

export function summarizeAlabamaCashRows(
  rows: readonly AlabamaCashRow[],
  fcpaCommitteeId: string
): AlabamaCashExtractSummary {
  const summary: AlabamaCashExtractSummary = {
    cash: { rowCount: 0, amountCents: 0 },
    inKind: { rowCount: 0, amountCents: 0 },
    returnedRows: { rowCount: 0, amountCents: 0 },
    negativeRows: { rowCount: 0, amountCents: 0 },
    amendedRowCount: 0,
  };
  for (const row of rows) {
    if (row.committeeId !== fcpaCommitteeId) continue;
    let bucket: AlabamaMoneySummary;
    if (CASH_TYPES.has(row.contributionType)) bucket = summary.cash;
    else if (row.contributionType.startsWith("In-Kind")) bucket = summary.inKind;
    else throw new Error(`Unknown Alabama contribution type: ${JSON.stringify(row.contributionType)}`);
    bucket.rowCount += 1;
    bucket.amountCents += row.amountCents;
    if (row.contributorType === "Returned (Cash Only)") {
      summary.returnedRows.rowCount += 1;
      summary.returnedRows.amountCents += row.amountCents;
    }
    if (row.amountCents < 0) {
      summary.negativeRows.rowCount += 1;
      summary.negativeRows.amountCents += row.amountCents;
    }
    if (row.amended === "Y") summary.amendedRowCount += 1;
  }
  return summary;
}

export type AlabamaExpenditureExtractSummary = {
  regular: AlabamaMoneySummary;
  lineOfCredit: AlabamaMoneySummary;
  amendedRowCount: number;
};

export function summarizeAlabamaExpenditureRows(
  rows: readonly AlabamaExpenditureRow[],
  fcpaCommitteeId: string
): AlabamaExpenditureExtractSummary {
  const summary: AlabamaExpenditureExtractSummary = {
    regular: { rowCount: 0, amountCents: 0 },
    lineOfCredit: { rowCount: 0, amountCents: 0 },
    amendedRowCount: 0,
  };
  for (const row of rows) {
    if (row.committeeId !== fcpaCommitteeId) continue;
    const bucket = row.expenditureType.includes("Line of Credit") ? summary.lineOfCredit : summary.regular;
    bucket.rowCount += 1;
    bucket.amountCents += row.amountCents;
    if (row.amended === "Y") summary.amendedRowCount += 1;
  }
  return summary;
}

// ---------------------------------------------------------------------------
// Reconciliation
//
// Authority contract (proven live on Tuberville, 99 filings, cent-exact):
// the race row's totals equal the sum of every filed report cover. Extracts
// are the transaction-level view and may undercount slightly (rows missing
// from the annual files), so they get a coverage ratio, not an exactness gate.

export type AlabamaComponentReconciliation = {
  raceCents: number;
  coverSumCents: number;
  extractCents: number;
  /** race - covers; 0 when the authority contract holds. */
  authorityDeltaCents: number;
  authorityStatus: "exact" | "mismatch";
  /**
   * extract / race; 1 when the extracts contain every reported dollar.
   * null when the race total is 0 (ratio undefined) — callers must treat a
   * nonzero extractCents alongside null coverage as a mismatch, not a pass.
   */
  extractCoverage: number | null;
};

export type AlabamaCommitteeReconciliation = {
  cash: AlabamaComponentReconciliation;
  inKind: AlabamaComponentReconciliation;
  expenditure: AlabamaComponentReconciliation;
  otherSources: { raceCents: number; };
  /** ENDINGFUNDS - (BEGINNINGFUNDS + cash + other - expenditures), from the race row alone. */
  raceIdentityDeltaCents: number;
};

function reconcileComponent(raceCents: number, coverSumCents: number, extractCents: number): AlabamaComponentReconciliation {
  return {
    raceCents,
    coverSumCents,
    extractCents,
    authorityDeltaCents: raceCents - coverSumCents,
    authorityStatus: raceCents === coverSumCents ? "exact" : "mismatch",
    extractCoverage: raceCents === 0 ? null : extractCents / raceCents,
  };
}

export function reconcileAlabamaCommittee(input: {
  raceRow: AlabamaRaceRow;
  cashSummary: AlabamaCashExtractSummary;
  expenditureSummary: AlabamaExpenditureExtractSummary;
  /** Covers of ALL of the committee's filings (current versions). */
  covers: readonly AlabamaFilingCover[];
}): AlabamaCommitteeReconciliation {
  const raceCashCents = centsFromPortalNumber(input.raceRow.MONETARYCONTRIB);
  const raceInKindCents = centsFromPortalNumber(input.raceRow.NONMONETARYCONTRIB);
  const raceExpenditureCents = centsFromPortalNumber(input.raceRow.MONETARYEXP);
  const sum = (select: (cover: AlabamaFilingCover) => number): number =>
    input.covers.reduce((total, cover) => total + select(cover), 0);
  const raceIdentityDeltaCents =
    centsFromPortalNumber(input.raceRow.ENDINGFUNDS) -
    (centsFromPortalNumber(input.raceRow.BEGINNINGFUNDS) +
      raceCashCents +
      centsFromPortalNumber(input.raceRow.OTHERSOURCES) -
      raceExpenditureCents);
  return {
    cash: reconcileComponent(raceCashCents, sum(alabamaCoverCashCents), input.cashSummary.cash.amountCents),
    inKind: reconcileComponent(raceInKindCents, sum(alabamaCoverInKindCents), input.cashSummary.inKind.amountCents),
    expenditure: reconcileComponent(
      raceExpenditureCents,
      sum(alabamaCoverExpenditureCents),
      input.expenditureSummary.regular.amountCents
    ),
    otherSources: { raceCents: centsFromPortalNumber(input.raceRow.OTHERSOURCES) },
    raceIdentityDeltaCents,
  };
}
