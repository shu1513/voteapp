import { parseCsvObjects } from "../../utils/csvObjects.js";

// AURORA "Export Results to CSV Only" columns (probed 2026-08-26; fixtures in
// backend/tests/fixtures/nevadaFinance/). Values are display strings: amounts
// like "$1,000.00", dates M/D/YYYY, report names are filer-typed free text.
export const NEVADA_CONTRIBUTION_CSV_COLUMNS = [
  "Contributor",
  "Date",
  "Amount",
  "Type",
  "Recipient",
  "Report",
] as const;

export const NEVADA_EXPENDITURE_CSV_COLUMNS = [
  "Payee",
  "Date",
  "Amount",
  "Type",
  "Payer",
  "Report",
] as const;

export type NevadaContributionCsvRow = {
  contributorName: string;
  date: string; // ISO yyyy-mm-dd
  amountCents: number;
  transactionType:
    | "Monetary Contribution"
    | "In Kind Contribution"
    | "Written Commitment"
    | "In Kind Written Commitment";
  filerName: string;
  filerKey: string;
  reportName: string;
  isLegalDefenseFund: boolean;
};

export type NevadaExpenditureCsvRow = {
  payeeName: string;
  date: string; // ISO yyyy-mm-dd
  amountCents: number;
  transactionType: "Monetary Expense" | "In Kind Expense";
  filerName: string;
  filerKey: string;
  reportName: string;
  isLegalDefenseFund: boolean;
};

/** AURORA filer names join CSV rows to links: uppercase, collapsed whitespace. */
export function nevadaFilerKey(filerName: string): string {
  return filerName.trim().replace(/\s+/g, " ").toLocaleUpperCase("en-US");
}

export function isNevadaLegalDefenseFundReportName(reportName: string): boolean {
  return /\bLegal Defense Fund\b/i.test(reportName);
}

export function parseNevadaCurrencyCents(value: string, context: string): number {
  // Accounting-style parentheses mark reversals (rejected deposits, refunds):
  // "($2,500.00)" is a negative row that filers net against their totals
  // (live-hit 2026-08-27: 70 such rows across the statewide 2025-2026 CSVs).
  const trimmed = value.trim();
  const negativeMatch = trimmed.match(/^\((.*)\)$/);
  const magnitudeText = (negativeMatch ? negativeMatch[1] : trimmed).trim();
  const match = magnitudeText.match(/^\$\s?(\d{1,3}(?:,\d{3})*|\d+)(?:\.(\d{2}))?$/);
  if (!match) {
    throw new Error(`Invalid Nevada AURORA amount ${JSON.stringify(value)} (${context})`);
  }
  const wholeDollars = Number(match[1].replace(/,/g, ""));
  const cents = wholeDollars * 100 + (match[2] ? Number(match[2]) : 0);
  if (!Number.isSafeInteger(cents)) {
    throw new Error(`Nevada AURORA amount out of range ${JSON.stringify(value)} (${context})`);
  }
  return negativeMatch ? -cents : cents;
}

export function parseNevadaCsvDate(value: string, context: string): string {
  const match = value.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!match) {
    throw new Error(`Invalid Nevada AURORA date ${JSON.stringify(value)} (${context})`);
  }
  const [, month, day, year] = match;
  const monthNum = Number(month);
  const dayNum = Number(day);
  // UTC round-trip rejects calendar-invalid dates (2/30, non-leap 2/29, ...).
  const roundTrip = new Date(Date.UTC(Number(year), monthNum - 1, dayNum));
  if (
    roundTrip.getUTCFullYear() !== Number(year) ||
    roundTrip.getUTCMonth() !== monthNum - 1 ||
    roundTrip.getUTCDate() !== dayNum
  ) {
    throw new Error(`Invalid Nevada AURORA date ${JSON.stringify(value)} (${context})`);
  }
  return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
}

// Written commitments are promised-but-unreceived money reported on their own
// summary lines (outside lines 1/5/7); the aggregator nets them out of the
// reconciliation window (14 such rows live-hit in the 2025-2026 statewide CSVs).
const CONTRIBUTION_TYPES = new Set([
  "Monetary Contribution",
  "In Kind Contribution",
  "Written Commitment",
  "In Kind Written Commitment",
]);
const EXPENDITURE_TYPES = new Set(["Monetary Expense", "In Kind Expense"]);

function requireCell(row: Record<string, string>, column: string, context: string): string {
  const value = row[column]?.trim();
  if (!value) {
    throw new Error(`Nevada AURORA CSV row missing ${column} (${context})`);
  }
  return value;
}

function requireWellFormed(parsed: { malformedRowCount: number }, kind: string): void {
  if (parsed.malformedRowCount > 0) {
    throw new Error(
      `Nevada AURORA ${kind} CSV contains ${parsed.malformedRowCount} malformed row(s); refusing to drop money silently`
    );
  }
}

export function parseNevadaContributionCsv(text: string): NevadaContributionCsvRow[] {
  const parsed = parseCsvObjects({ text, requiredHeaders: NEVADA_CONTRIBUTION_CSV_COLUMNS });
  requireWellFormed(parsed, "contribution");
  return parsed.rows.map((row, index) => {
    const context = `contribution row ${index + 1}`;
    const transactionType = requireCell(row, "Type", context);
    if (!CONTRIBUTION_TYPES.has(transactionType)) {
      throw new Error(`Unknown Nevada contribution type ${JSON.stringify(transactionType)} (${context})`);
    }
    const filerName = requireCell(row, "Recipient", context);
    const reportName = requireCell(row, "Report", context);
    return {
      contributorName: requireCell(row, "Contributor", context),
      date: parseNevadaCsvDate(requireCell(row, "Date", context), context),
      amountCents: parseNevadaCurrencyCents(requireCell(row, "Amount", context), context),
      transactionType: transactionType as NevadaContributionCsvRow["transactionType"],
      filerName,
      filerKey: nevadaFilerKey(filerName),
      reportName,
      isLegalDefenseFund: isNevadaLegalDefenseFundReportName(reportName),
    };
  });
}

export function parseNevadaExpenditureCsv(text: string): NevadaExpenditureCsvRow[] {
  const parsed = parseCsvObjects({ text, requiredHeaders: NEVADA_EXPENDITURE_CSV_COLUMNS });
  requireWellFormed(parsed, "expenditure");
  return parsed.rows.map((row, index) => {
    const context = `expenditure row ${index + 1}`;
    const transactionType = requireCell(row, "Type", context);
    if (!EXPENDITURE_TYPES.has(transactionType)) {
      throw new Error(`Unknown Nevada expenditure type ${JSON.stringify(transactionType)} (${context})`);
    }
    const filerName = requireCell(row, "Payer", context);
    const reportName = requireCell(row, "Report", context);
    return {
      payeeName: requireCell(row, "Payee", context),
      date: parseNevadaCsvDate(requireCell(row, "Date", context), context),
      amountCents: parseNevadaCurrencyCents(requireCell(row, "Amount", context), context),
      transactionType: transactionType as NevadaExpenditureCsvRow["transactionType"],
      filerName,
      filerKey: nevadaFilerKey(filerName),
      reportName,
      isLegalDefenseFund: isNevadaLegalDefenseFundReportName(reportName),
    };
  });
}
