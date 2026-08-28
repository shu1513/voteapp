import { isNevadaLegalDefenseFundReportName } from "./nevadaAuroraCsv.js";

// Parses the candidate-form C&E summary (lines 1-13) out of a ViewCCEReport
// HTML page, classifies AURORA report names, selects the cycle's effective
// report versions, and folds their "This Period" columns into cycle totals.
// Fixtures: backend/tests/fixtures/nevadaFinance/ (semantics proven 2026-08-26:
// annual filings are self-contained, the CE#1-4 cumulative chain restarts each
// election year, amended documents replace originals).

export type NevadaReportName = {
  baseName: string;
  isAmended: boolean;
  isLegalDefenseFund: boolean;
};

export function classifyNevadaReportName(reportName: string): NevadaReportName {
  const trimmed = reportName.trim().replace(/\s+/g, " ");
  const match = trimmed.match(/^(.*?)\s*\(([^()]*)\)$/);
  let baseName = trimmed;
  let isAmended = false;
  if (match) {
    const qualifiers = match[2].split(",").map((part) => part.trim().toLocaleLowerCase("en-US"));
    const known = qualifiers.every((part) => part === "amended" || part === "legal defense fund");
    if (known && qualifiers.length > 0) {
      baseName = match[1];
      isAmended = qualifiers.includes("amended");
    }
  }
  return {
    baseName,
    isAmended,
    isLegalDefenseFund: isNevadaLegalDefenseFundReportName(trimmed),
  };
}

export type NevadaReportPeriod = { start: string; end: string };

/**
 * Period covered by a recognized C&E report, from its base name plus the
 * detail-grid Year column (the period/election year — a "2026 Annual CE
 * Filing" row carries Year 2025). Unrecognized names (financial disclosures,
 * registration PDFs, filer-typed junk) return null.
 */
export function nevadaReportPeriod(baseName: string, gridYear: number): NevadaReportPeriod | null {
  const ceMatch = baseName.match(/^CE Report ([1-4])$/);
  if (ceMatch) {
    const quarter = Number(ceMatch[1]);
    const startMonth = (quarter - 1) * 3 + 1;
    const endMonth = quarter * 3;
    const endDay = endMonth === 6 || endMonth === 9 ? 30 : 31;
    return {
      start: `${gridYear}-${String(startMonth).padStart(2, "0")}-01`,
      end: `${gridYear}-${String(endMonth).padStart(2, "0")}-${endDay}`,
    };
  }
  const annualMatch = baseName.match(/^(\d{4}) Annual CE Filing$/);
  if (annualMatch) {
    const labelYear = Number(annualMatch[1]);
    if (labelYear !== gridYear + 1) {
      throw new Error(
        `Nevada annual filing label year ${labelYear} does not cover grid year ${gridYear} (expected label ${gridYear + 1})`
      );
    }
    return { start: `${gridYear}-01-01`, end: `${gridYear}-12-31` };
  }
  return null;
}

export type NevadaReportListRow = {
  reportName: string;
  /** Detail-grid Year column: the period/election year. */
  year: number;
  /** ISO yyyy-mm-dd file date. */
  fileDate: string;
  office: string;
  syn: string;
};

export type NevadaSelectedReport = NevadaReportListRow & {
  name: NevadaReportName;
  period: NevadaReportPeriod;
};

export type NevadaCycleReportSelection = {
  selected: NevadaSelectedReport[];
  legalDefenseFundCount: number;
  unrecognizedReportNames: string[];
};

/**
 * Effective cycle reports for one candidate: the election-year CE #1-4 plus
 * the prior-year annual filing (fixed two-year window — the coverage note
 * names it), Legal Defense Fund documents excluded, and per logical report
 * (base name + year) the amended document wins, newest file date breaking
 * ties among equals.
 */
export function selectNevadaCycleReports(input: {
  rows: readonly NevadaReportListRow[];
  electionYear: number;
}): NevadaCycleReportSelection {
  const byLogicalReport = new Map<string, NevadaSelectedReport>();
  let legalDefenseFundCount = 0;
  const unrecognizedReportNames: string[] = [];

  for (const row of input.rows) {
    const name = classifyNevadaReportName(row.reportName);
    if (name.isLegalDefenseFund) {
      legalDefenseFundCount += 1;
      continue;
    }
    const period = nevadaReportPeriod(name.baseName, row.year);
    if (period === null) {
      unrecognizedReportNames.push(row.reportName);
      continue;
    }
    const isElectionYearCe =
      /^CE Report [1-4]$/.test(name.baseName) && row.year === input.electionYear;
    const isPriorYearAnnual =
      /^\d{4} Annual CE Filing$/.test(name.baseName) && row.year === input.electionYear - 1;
    if (!isElectionYearCe && !isPriorYearAnnual) {
      continue;
    }
    const key = `${name.baseName} ${row.year}`;
    const candidate: NevadaSelectedReport = { ...row, name, period };
    const existing = byLogicalReport.get(key);
    if (!existing) {
      byLogicalReport.set(key, candidate);
      continue;
    }
    if (candidate.name.isAmended === existing.name.isAmended && candidate.fileDate === existing.fileDate) {
      throw new Error(
        `Nevada report selection tie for ${JSON.stringify(row.reportName)} (${row.year}); cannot pick a version`
      );
    }
    const preferCandidate =
      candidate.name.isAmended !== existing.name.isAmended
        ? candidate.name.isAmended
        : candidate.fileDate > existing.fileDate;
    if (preferCandidate) {
      byLogicalReport.set(key, candidate);
    }
  }

  const selected = [...byLogicalReport.values()].sort((left, right) =>
    left.period.end < right.period.end ? -1 : left.period.end > right.period.end ? 1 : 0
  );
  return { selected, legalDefenseFundCount, unrecognizedReportNames };
}

export type NevadaReportSummaryLine = {
  periodCents: number;
  cumulativeCents: number | null;
};

export type NevadaReportSummary = {
  /** Lines 1-12 keyed by line number; both columns in cents. */
  lines: Record<number, NevadaReportSummaryLine>;
  /** Line 13 ending fund balance in cents. */
  endingFundBalanceCents: number;
  filedOn: string | null;
};

function decodeHtmlText(fragment: string): string {
  return fragment
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\s+/g, " ")
    .trim();
}

function parseSummaryAmountCents(cell: string, context: string): number | null {
  const match = cell.match(/^\$\s?(\d{1,3}(?:,\d{3})*|\d+)(?:\.(\d{2}))?$/);
  if (!match) return null;
  const cents = Number(match[1].replace(/,/g, "")) * 100 + (match[2] ? Number(match[2]) : 0);
  if (!Number.isSafeInteger(cents)) {
    throw new Error(`Nevada report summary amount out of range ${JSON.stringify(cell)} (${context})`);
  }
  return cents;
}

const FILED_ON_PATTERN = /FILED\s+([A-Z][a-z]{2}) (\d{1,2}) (\d{4})/;
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/**
 * Extracts the candidate-form summary (lines 1-13) from a ViewCCEReport page.
 * Group/PAC forms (the $1,000-threshold lines-1-8 layout) are rejected:
 * Nevada v1 only reads candidate reports. The first occurrence of each line
 * number wins, so itemized-schedule text later in the page cannot shadow the
 * summary block.
 */
export function parseNevadaCandidateReportSummary(html: string, context: string): NevadaReportSummary {
  const lines: Record<number, NevadaReportSummaryLine> = {};
  let endingFundBalanceCents: number | null = null;

  for (const rowMatch of html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...rowMatch[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) =>
      decodeHtmlText(cell[1])
    );
    if (cells.length === 0) continue;
    const labelIndex = cells.findIndex((cell) => /^\d{1,2}\.\s/.test(cell));
    if (labelIndex < 0) continue;
    const lineNumber = Number.parseInt(cells[labelIndex], 10);
    if (lineNumber < 1 || lineNumber > 13) continue;
    const amounts: number[] = [];
    for (const cell of cells.slice(labelIndex + 1)) {
      const cents = parseSummaryAmountCents(cell, `${context} line ${lineNumber}`);
      if (cents !== null) amounts.push(cents);
    }
    if (amounts.length === 0) continue;
    if (lineNumber === 13) {
      endingFundBalanceCents ??= amounts[amounts.length - 1];
      continue;
    }
    lines[lineNumber] ??= {
      periodCents: amounts[0],
      cumulativeCents: amounts.length > 1 ? amounts[amounts.length - 1] : null,
    };
  }

  for (let lineNumber = 1; lineNumber <= 12; lineNumber += 1) {
    if (!lines[lineNumber]) {
      throw new Error(
        `Nevada report summary missing line ${lineNumber} (${context}); not a candidate-form report?`
      );
    }
  }
  if (endingFundBalanceCents === null) {
    throw new Error(`Nevada report summary missing line 13 (${context})`);
  }

  const contributionSum = [1, 2, 3, 4, 5, 6, 7].reduce((sum, n) => sum + lines[n].periodCents, 0);
  if (contributionSum !== lines[8].periodCents) {
    throw new Error(
      `Nevada report summary line 8 mismatch (${context}): lines 1-7 sum ${contributionSum} != ${lines[8].periodCents}`
    );
  }
  const expenseSum = [9, 10, 11].reduce((sum, n) => sum + lines[n].periodCents, 0);
  if (expenseSum !== lines[12].periodCents) {
    throw new Error(
      `Nevada report summary line 12 mismatch (${context}): lines 9-11 sum ${expenseSum} != ${lines[12].periodCents}`
    );
  }

  const filedMatch = decodeHtmlText(html).match(FILED_ON_PATTERN);
  let filedOn: string | null = null;
  if (filedMatch) {
    const month = MONTHS.indexOf(filedMatch[1]) + 1;
    if (month > 0) {
      filedOn = `${filedMatch[3]}-${String(month).padStart(2, "0")}-${filedMatch[2].padStart(2, "0")}`;
    }
  }
  return { lines, endingFundBalanceCents, filedOn };
}

export type NevadaCycleSummary = {
  totalReceiptsCents: number;
  totalDisbursementsCents: number;
  cashOnHandCents: number;
  /** Bounds for reconciling itemized CSV sums (filers may itemize <=$100 money). */
  itemizedContributionFloorCents: number;
  itemizedContributionCeilingCents: number;
  itemizedExpenseFloorCents: number;
  itemizedExpenseCeilingCents: number;
  latestPeriodEnd: string;
};

/**
 * Folds the selected reports' "This Period" columns into cycle totals (the
 * cumulative column restarts at CE#1 each election year and never spans the
 * prior-year annual filing). Cash on hand comes from the report covering the
 * latest period end — file dates are irrelevant here because selection already
 * picked one effective document per period.
 */
export function buildNevadaCycleSummary(
  reports: readonly { report: NevadaSelectedReport; summary: NevadaReportSummary }[]
): NevadaCycleSummary {
  if (reports.length === 0) {
    throw new Error("Nevada cycle summary requires at least one selected report");
  }
  let totalReceiptsCents = 0;
  let totalDisbursementsCents = 0;
  let itemizedContributionFloorCents = 0;
  let unitemizedContributionCents = 0;
  let itemizedExpenseFloorCents = 0;
  let unitemizedExpenseCents = 0;
  let latest = reports[0];
  const seenPeriodEnds = new Set<string>();
  for (const entry of reports) {
    const periodEnd = entry.report.period.end;
    if (seenPeriodEnds.has(periodEnd)) {
      throw new Error(`Nevada cycle summary received two reports ending ${periodEnd}`);
    }
    seenPeriodEnds.add(periodEnd);
    totalReceiptsCents += entry.summary.lines[8].periodCents;
    totalDisbursementsCents += entry.summary.lines[12].periodCents;
    itemizedContributionFloorCents +=
      entry.summary.lines[1].periodCents + entry.summary.lines[5].periodCents;
    unitemizedContributionCents += entry.summary.lines[7].periodCents;
    itemizedExpenseFloorCents +=
      entry.summary.lines[9].periodCents + entry.summary.lines[10].periodCents;
    unitemizedExpenseCents += entry.summary.lines[11].periodCents;
    if (periodEnd > latest.report.period.end) latest = entry;
  }
  return {
    totalReceiptsCents,
    totalDisbursementsCents,
    cashOnHandCents: latest.summary.endingFundBalanceCents,
    itemizedContributionFloorCents,
    itemizedContributionCeilingCents: itemizedContributionFloorCents + unitemizedContributionCents,
    itemizedExpenseFloorCents,
    itemizedExpenseCeilingCents: itemizedExpenseFloorCents + unitemizedExpenseCents,
    latestPeriodEnd: latest.report.period.end,
  };
}
