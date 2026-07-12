import type { IllinoisSbeD2ReportSummary } from "./illinoisSbeNormalizedArtifact.js";

export type IllinoisD2FinanceSummary = {
  totalReceipts: number | null;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  debtsOwed: number | null;
  sourceUrl: string | null;
  includedReportCount: number;
};

function cycleBounds(electionYear: number): { start: string; end: string } {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100) {
    throw new Error(`Invalid Illinois D-2 election year: ${electionYear}`);
  }
  return { start: `${electionYear - 1}-01-01`, end: `${electionYear}-12-31` };
}

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function sumNullable(
  reports: readonly IllinoisSbeD2ReportSummary[],
  read: (report: IllinoisSbeD2ReportSummary) => number | null
): number | null {
  const values = reports.flatMap((report) => {
    const value = read(report);
    return value === null ? [] : [value];
  });
  return values.length === 0 ? null : roundCurrency(values.reduce((sum, value) => sum + value, 0));
}

export function aggregateIllinoisD2Summaries(input: {
  electionYear: number;
  committeeId: string;
  reports: readonly IllinoisSbeD2ReportSummary[];
}): IllinoisD2FinanceSummary | null {
  const committeeId = input.committeeId.trim();
  if (!committeeId) {
    throw new Error("Illinois D-2 committee ID is required");
  }
  const bounds = cycleBounds(input.electionYear);
  const latestByPeriod = new Map<string, IllinoisSbeD2ReportSummary>();

  for (const report of input.reports) {
    if (
      report.committeeId.trim() !== committeeId ||
      report.periodStart < bounds.start ||
      report.periodEnd > bounds.end
    ) {
      continue;
    }
    const key = `${report.periodStart}\u0000${report.periodEnd}`;
    const existing = latestByPeriod.get(key);
    if (!existing || report.filedAt > existing.filedAt || (report.filedAt === existing.filedAt && report.reportId > existing.reportId)) {
      latestByPeriod.set(key, report);
    }
  }

  const reports = [...latestByPeriod.values()].sort(
    (left, right) =>
      left.periodEnd.localeCompare(right.periodEnd) ||
      left.filedAt.localeCompare(right.filedAt) ||
      left.reportId.localeCompare(right.reportId)
  );
  if (reports.length === 0) {
    return null;
  }
  const latestBalanceReport = [...reports].reverse().find(
    (report) => report.cashOnHand !== null || report.debtsOwed !== null
  );

  return {
    totalReceipts: sumNullable(reports, (report) => report.totalReceipts),
    totalDisbursements: sumNullable(reports, (report) => report.totalDisbursements),
    cashOnHand: latestBalanceReport?.cashOnHand ?? null,
    debtsOwed: latestBalanceReport?.debtsOwed ?? null,
    sourceUrl: reports[reports.length - 1]?.sourceUrl ?? null,
    includedReportCount: reports.length,
  };
}
