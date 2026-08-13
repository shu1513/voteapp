// Direct-campaign aggregation for one resolved Phoenix candidate committee
// (plan Phase 3). Pure over parsed canonical reports; discovery, PDF fetch,
// and parsing are the sync module's orchestration.
//
// Composition rules, pinned live by the Phase 0 probe (2026-08-12):
//
//   total_raised   = Σ Schedule A line 1(m) period cash (net monetary
//                    contributions) over in-cycle canonical reports
//   total_spent    = Σ Schedule B line 16 period cash
//   loans_received = Σ Schedule A line 2(e) period cash
//   cash_on_hand   = latest in-cycle report's cover (d) closing balance
//                    (signed — an indebted committee legitimately reports
//                    negative)
//   debts_owed     = NOT parsed in v1 (disbursements line 12 prints only in
//                    the equity column and its period-vs-cumulative semantics
//                    are unverified) — stays null
//
// The cover's (b)/(c) cycle-to-date column is a CROSS-CHECK, never a total:
// it RESETS at the Apr-1 odd-year cycle boundary for committees that keep one
// COP ID across cycles, and an amendment to an earlier report STRANDS the
// column of reports filed in between (verified live on CAN-22-6). Cycle
// totals are therefore Σ period values over canonical reports whose period
// falls inside the link's portal cycle.
//
// Occupation/employer breakdowns come from Schedule A(1)(a) + A(1)(c)
// itemized rows (the only occupation/employer source — grids carry neither),
// reconciled per report against receipts lines 1(a)/1(c) to the cent.
// contribution_size buckets are impossible (A(1)(b) is one aggregate) and
// deliberately absent.

import type { PhoenixParsedReport } from "./phoenixReportPdfParser.js";
import { phoenixCandidateCycleForDate } from "./phoenixEfilingClient.js";

export type PhoenixDirectViolationType =
  /** An in-cycle report's own arithmetic fails (1(a..j)=1(k), 1(k)-1(l)=1(m),
   * 1(m)+2(e)+other=13, 13=(b), 16=(c), (a)+(b)-(c)=(d)). */
  | "cover_arithmetic"
  /** Σ itemized A(1)(a)/A(1)(c) rows ≠ receipts line 1(a)/1(c) on an
   * in-cycle report — the occupation/employer basis is unreliable. */
  | "schedule_reconciliation"
  /** Two in-cycle canonical reports' periods overlap — canonical selection
   * failed and period sums would double-count. */
  | "period_overlap"
  /** A report's (a) ≠ the previous canonical report's (d) across CONTIGUOUS
   * periods — the filer restated its opening balance (live: CAN-25-5 opens
   * Q1 with $7,171.83 after closing the annual at $7,554.73). Cycle totals
   * sum period (b)/(c) values and do not depend on (a), so this is
   * diagnostics — the same call San José and San Diego make. */
  | "cash_chain_break"
  /** A day between consecutive in-cycle reports is covered by neither, and
   * the balances still chain across it — nothing moved in the uncovered
   * days. Diagnostics. */
  | "period_gap"
  /** A gap in period coverage where the balances ALSO fail to chain: money
   * moved during days no discovered report covers, so the cycle sums are
   * provably incomplete. The one blocking coverage failure. */
  | "coverage_hole"
  /** The cover's cycle-to-date column disagrees with the running period sum
   * (expected when an earlier report was amended — diagnostics only). */
  | "cycle_column_discrepancy"
  /** The first in-cycle report opens with nonzero cash and no earlier
   * canonical report explains it. Cycle-scoped raised/spent are unaffected
   * (carryover is a balance, not cycle activity) — diagnostics only. */
  | "opening_balance_unexplained"
  /** A cycle flow total came out negative — unpublishable (the DB CHECK and
   * the writer both reject negative flows). */
  | "negative_cycle_total";

export type PhoenixDirectViolation = {
  type: PhoenixDirectViolationType;
  /** The report the violation anchors to; null for committee-level checks. */
  reportPackageId: string | null;
  message: string;
};

export type PhoenixCanonicalReport = {
  reportPackageId: string;
  reportName: string;
  submittedDateMs: number;
  parsed: PhoenixParsedReport;
};

export type PhoenixDirectBreakdown = {
  categoryType: "occupation" | "employer";
  categoryName: string;
  amountCents: number;
  /** Positive contribution rows in this category, not distinct people
   * (the SJ/SD semantics the shared summary card already displays). */
  contributorCount: number;
};

export type PhoenixDirectFinanceAggregate = {
  totalRaisedCents: number;
  totalSpentCents: number;
  loansReceivedCents: number;
  /** Latest in-cycle cover (d); null when no report covers the cycle. */
  cashOnHandCents: number | null;
  /** Σ line 1(b) in-state ≤$100 aggregate — diagnostics, not published. */
  unitemizedCents: number;
  /** Latest in-cycle period end (ISO) — data recency, not sync time. */
  reportedThrough: string | null;
  coverageStart: string | null;
  /** In-cycle canonical reports in period order. Empty = nothing filed yet. */
  reports: { reportPackageId: string; reportName: string; periodFrom: string; periodTo: string }[];
  breakdowns: PhoenixDirectBreakdown[];
  violations: PhoenixDirectViolation[];
  diagnostics: {
    reportsSeen: number;
    inCycleReports: number;
    a1aRows: number;
    a1cRows: number;
    /** Itemized rows lacking occupation or employer (excluded from that
     * category's breakdown, still in the totals). */
    rowsWithoutOccupation: number;
    rowsWithoutEmployer: number;
  };
};

function usd(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}$${Math.trunc(abs / 100).toLocaleString("en-US")}.${String(abs % 100).padStart(2, "0")}`;
}

function nextIsoDay(isoDate: string): string {
  const [year, month, day] = isoDate.split("-").map(Number);
  return new Date(Date.UTC(year!, month! - 1, day! + 1)).toISOString().slice(0, 10);
}

export function aggregatePhoenixDirectFinance(input: {
  copId: string;
  /** ALL canonical reports for the committee (any period) —
   * pre-cycle reports feed the boundary cash-chain check. */
  reports: readonly PhoenixCanonicalReport[];
  /** The link row's portal cycle bounds (ISO dates). */
  portalCycleStart: string;
  portalCycleEnd: string;
  maxBreakdownsPerCategory?: number;
}): PhoenixDirectFinanceAggregate {
  const limit = input.maxBreakdownsPerCategory ?? 20;
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new Error(`Invalid Phoenix direct breakdown limit: ${limit}`);
  }
  const cycleStartYear = phoenixCandidateCycleForDate(input.portalCycleStart).startYear;
  if (
    phoenixCandidateCycleForDate(input.portalCycleEnd).startYear !== cycleStartYear
  ) {
    throw new Error(
      `Phoenix cycle bounds ${input.portalCycleStart}..${input.portalCycleEnd} span two portal cycles`,
    );
  }
  const violations: PhoenixDirectViolation[] = [];

  // --- Period order across ALL canonical reports (cover dates are the
  // authoritative period identity; grids expose one package per period).
  const ordered = [...input.reports].sort((a, b) =>
    a.parsed.cover.periodFrom.localeCompare(b.parsed.cover.periodFrom) ||
    a.parsed.cover.periodTo.localeCompare(b.parsed.cover.periodTo),
  );
  const inCycle = ordered.filter(
    (report) =>
      phoenixCandidateCycleForDate(report.parsed.cover.periodFrom).startYear ===
      cycleStartYear,
  );
  const inCycleIds = new Set(inCycle.map((report) => report.reportPackageId));

  // --- Per-report arithmetic (in-cycle only; out-of-cycle reports serve the
  // chain check and must merely have parsed).
  for (const report of inCycle) {
    const { cover, receipts } = report.parsed;
    const line1Sum = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"].reduce(
      (sum, key) => sum + (receipts.line1[key] ?? 0),
      0,
    );
    const checks: [string, boolean][] = [
      ["sum(1a..1j)=1k", line1Sum === receipts.line1.k],
      ["1k-1l=1m", receipts.line1.k! - receipts.line1.l! === receipts.line1.m],
      [
        "1m+2e+other=13",
        receipts.line1.m! + receipts.line2eCents + receipts.otherCashCents ===
          receipts.line13CashCents,
      ],
      ["13=cover(b)", receipts.line13CashCents === cover.receiptsPeriodCents],
      ["16=cover(c)", report.parsed.line16CashCents === cover.disbursementsPeriodCents],
      [
        "(a)+(b)-(c)=(d)",
        cover.beginCents + cover.receiptsPeriodCents - cover.disbursementsPeriodCents ===
          cover.closeCents,
      ],
    ];
    for (const [label, ok] of checks) {
      if (!ok) {
        violations.push({
          type: "cover_arithmetic",
          reportPackageId: report.reportPackageId,
          message: `${report.reportName}: ${label} does not hold`,
        });
      }
    }
    const a1aSum = report.parsed.a1aEntries.reduce((sum, entry) => sum + entry.amountCents, 0);
    const a1cSum = report.parsed.a1cEntries.reduce((sum, entry) => sum + entry.amountCents, 0);
    if (a1aSum !== receipts.line1.a) {
      violations.push({
        type: "schedule_reconciliation",
        reportPackageId: report.reportPackageId,
        message: `${report.reportName}: A(1)(a) rows sum to ${usd(a1aSum)} but line 1(a) is ${usd(receipts.line1.a!)}`,
      });
    }
    if (a1cSum !== receipts.line1.c) {
      violations.push({
        type: "schedule_reconciliation",
        reportPackageId: report.reportPackageId,
        message: `${report.reportName}: A(1)(c) rows sum to ${usd(a1cSum)} but line 1(c) is ${usd(receipts.line1.c!)}`,
      });
    }
  }

  // --- Chain checks across the full ordered sequence; a violation is typed
  // by where its LATER report sits (an out-of-cycle break is history).
  for (let index = 1; index < ordered.length; index += 1) {
    const previous = ordered[index - 1]!.parsed.cover;
    const current = ordered[index]!.parsed.cover;
    const currentReport = ordered[index]!;
    const currentInCycle = inCycleIds.has(currentReport.reportPackageId);
    const chainHolds = current.beginCents === previous.closeCents;
    if (current.periodFrom <= previous.periodTo) {
      if (currentInCycle || inCycleIds.has(ordered[index - 1]!.reportPackageId)) {
        violations.push({
          type: "period_overlap",
          reportPackageId: currentReport.reportPackageId,
          message: `${currentReport.reportName} (${current.periodFrom}..) overlaps the previous report (..${previous.periodTo})`,
        });
      }
    } else if (
      // A gap is meaningful only INSIDE the cycle window — the module never
      // aggregates the previous cycle, so the boundary "gap" between a
      // carryover committee's cycles is expected (the chain check still
      // runs across it).
      currentInCycle &&
      inCycleIds.has(ordered[index - 1]!.reportPackageId) &&
      current.periodFrom !== nextIsoDay(previous.periodTo)
    ) {
      const uncovered = `${nextIsoDay(previous.periodTo)}..${current.periodFrom}`;
      violations.push(
        chainHolds
          ? {
              type: "period_gap",
              reportPackageId: currentReport.reportPackageId,
              message: `no discovered report covers ${uncovered}; the balances chain across it, so nothing moved`,
            }
          : {
              type: "coverage_hole",
              reportPackageId: currentReport.reportPackageId,
              message: `no discovered report covers ${uncovered} and the balance changed by ${usd(current.beginCents - previous.closeCents)} across it — money moved in days no report covers`,
            },
      );
    } else if (currentInCycle && !chainHolds) {
      violations.push({
        type: "cash_chain_break",
        reportPackageId: currentReport.reportPackageId,
        message: `${currentReport.reportName} opens with ${usd(current.beginCents)} but the previous report closed with ${usd(previous.closeCents)}`,
      });
    }
    // Cycle-to-date cross-check, same portal cycle only (the column resets
    // at the boundary and amendments strand it — diagnostics).
    const sameCycle =
      phoenixCandidateCycleForDate(current.periodFrom).startYear ===
      phoenixCandidateCycleForDate(previous.periodFrom).startYear;
    if (
      sameCycle &&
      currentInCycle &&
      previous.receiptsCycleCents !== null &&
      current.receiptsCycleCents !== null &&
      current.receiptsCycleCents !==
        previous.receiptsCycleCents + current.receiptsPeriodCents
    ) {
      violations.push({
        type: "cycle_column_discrepancy",
        reportPackageId: currentReport.reportPackageId,
        message: `(b) cycle column of ${currentReport.reportName} is ${usd(current.receiptsCycleCents)}, prior cycle + period = ${usd(previous.receiptsCycleCents + current.receiptsPeriodCents)} (expected when an earlier report was amended)`,
      });
    }
    if (
      sameCycle &&
      currentInCycle &&
      previous.disbursementsCycleCents !== null &&
      current.disbursementsCycleCents !== null &&
      current.disbursementsCycleCents !==
        previous.disbursementsCycleCents + current.disbursementsPeriodCents
    ) {
      violations.push({
        type: "cycle_column_discrepancy",
        reportPackageId: currentReport.reportPackageId,
        message: `(c) cycle column of ${currentReport.reportName} is ${usd(current.disbursementsCycleCents)}, prior cycle + period = ${usd(previous.disbursementsCycleCents + current.disbursementsPeriodCents)} (expected when an earlier report was amended)`,
      });
    }
  }
  const firstInCycle = inCycle[0] ?? null;
  if (firstInCycle !== null && firstInCycle.parsed.cover.beginCents !== 0) {
    const firstIndex = ordered.indexOf(firstInCycle);
    if (firstIndex === 0) {
      violations.push({
        type: "opening_balance_unexplained",
        reportPackageId: firstInCycle.reportPackageId,
        message: `first discovered report ${firstInCycle.reportName} opens with ${usd(firstInCycle.parsed.cover.beginCents)} and no earlier report explains it`,
      });
    }
  }

  // --- Totals over in-cycle reports. ---
  const totalRaisedCents = inCycle.reduce(
    (sum, report) => sum + report.parsed.receipts.line1.m!,
    0,
  );
  const totalSpentCents = inCycle.reduce(
    (sum, report) => sum + report.parsed.line16CashCents,
    0,
  );
  const loansReceivedCents = inCycle.reduce(
    (sum, report) => sum + report.parsed.receipts.line2eCents,
    0,
  );
  const unitemizedCents = inCycle.reduce(
    (sum, report) => sum + (report.parsed.receipts.line1.b ?? 0),
    0,
  );
  for (const [label, cents] of [
    ["total raised", totalRaisedCents],
    ["total spent", totalSpentCents],
    ["loans received", loansReceivedCents],
  ] as const) {
    if (cents < 0) {
      violations.push({
        type: "negative_cycle_total",
        reportPackageId: null,
        message: `${label} came out ${usd(cents)}; negative flows are unpublishable`,
      });
    }
  }
  const latest = inCycle.length > 0 ? inCycle[inCycle.length - 1]! : null;

  // --- Occupation/employer breakdowns from in-cycle itemized rows. ---
  const maps = new Map<
    PhoenixDirectBreakdown["categoryType"],
    Map<string, { name: string; cents: number; count: number }>
  >();
  const add = (
    type: PhoenixDirectBreakdown["categoryType"],
    rawName: string | null,
    cents: number,
  ): boolean => {
    const name = rawName?.trim().replace(/\s+/g, " ");
    if (!name) return false;
    const values = maps.get(type) ?? new Map();
    const key = name.toUpperCase();
    const current = values.get(key) ?? { name, cents: 0, count: 0 };
    current.cents += cents;
    current.count += cents > 0 ? 1 : 0;
    values.set(key, current);
    maps.set(type, values);
    return true;
  };
  let a1aRows = 0;
  let a1cRows = 0;
  let rowsWithoutOccupation = 0;
  let rowsWithoutEmployer = 0;
  for (const report of inCycle) {
    a1aRows += report.parsed.a1aEntries.length;
    a1cRows += report.parsed.a1cEntries.length;
    for (const entry of [...report.parsed.a1aEntries, ...report.parsed.a1cEntries]) {
      if (!add("occupation", entry.occupation, entry.amountCents)) {
        rowsWithoutOccupation += 1;
      }
      if (!add("employer", entry.employer, entry.amountCents)) {
        rowsWithoutEmployer += 1;
      }
    }
  }
  const breakdowns: PhoenixDirectBreakdown[] = [];
  for (const type of ["occupation", "employer"] as const) {
    breakdowns.push(
      ...[...(maps.get(type)?.values() ?? [])]
        .filter((value) => value.cents > 0)
        .sort((a, b) => b.cents - a.cents || a.name.localeCompare(b.name))
        .slice(0, limit)
        .map((value) => ({
          categoryType: type,
          categoryName: value.name,
          amountCents: value.cents,
          contributorCount: value.count,
        })),
    );
  }

  return {
    totalRaisedCents,
    totalSpentCents,
    loansReceivedCents,
    cashOnHandCents: latest?.parsed.cover.closeCents ?? null,
    unitemizedCents,
    reportedThrough: latest?.parsed.cover.periodTo ?? null,
    coverageStart: firstInCycle?.parsed.cover.periodFrom ?? null,
    reports: inCycle.map((report) => ({
      reportPackageId: report.reportPackageId,
      reportName: report.reportName,
      periodFrom: report.parsed.cover.periodFrom,
      periodTo: report.parsed.cover.periodTo,
    })),
    breakdowns,
    violations,
    diagnostics: {
      reportsSeen: input.reports.length,
      inCycleReports: inCycle.length,
      a1aRows,
      a1cRows,
      rowsWithoutOccupation,
      rowsWithoutEmployer,
    },
  };
}
