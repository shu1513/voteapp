import { describe, expect, it } from "vitest";
import {
  aggregatePhoenixDirectFinance,
  type PhoenixCanonicalReport,
} from "../../../src/pipeline/phoenixFinance/phoenixDirectFinanceAggregator.js";
import type {
  PhoenixParsedReport,
  PhoenixScheduleEntry,
} from "../../../src/pipeline/phoenixFinance/phoenixReportPdfParser.js";

const CYCLE = { portalCycleStart: "2025-04-01", portalCycleEnd: "2027-03-31" };

let packageCounter = 0;

/** Builds an internally consistent report; overrides then break specific
 * invariants on purpose. */
function report(input: {
  name: string;
  from: string;
  to: string;
  begin: number;
  a?: number;
  b?: number;
  c?: number;
  refunds?: number;
  loans?: number;
  spent?: number;
  receiptsCycle?: number | null;
  a1aEntries?: PhoenixScheduleEntry[];
  a1cEntries?: PhoenixScheduleEntry[];
  mutate?: (parsed: PhoenixParsedReport) => void;
}): PhoenixCanonicalReport {
  const a = input.a ?? 0;
  const b = input.b ?? 0;
  const c = input.c ?? 0;
  const refunds = input.refunds ?? 0;
  const loans = input.loans ?? 0;
  const spent = input.spent ?? 0;
  const k = a + b + c;
  const m = k - refunds;
  const line13 = m + loans;
  const parsed: PhoenixParsedReport = {
    cover: {
      reportName: input.name,
      periodFrom: input.from,
      periodTo: input.to,
      officeSought: "Council Member District 4",
      beginCents: input.begin,
      receiptsPeriodCents: line13,
      receiptsCycleCents: input.receiptsCycle ?? null,
      disbursementsPeriodCents: spent,
      disbursementsCycleCents: null,
      closeCents: input.begin + line13 - spent,
    },
    receipts: {
      line1: { a, b, c, d: 0, e: 0, f: 0, g: 0, h: 0, i: 0, j: 0, k, l: refunds, m },
      line2eCents: loans,
      otherCashCents: 0,
      line13CashCents: line13,
    },
    line16CashCents: spent,
    line6CashCents: null,
    a1aEntries: input.a1aEntries ?? (a > 0 ? [entry(a)] : []),
    a1cEntries: input.a1cEntries ?? (c > 0 ? [entry(c, "Out Of State Corp")] : []),
    b6Entries: [],
  };
  input.mutate?.(parsed);
  packageCounter += 1;
  return {
    reportPackageId: `00000000-0000-0000-0000-${String(packageCounter).padStart(12, "0")}`,
    reportName: input.name,
    submittedDateMs: packageCounter,
    parsed,
  };
}

function entry(
  amountCents: number,
  employer = "Desert Law LLP",
  occupation: string | null = "Attorney",
): PhoenixScheduleEntry {
  return { amountCents, date: "01/15/2026", name: "Pat Donor", occupation, employer };
}

describe("aggregatePhoenixDirectFinance", () => {
  it("sums period values over in-cycle reports and takes the latest close", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-4",
      reports: [
        report({ name: "Annual 2026", from: "2025-04-01", to: "2026-03-31", begin: 0, a: 100_00, b: 50_00, refunds: 10_00, loans: 25_00, spent: 40_00 }),
        report({ name: "Q1", from: "2026-04-01", to: "2026-06-30", begin: 125_00, a: 200_00, spent: 60_00 }),
      ],
      ...CYCLE,
    });
    // raised = (150-10) + 200; loans separate; cash = latest (d).
    expect(result.totalRaisedCents).toBe(340_00);
    expect(result.totalSpentCents).toBe(100_00);
    expect(result.loansReceivedCents).toBe(25_00);
    expect(result.cashOnHandCents).toBe(125_00 + 200_00 - 60_00);
    expect(result.unitemizedCents).toBe(50_00);
    expect(result.reportedThrough).toBe("2026-06-30");
    expect(result.coverageStart).toBe("2025-04-01");
    expect(result.reports).toHaveLength(2);
    expect(result.violations).toEqual([]);
    // Same employer across reports merges into one breakdown row.
    expect(result.breakdowns).toContainEqual({
      categoryType: "employer",
      categoryName: "Desert Law LLP",
      amountCents: 300_00,
      contributorCount: 2,
    });
  });

  it("excludes pre-cycle reports from totals while the boundary chain still checks", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-21-16",
      reports: [
        // Robinson's shape: one COP ID across cycles.
        report({ name: "2023 Q4", from: "2023-10-01", to: "2023-12-31", begin: 0, a: 999_00 }),
        report({ name: "Annual 2026", from: "2025-04-01", to: "2026-03-31", begin: 999_00, a: 100_00 }),
      ],
      ...CYCLE,
    });
    expect(result.totalRaisedCents).toBe(100_00);
    expect(result.diagnostics.reportsSeen).toBe(2);
    expect(result.diagnostics.inCycleReports).toBe(1);
    // Carryover explained by the prior report's close — no violation.
    expect(result.violations).toEqual([]);
  });

  it("reports a restated opening balance on contiguous periods as diagnostics (live CAN-25-5)", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-5",
      reports: [
        report({ name: "Annual 2026", from: "2025-04-01", to: "2026-03-31", begin: 0, a: 100_00 }),
        report({ name: "Q1", from: "2026-04-01", to: "2026-06-30", begin: 73_00, a: 50_00 }),
      ],
      ...CYCLE,
    });
    expect(result.violations).toEqual([
      expect.objectContaining({ type: "cash_chain_break" }),
    ]);
    // Cycle totals sum period values and never touch (a).
    expect(result.totalRaisedCents).toBe(150_00);
  });

  it("flags a coverage hole when a period gap ALSO breaks the balance chain", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-4",
      reports: [
        report({ name: "Annual 2026", from: "2025-04-01", to: "2026-03-31", begin: 0, a: 100_00 }),
        // Q1 (Apr-Jun) missing AND the balance moved across those days.
        report({ name: "Q2", from: "2026-07-01", to: "2026-09-30", begin: 140_00, a: 5_00 }),
      ],
      ...CYCLE,
    });
    expect(result.violations).toEqual([
      expect.objectContaining({ type: "coverage_hole" }),
    ]);
  });

  it("treats a period gap with an intact chain as diagnostics", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-4",
      reports: [
        report({ name: "Annual 2026", from: "2025-04-01", to: "2026-03-31", begin: 0, a: 100_00 }),
        // Q1 (Apr-Jun) missing from discovery; the balances chain across
        // the uncovered days, so nothing moved there.
        report({ name: "Q2", from: "2026-07-01", to: "2026-09-30", begin: 100_00, a: 5_00 }),
      ],
      ...CYCLE,
    });
    expect(result.violations).toEqual([
      expect.objectContaining({ type: "period_gap" }),
    ]);
    expect(result.totalRaisedCents).toBe(105_00);
  });

  it("flags overlapping in-cycle periods (canonical selection failed)", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-22-6",
      reports: [
        report({ name: "Annual 2026", from: "2025-04-01", to: "2026-03-31", begin: 0, a: 100_00 }),
        report({ name: "Annual 2026 - [AMENDMENT]", from: "2025-04-01", to: "2026-03-31", begin: 0, a: 100_00 }),
      ],
      ...CYCLE,
    });
    expect(result.violations).toContainEqual(
      expect.objectContaining({ type: "period_overlap" }),
    );
  });

  it("flags broken cover arithmetic and schedule reconciliation per report", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-4",
      reports: [
        report({
          name: "Q1",
          from: "2026-04-01",
          to: "2026-06-30",
          begin: 0,
          a: 100_00,
          mutate: (parsed) => {
            parsed.cover.closeCents += 27_00; // (a)+(b)-(c)=(d) breaks
            parsed.a1aEntries = [entry(99_00)]; // A1a rows != 1(a)
          },
        }),
      ],
      ...CYCLE,
    });
    expect(result.violations).toContainEqual(
      expect.objectContaining({ type: "cover_arithmetic" }),
    );
    expect(result.violations).toContainEqual(
      expect.objectContaining({ type: "schedule_reconciliation" }),
    );
  });

  it("reports the stranded cycle column as diagnostics (amendment semantics)", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-22-6",
      reports: [
        report({ name: "Annual 2026", from: "2025-04-01", to: "2026-03-31", begin: 0, a: 100_00, receiptsCycle: 100_00 }),
        // Cycle column matches the PRE-amendment annual (the live CAN-22-6
        // case): running sum disagrees.
        report({ name: "Q1", from: "2026-04-01", to: "2026-06-30", begin: 100_00, a: 20_00, receiptsCycle: 118_00 }),
      ],
      ...CYCLE,
    });
    expect(result.violations).toEqual([
      expect.objectContaining({ type: "cycle_column_discrepancy" }),
    ]);
  });

  it("flags an unexplained nonzero opening balance on the first-ever report", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-4",
      reports: [
        report({ name: "Annual 2026", from: "2025-04-01", to: "2026-03-31", begin: 42_00, a: 100_00 }),
      ],
      ...CYCLE,
    });
    expect(result.violations).toEqual([
      expect.objectContaining({ type: "opening_balance_unexplained" }),
    ]);
  });

  it("flags a negative cycle total (refunds exceeding contributions)", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-4",
      reports: [
        report({
          name: "Q1",
          from: "2026-04-01",
          to: "2026-06-30",
          begin: 100_00,
          a: 10_00,
          refunds: 50_00,
          a1aEntries: [entry(10_00)],
        }),
      ],
      ...CYCLE,
    });
    expect(result.totalRaisedCents).toBe(-40_00);
    expect(result.violations).toContainEqual(
      expect.objectContaining({ type: "negative_cycle_total" }),
    );
  });

  it("counts itemized rows without occupation/employer instead of inventing categories", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-4",
      reports: [
        report({
          name: "Q1",
          from: "2026-04-01",
          to: "2026-06-30",
          begin: 0,
          a: 30_00,
          a1aEntries: [entry(10_00), entry(20_00, "", null)],
        }),
      ],
      ...CYCLE,
    });
    expect(result.diagnostics.rowsWithoutOccupation).toBe(1);
    expect(result.diagnostics.rowsWithoutEmployer).toBe(1);
    expect(
      result.breakdowns.filter((row) => row.categoryType === "occupation"),
    ).toHaveLength(1);
  });

  it("returns empty totals when nothing is filed in the cycle", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-26-1",
      reports: [],
      ...CYCLE,
    });
    expect(result.totalRaisedCents).toBe(0);
    expect(result.cashOnHandCents).toBeNull();
    expect(result.reportedThrough).toBeNull();
    expect(result.reports).toEqual([]);
    expect(result.violations).toEqual([]);
  });

  it("rejects cycle bounds spanning two portal cycles", () => {
    expect(() =>
      aggregatePhoenixDirectFinance({
        copId: "CAN-25-4",
        reports: [],
        portalCycleStart: "2025-04-01",
        portalCycleEnd: "2027-04-01",
      }),
    ).toThrow(/span two portal cycles/);
  });

  it("rejects an invalid breakdown limit", () => {
    expect(() =>
      aggregatePhoenixDirectFinance({
        copId: "CAN-25-4",
        reports: [],
        ...CYCLE,
        maxBreakdownsPerCategory: 0,
      }),
    ).toThrow(/Invalid Phoenix direct breakdown limit: 0/);
  });

  it("truncates breakdown rows to the per-category limit, largest first", () => {
    const result = aggregatePhoenixDirectFinance({
      copId: "CAN-25-4",
      reports: [
        report({
          name: "Q1",
          from: "2026-04-01",
          to: "2026-06-30",
          begin: 0,
          a: 60_00,
          a1aEntries: [
            entry(10_00, "Small Shop"),
            entry(30_00, "Big Shop"),
            entry(20_00, "Mid Shop"),
          ],
        }),
      ],
      ...CYCLE,
      maxBreakdownsPerCategory: 1,
    });
    expect(
      result.breakdowns.filter((row) => row.categoryType === "employer"),
    ).toEqual([
      {
        categoryType: "employer",
        categoryName: "Big Shop",
        amountCents: 30_00,
        contributorCount: 1,
      },
    ]);
  });
});
