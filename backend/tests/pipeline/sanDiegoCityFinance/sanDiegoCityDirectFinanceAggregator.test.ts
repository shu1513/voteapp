import { describe, expect, it } from "vitest";

import type {
  EfileCalContributionRow,
  EfileCalLoanRow,
  EfileCalSummaryRow,
} from "../../../src/pipeline/efileCalFinance/efileCalWorkbookParser.js";
import { aggregateSanDiegoCityDirectFinance } from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityDirectFinanceAggregator.js";

// Rename-only copy of the San José aggregator tests: every scenario
// reproduces a phenomenon observed in a live efile.systems export (the SJ
// 2025+2026 files; the aggregator is agency-agnostic and the SD Phase 0 probe
// reconciled it cent-exact against the live San Diego workbooks).

const FILER = "1484291";

function base(overrides: Partial<EfileCalSummaryRow> = {}) {
  return {
    filerId: FILER,
    filerName: "Test Committee",
    reportNum: "000",
    eFilingId: "100",
    origEFilingId: "100",
    cmtteType: "C",
    rptDate: "2026-02-01",
    fromDate: "2026-01-01",
    thruDate: "2026-03-31",
    electDate: null,
    formType: "F460",
    ...overrides,
  };
}

type Line = readonly [formType: string, lineItem: string, amountACents: number, amountBCents?: number | null];

function filing(
  meta: { eFilingId: string; fromDate: string; thruDate: string; rptDate?: string; reportNum?: string },
  lines: readonly Line[],
): EfileCalSummaryRow[] {
  return lines.map(([formType, lineItem, amountACents, amountBCents]) => ({
    ...base({
      eFilingId: meta.eFilingId,
      origEFilingId: meta.eFilingId,
      fromDate: meta.fromDate,
      thruDate: meta.thruDate,
      rptDate: meta.rptDate ?? "2026-02-01",
      reportNum: meta.reportNum ?? "000",
    }),
    formType,
    lineItem,
    amountACents,
    amountBCents: amountBCents === undefined ? amountACents : amountBCents,
    amountCCents: null,
  }));
}

/** A consistent F460 block: raised/spent/cash flow through the core lines. */
function coreLines(input: {
  monetary: number;
  loans?: number;
  nonmonetary?: number;
  spent: number;
  beginCash: number;
  endCash: number;
  debts?: number;
  unitemized?: number;
}): Line[] {
  const loans = input.loans ?? 0;
  const nonmonetary = input.nonmonetary ?? 0;
  return [
    ["F460", "1", input.monetary],
    ["F460", "2", loans],
    ["F460", "3", input.monetary + loans],
    ["F460", "4", nonmonetary],
    ["F460", "5", input.monetary + loans + nonmonetary],
    ["F460", "11", input.spent],
    ["F460", "12", input.beginCash],
    ["F460", "16", input.endCash],
    ["F460", "19", input.debts ?? 0],
    ["A", "2", input.unitemized ?? 0],
    ["B1", "1", loans],
  ];
}

function aRow(overrides: Partial<EfileCalContributionRow> = {}): EfileCalContributionRow {
  return {
    ...base({ formType: "A" }),
    tranId: "T1",
    entityCd: "IND",
    contributorLastName: "Donor",
    contributorFirstName: "Daisy",
    contributorOccupation: "Teacher",
    contributorEmployer: "School",
    contributorSelfEmployed: false,
    amountCents: 0,
    cumulativeYtdCents: null,
    receiptDate: null,
    memo: false,
    ...overrides,
  };
}

function b1Row(overrides: Partial<EfileCalLoanRow> = {}): EfileCalLoanRow {
  return {
    ...base({ formType: "B1" }),
    tranId: "L1",
    entityCd: "IND",
    lenderLastName: "Lender",
    lenderFirstName: "Lee",
    lenderOccupation: null,
    lenderEmployer: null,
    loanAmt1Cents: 0,
    loanAmt2Cents: null,
    loanAmt3Cents: null,
    loanAmt4Cents: null,
    loanAmt5Cents: null,
    loanAmt6Cents: null,
    loanAmt7Cents: null,
    loanAmt8Cents: null,
    memo: false,
    ...overrides,
  };
}

function aggregate(input: {
  summary: EfileCalSummaryRow[];
  scheduleA?: EfileCalContributionRow[];
  scheduleC?: EfileCalContributionRow[];
  scheduleB1?: EfileCalLoanRow[];
}) {
  return aggregateSanDiegoCityDirectFinance({
    filerId: FILER,
    summary: input.summary,
    scheduleA: input.scheduleA ?? [],
    scheduleC: input.scheduleC ?? [],
    scheduleB1: input.scheduleB1 ?? [],
  });
}

function violationTypes(result: ReturnType<typeof aggregate>): string[] {
  return result.violations.map((violation) => violation.type);
}

describe("aggregateSanDiegoCityDirectFinance", () => {
  it("computes the cycle formulas across calendar-year files, loans excluded from raised", () => {
    // Two filings spanning the 2025/2026 boundary (Bien Doan's shape): raised
    // = Σ line 1 + Σ line 4, never line 5; cash/debts from the latest filing.
    const summary = [
      ...filing({ eFilingId: "1", fromDate: "2025-01-01", thruDate: "2025-12-31", rptDate: "2026-02-02" }, [
        ...coreLines({ monetary: 100_00, spent: 40_00, beginCash: 0, endCash: 60_00 }),
      ]),
      ...filing({ eFilingId: "2", fromDate: "2026-01-01", thruDate: "2026-06-30", rptDate: "2026-07-31" }, [
        ...coreLines({
          monetary: 200_00,
          loans: 50_00,
          nonmonetary: 10_00,
          spent: 70_00,
          beginCash: 60_00,
          endCash: 240_00,
          debts: 50_00,
          unitemized: 25_00,
        }),
      ]),
    ];
    const scheduleA = [
      aRow({ eFilingId: "1", tranId: "A1", amountCents: 100_00 }),
      aRow({ eFilingId: "2", tranId: "A2", amountCents: 175_00 }),
    ];
    const scheduleC = [
      aRow({ eFilingId: "2", formType: "C", tranId: "C1", amountCents: 10_00, entityCd: "OTH" }),
    ];
    const scheduleB1 = [b1Row({ eFilingId: "2", loanAmt1Cents: 50_00 })];
    const result = aggregate({ summary, scheduleA, scheduleC, scheduleB1 });
    expect(result).toMatchObject({
      totalRaisedCents: 310_00,
      totalSpentCents: 110_00,
      loansReceivedCents: 50_00,
      cashOnHandCents: 240_00,
      debtsOwedCents: 50_00,
      unitemizedCents: 25_00,
      unitemizedNonmonetaryCents: 0,
      reportedThrough: "2026-06-30",
      coverageStart: "2025-01-01",
    });
    expect(result.filings.map((entry) => entry.eFilingId)).toEqual(["1", "2"]);
    expect(result.violations).toEqual([]);
  });

  it("keeps exactly one filing per period and drops the loser's transaction rows (Van Le case)", () => {
    // The live most_recent_only export carries two independent amendment
    // chains for Van Le's 2025-07-01..12-31 period, Schedule A rows duplicated
    // too. Latest Rpt_Date wins; the loser's rows must not double anything.
    const summary = [
      ...filing(
        { eFilingId: "24667", fromDate: "2025-07-01", thruDate: "2025-12-31", rptDate: "2026-04-21", reportNum: "002" },
        coreLines({ monetary: 100_00, spent: 0, beginCash: 0, endCash: 100_00, unitemized: 20_00 }),
      ),
      ...filing(
        { eFilingId: "25304", fromDate: "2025-07-01", thruDate: "2025-12-31", rptDate: "2026-07-13", reportNum: "001" },
        coreLines({ monetary: 90_00, spent: 0, beginCash: 0, endCash: 90_00, unitemized: 10_00 }),
      ),
    ];
    const scheduleA = [
      aRow({ eFilingId: "24667", tranId: "X1", amountCents: 80_00, contributorOccupation: "Stale" }),
      aRow({ eFilingId: "25304", tranId: "X1", amountCents: 80_00, contributorOccupation: "Current" }),
    ];
    const result = aggregate({ summary, scheduleA });
    expect(result.totalRaisedCents).toBe(90_00);
    expect(result.unitemizedCents).toBe(10_00);
    expect(result.filings.map((entry) => entry.eFilingId)).toEqual(["25304"]);
    expect(result.diagnostics.duplicateFilingsExcluded).toBe(1);
    expect(violationTypes(result)).toContain("duplicate_period_filings");
    // Loser's Schedule A row is gone: one occupation row, and reconciliation
    // (90.00 = 80.00 + 10.00 unitemized) holds without a violation.
    expect(result.breakdowns.filter((entry) => entry.categoryType === "occupation")).toEqual([
      { categoryType: "occupation", categoryName: "Current", amountCents: 80_00, contributorCount: 1 },
    ]);
    expect(violationTypes(result)).not.toContain("contribution_reconciliation");
  });

  it("reports the Amount_B line-arithmetic error baked into Bien Doan's live 460", () => {
    // Live: line 1 YTD 91,178.91 + line 2 YTD 20,000.00 but line 3 YTD
    // 131,178.91 — a $20,000 overstatement. Amount_A stays consistent.
    const summary = filing({ eFilingId: "25451", fromDate: "2026-06-01", thruDate: "2026-06-30" }, [
      ["F460", "1", 0, 91_178_91],
      ["F460", "2", 0, 20_000_00],
      ["F460", "3", 0, 131_178_91],
      ["F460", "4", 0, 2_200_00],
      ["F460", "5", 0, 133_378_91],
      ["F460", "11", 15_817_00],
      ["F460", "12", 48_485_66],
      ["F460", "16", 32_668_66],
      ["F460", "19", 20_000_00],
    ]);
    const result = aggregate({ summary });
    const arithmetic = result.violations.filter((violation) => violation.type === "line_arithmetic");
    expect(arithmetic).toHaveLength(1);
    expect(arithmetic[0]!.message).toContain("Amount_B");
    expect(arithmetic[0]!.message).toContain("131178.91");
    // The published totals never read the malformed lines 3/5 (this filing
    // raised nothing in-period: line 1 and line 4 Amount_A are both zero).
    expect(result.totalRaisedCents).toBe(0);
  });

  it("reports period gaps, overlaps, and cash-chain breaks (Ortiz / Doan cases)", () => {
    const summary = [
      ...filing({ eFilingId: "1", fromDate: "2026-01-01", thruDate: "2026-04-18", rptDate: "2026-04-23" }, [
        ...coreLines({ monetary: 0, spent: 0, beginCash: 0, endCash: 522_25 }),
      ]),
      // Overlaps: starts ON the previous Thru_Date, and restates beginning
      // cash 50 cents lower (both observed on Peter Ortiz).
      ...filing({ eFilingId: "2", fromDate: "2026-04-18", thruDate: "2026-05-31", rptDate: "2026-06-01" }, [
        ...coreLines({ monetary: 0, spent: 0, beginCash: 521_75, endCash: 400_00 }),
      ]),
      // Gap: 2026-06-01 covered by neither (observed on Bien Doan).
      ...filing({ eFilingId: "3", fromDate: "2026-06-02", thruDate: "2026-06-30", rptDate: "2026-08-01" }, [
        ...coreLines({ monetary: 0, spent: 0, beginCash: 400_00, endCash: 400_00 }),
      ]),
    ];
    const result = aggregate({ summary });
    expect(violationTypes(result)).toEqual(
      expect.arrayContaining(["period_overlap", "cash_chain", "period_gap"]),
    );
  });

  it("flags nonzero opening cash as uncovered prior activity (Altwer case)", () => {
    // Live: Genny Altwer's export history starts 2026-01-01 with $47,353.73
    // already banked — her 2025 filings are simply absent from the export.
    const summary = filing({ eFilingId: "24689", fromDate: "2026-01-01", thruDate: "2026-04-18" }, [
      ...coreLines({ monetary: 100_00, spent: 0, beginCash: 47_353_73, endCash: 47_453_73 }),
    ]);
    const result = aggregate({ summary, scheduleA: [aRow({ eFilingId: "24689", amountCents: 100_00 })] });
    const flagged = result.violations.filter((violation) => violation.type === "prior_activity_uncovered");
    expect(flagged).toHaveLength(1);
    expect(flagged[0]!.message).toContain("47353.73");
  });

  it("treats absent A|2 / C|2 / B1 blocks as zero but flags a missing core F460 line", () => {
    // Live filings legitimately omit the unitemized pseudo-lines; a filing
    // with no line 16 row is a different, reportable animal.
    const summary = filing({ eFilingId: "9", fromDate: "2026-01-01", thruDate: "2026-06-30" }, [
      ["F460", "1", 100_00],
      ["F460", "2", 0],
      ["F460", "3", 100_00],
      ["F460", "4", 0],
      ["F460", "5", 100_00],
      ["F460", "11", 0],
      ["F460", "12", 0],
      ["F460", "19", 0],
      // no F460 line 16, no A|2, no C|2, no B1 block
    ]);
    const result = aggregate({ summary, scheduleA: [aRow({ eFilingId: "9", amountCents: 100_00 })] });
    expect(result.unitemizedCents).toBe(0);
    expect(result.loansReceivedCents).toBe(0);
    expect(result.cashOnHandCents).toBeNull();
    const missing = result.violations.filter((violation) => violation.type === "missing_summary_line");
    expect(missing).toHaveLength(1);
    expect(missing[0]!.message).toContain("line 16");
  });

  it("reconciles itemized rows against the covers and reports drift", () => {
    const summary = filing({ eFilingId: "5", fromDate: "2026-01-01", thruDate: "2026-06-30" }, [
      ...coreLines({ monetary: 100_00, spent: 0, beginCash: 0, endCash: 100_00, loans: 30_00 }),
    ]);
    // Rows say 90.00, cover says 100.00; B1 sheet says 20.00, cover says 30.00.
    const result = aggregate({
      summary,
      scheduleA: [aRow({ eFilingId: "5", amountCents: 90_00 })],
      scheduleB1: [b1Row({ eFilingId: "5", loanAmt1Cents: 20_00 })],
    });
    const types = violationTypes(result);
    expect(types).toContain("contribution_reconciliation");
    expect(types).toContain("loan_cross_check");
  });

  it("builds breakdowns from IND rows only, memo rows excluded, refunds never bucketed", () => {
    const summary = filing({ eFilingId: "7", fromDate: "2026-01-01", thruDate: "2026-06-30" }, [
      ...coreLines({ monetary: 555_00, spent: 0, beginCash: 0, endCash: 555_00 }),
    ]);
    const scheduleA = [
      aRow({ eFilingId: "7", tranId: "1", amountCents: 600_00, contributorOccupation: "Teacher" }),
      // Refund: stays in occupation cents, never in a size bucket.
      aRow({ eFilingId: "7", tranId: "2", amountCents: -100_00, contributorOccupation: "Teacher" }),
      // Non-individual: money counts, no occupation/employer attribution.
      aRow({ eFilingId: "7", tranId: "3", amountCents: 55_00, entityCd: "OTH", contributorOccupation: "PAC Staff" }),
      // Memo rows are excluded from official totals (SF-proven CAL semantics).
      aRow({ eFilingId: "7", tranId: "4", amountCents: 999_00, memo: true }),
    ];
    const result = aggregate({ summary, scheduleA });
    expect(result.breakdowns).toEqual([
      { categoryType: "occupation", categoryName: "Teacher", amountCents: 500_00, contributorCount: 1 },
      { categoryType: "employer", categoryName: "School", amountCents: 500_00, contributorCount: 1 },
      { categoryType: "contribution_size", categoryName: "$500-$999", amountCents: 600_00, contributorCount: 1 },
      { categoryType: "contribution_size", categoryName: "$1-$99", amountCents: 55_00, contributorCount: 1 },
    ]);
    expect(result.diagnostics).toMatchObject({
      refundRows: 1,
      refundCents: -100_00,
      memoRowsExcluded: 1,
      memoCentsExcluded: 999_00,
    });
    expect(violationTypes(result)).not.toContain("contribution_reconciliation");
  });

  it("returns an empty aggregate (nothing to publish) for a committee with no filings", () => {
    const result = aggregate({ summary: [] });
    expect(result).toMatchObject({
      totalRaisedCents: 0,
      cashOnHandCents: null,
      debtsOwedCents: null,
      reportedThrough: null,
      coverageStart: null,
      filings: [],
      violations: [],
    });
  });

  it("flags a repeated summary line and uses the first occurrence", () => {
    const summary = [
      ...filing({ eFilingId: "8", fromDate: "2026-01-01", thruDate: "2026-06-30" }, [
        ...coreLines({ monetary: 100_00, spent: 0, beginCash: 0, endCash: 100_00 }),
        ["F460", "1", 999_00],
      ]),
    ];
    const result = aggregate({ summary, scheduleA: [aRow({ eFilingId: "8", amountCents: 100_00 })] });
    expect(result.totalRaisedCents).toBe(100_00);
    expect(violationTypes(result)).toContain("duplicate_summary_line");
  });

  it("excludes a filing whose rows disagree on period metadata", () => {
    const rows = filing({ eFilingId: "6", fromDate: "2026-01-01", thruDate: "2026-06-30" }, [
      ...coreLines({ monetary: 100_00, spent: 0, beginCash: 0, endCash: 100_00 }),
    ]);
    rows[1] = { ...rows[1]!, thruDate: "2026-07-31" };
    const result = aggregate({ summary: rows });
    expect(result.filings).toEqual([]);
    expect(result.totalRaisedCents).toBe(0);
    expect(violationTypes(result)).toContain("filing_unusable");
  });
});
