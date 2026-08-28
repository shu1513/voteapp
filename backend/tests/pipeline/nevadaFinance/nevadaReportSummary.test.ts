import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  buildNevadaCycleSummary,
  classifyNevadaReportName,
  nevadaReportPeriod,
  parseNevadaCandidateReportSummary,
  selectNevadaCycleReports,
  type NevadaReportListRow,
  type NevadaReportSummary,
  type NevadaSelectedReport,
} from "../../../src/pipeline/nevadaFinance/nevadaReportSummary.js";

const FIXTURES = new URL("../../fixtures/nevadaFinance/", import.meta.url);

function listRow(overrides: Partial<NevadaReportListRow>): NevadaReportListRow {
  return {
    reportName: "CE Report 2",
    year: 2026,
    fileDate: "2026-07-15",
    office: "Governor",
    syn: "token",
    ...overrides,
  };
}

describe("classifyNevadaReportName", () => {
  it("splits known qualifiers and keeps junk names opaque", () => {
    expect(classifyNevadaReportName("CE Report 2")).toEqual({
      baseName: "CE Report 2",
      isAmended: false,
      isLegalDefenseFund: false,
    });
    expect(classifyNevadaReportName("CE Report 4 (Amended)")).toMatchObject({
      baseName: "CE Report 4",
      isAmended: true,
    });
    expect(classifyNevadaReportName("2026 Annual CE Filing (Legal Defense Fund)")).toMatchObject({
      baseName: "2026 Annual CE Filing",
      isLegalDefenseFund: true,
    });
    expect(classifyNevadaReportName("CE Report 2 (Amended, Legal Defense Fund)")).toMatchObject({
      baseName: "CE Report 2",
      isAmended: true,
      isLegalDefenseFund: true,
    });
    // Unknown parentheticals stay part of the opaque name.
    expect(classifyNevadaReportName("Report (Special)").baseName).toBe("Report (Special)");
    expect(classifyNevadaReportName("Tick").baseName).toBe("Tick");
  });
});

describe("nevadaReportPeriod", () => {
  it("maps CE quarters and prior-year annual filings", () => {
    expect(nevadaReportPeriod("CE Report 1", 2026)).toEqual({ start: "2026-01-01", end: "2026-03-31" });
    expect(nevadaReportPeriod("CE Report 2", 2026)).toEqual({ start: "2026-04-01", end: "2026-06-30" });
    expect(nevadaReportPeriod("CE Report 3", 2026)).toEqual({ start: "2026-07-01", end: "2026-09-30" });
    expect(nevadaReportPeriod("CE Report 4", 2026)).toEqual({ start: "2026-10-01", end: "2026-12-31" });
    expect(nevadaReportPeriod("2026 Annual CE Filing", 2025)).toEqual({
      start: "2025-01-01",
      end: "2025-12-31",
    });
    expect(nevadaReportPeriod("2026 Candidate Financial Disclosure", 2026)).toBeNull();
    expect(nevadaReportPeriod("Tick", 2026)).toBeNull();
    expect(() => nevadaReportPeriod("2026 Annual CE Filing", 2026)).toThrow(/label year/);
  });
});

describe("selectNevadaCycleReports", () => {
  it("keeps election-year CEs plus the prior-year annual, prefers amended, drops LDF", () => {
    const selection = selectNevadaCycleReports({
      electionYear: 2026,
      rows: [
        listRow({ reportName: "CE Report 2", fileDate: "2026-07-15", syn: "ce2" }),
        listRow({ reportName: "CE Report 2 (Legal Defense Fund)", syn: "ce2ldf" }),
        listRow({ reportName: "CE Report 1", fileDate: "2026-04-15", syn: "ce1" }),
        listRow({
          reportName: "2026 Annual CE Filing (Amended)",
          year: 2025,
          fileDate: "2026-08-25",
          syn: "annual-amended",
        }),
        listRow({ reportName: "2026 Annual CE Filing", year: 2025, fileDate: "2026-01-15", syn: "annual" }),
        listRow({ reportName: "CE Report 4", year: 2024, fileDate: "2025-01-15", syn: "old-ce4" }),
        listRow({ reportName: "2026 Candidate Financial Disclosure", fileDate: "2026-03-23", syn: "fds" }),
        listRow({ reportName: "Tick", fileDate: "2026-01-01", syn: "junk" }),
      ],
    });
    expect(selection.selected.map((report) => report.syn)).toEqual(["annual-amended", "ce1", "ce2"]);
    expect(selection.legalDefenseFundCount).toBe(1);
    expect(selection.unrecognizedReportNames).toEqual([
      "2026 Candidate Financial Disclosure",
      "Tick",
    ]);
  });

  it("breaks same-flag duplicates by file date and refuses exact ties", () => {
    const byDate = selectNevadaCycleReports({
      electionYear: 2026,
      rows: [
        listRow({ reportName: "CE Report 1", fileDate: "2026-04-15", syn: "first" }),
        listRow({ reportName: "CE Report 1", fileDate: "2026-04-20", syn: "second" }),
      ],
    });
    expect(byDate.selected.map((report) => report.syn)).toEqual(["second"]);
    expect(() =>
      selectNevadaCycleReports({
        electionYear: 2026,
        rows: [
          listRow({ reportName: "CE Report 1", fileDate: "2026-04-15", syn: "first" }),
          listRow({ reportName: "CE Report 1", fileDate: "2026-04-15", syn: "clone" }),
        ],
      })
    ).toThrow(/selection tie/);
  });
});

describe("parseNevadaCandidateReportSummary", () => {
  it("reads the real Herndon CE#2 summary table to the fixture values", async () => {
    const html = await readFile(new URL("summary-table-herndon-ce2-2026.html", FIXTURES), "utf8");
    const summary = parseNevadaCandidateReportSummary(html, "herndon ce2");
    expect(summary.lines[1]).toEqual({ periodCents: 0, cumulativeCents: 0 });
    expect(summary.lines[9]).toEqual({ periodCents: 308_90, cumulativeCents: 3_705_44 });
    expect(summary.lines[11]).toEqual({ periodCents: 402_29, cumulativeCents: 560_95 });
    expect(summary.lines[12]).toEqual({ periodCents: 711_19, cumulativeCents: 4_266_39 });
    expect(summary.endingFundBalanceCents).toBe(9_269_50);
  });

  it("rejects non-candidate layouts and internally inconsistent summaries", () => {
    expect(() => parseNevadaCandidateReportSummary("<table></table>", "empty")).toThrow(/missing line 1/);
    const row = (line: string, period: string, cumulative: string) =>
      `<tr><td>${line}</td><td>&nbsp;</td><td>$ ${period}&nbsp;</td><td>&nbsp;$${cumulative}</td></tr>`;
    const lines = [
      row("1. Total Monetary Contributions Received in Excess of $100", "10.00", "10.00"),
      ...[2, 3, 4, 5, 6, 7].map((n) => row(`${n}. Other`, "0.00", "0.00")),
      row("8. Total Amount of All Contributions (Add Lines 1 through 7)", "99.00", "99.00"),
      ...[9, 10, 11].map((n) => row(`${n}. Expense line`, "0.00", "0.00")),
      row("12. Total Amount of All Expenses (Add Lines 9 through 11)", "0.00", "0.00"),
      `<tr><td>13. Fund balance at the end of the reporting period</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;$5.00</td></tr>`,
    ].join("");
    expect(() => parseNevadaCandidateReportSummary(`<table>${lines}</table>`, "bad")).toThrow(
      /line 8 mismatch/
    );
  });
});

describe("buildNevadaCycleSummary", () => {
  function summary(values: {
    line1: number;
    line5?: number;
    line7?: number;
    line8: number;
    line9: number;
    line11?: number;
    line12: number;
    balance: number;
  }): NevadaReportSummary {
    const zero = { periodCents: 0, cumulativeCents: null };
    return {
      lines: {
        1: { periodCents: values.line1, cumulativeCents: null },
        2: zero,
        3: zero,
        4: zero,
        5: { periodCents: values.line5 ?? 0, cumulativeCents: null },
        6: zero,
        7: { periodCents: values.line7 ?? 0, cumulativeCents: null },
        8: { periodCents: values.line8, cumulativeCents: null },
        9: { periodCents: values.line9, cumulativeCents: null },
        10: zero,
        11: { periodCents: values.line11 ?? 0, cumulativeCents: null },
        12: { periodCents: values.line12, cumulativeCents: null },
      },
      endingFundBalanceCents: values.balance,
      filedOn: null,
    };
  }

  function selected(reportName: string, year: number, fileDate: string): NevadaSelectedReport {
    const row = listRow({ reportName, year, fileDate, syn: reportName });
    const { selected: reports } = selectNevadaCycleReports({ rows: [row], electionYear: 2026 });
    expect(reports).toHaveLength(1);
    return reports[0];
  }

  it("sums This-Period columns across the Cannizzaro fixture reports and takes cash from the latest period", () => {
    // Values from backend/tests/fixtures/nevadaFinance/report-summaries.json.
    const annual = summary({
      line1: 618_690_78,
      line5: 3_689_15,
      line7: 23_007_66,
      line8: 645_387_59,
      line9: 237_875_30,
      line11: 418_93,
      line12: 238_294_23,
      balance: 815_319_84,
    });
    const ce1 = summary({
      line1: 261_105_26,
      line5: 4_141_88,
      line7: 10_946_82,
      line8: 276_193_96,
      line9: 100_599_77,
      line11: 223_94,
      line12: 100_823_71,
      balance: 986_448_89,
    });
    const ce2 = summary({
      line1: 562_379_39,
      line5: 3_431_59,
      line7: 23_437_96,
      line8: 589_248_94,
      line9: 1_331_849_44,
      line11: 221_26,
      line12: 1_332_070_70,
      balance: 240_195_54,
    });
    const cycle = buildNevadaCycleSummary([
      { report: selected("2026 Annual CE Filing", 2025, "2026-01-15"), summary: annual },
      { report: selected("CE Report 1", 2026, "2026-04-15"), summary: ce1 },
      { report: selected("CE Report 2", 2026, "2026-07-15"), summary: ce2 },
    ]);
    expect(cycle.totalReceiptsCents).toBe(645_387_59 + 276_193_96 + 589_248_94);
    expect(cycle.totalDisbursementsCents).toBe(238_294_23 + 100_823_71 + 1_332_070_70);
    expect(cycle.cashOnHandCents).toBe(240_195_54);
    expect(cycle.latestPeriodEnd).toBe("2026-06-30");
    expect(cycle.itemizedContributionFloorCents).toBe(
      618_690_78 + 3_689_15 + 261_105_26 + 4_141_88 + 562_379_39 + 3_431_59
    );
    expect(cycle.itemizedContributionCeilingCents).toBe(
      cycle.itemizedContributionFloorCents + 23_007_66 + 10_946_82 + 23_437_96
    );
  });

  it("adds loan lines to the ceiling and clamps negative unitemized slack", () => {
    const mk = (over: Record<number, number>) => {
      const lines: Record<number, { periodCents: number; cumulativeCents: number | null }> = {};
      for (let n = 1; n <= 12; n += 1) lines[n] = { periodCents: over[n] ?? 0, cumulativeCents: null };
      return { lines, endingFundBalanceCents: 0, filedOn: null };
    };
    const report = (end: string) =>
      ({ reportName: "r", year: 2026, fileDate: "2026-07-15", office: "o", syn: end,
         name: { baseName: "r", isAmended: false, isLegalDefenseFund: false },
         period: { start: "2026-01-01", end } }) as never;
    const cycle = buildNevadaCycleSummary([
      { report: report("2026-03-31"), summary: mk({ 1: 100_00, 2: 50_00, 5: 10_00, 7: -20_00, 8: 140_00 }) },
    ]);
    // negative line 7 lowers the floor (reversals may be itemized), loans widen the ceiling
    expect(cycle.itemizedContributionFloorCents).toBe(90_00);
    expect(cycle.itemizedContributionCeilingCents).toBe(160_00);
  });

  it("refuses duplicate periods", () => {
    const ce2 = summary({
      line1: 0,
      line8: 0,
      line9: 0,
      line12: 0,
      balance: 0,
    });
    const report = selected("CE Report 2", 2026, "2026-07-15");
    expect(() =>
      buildNevadaCycleSummary([
        { report, summary: ce2 },
        { report, summary: ce2 },
      ])
    ).toThrow(/two reports ending/);
  });
});
