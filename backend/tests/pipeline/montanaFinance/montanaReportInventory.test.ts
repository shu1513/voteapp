import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

import { parseMontanaCersReportInventory } from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import type { MontanaCersReportInventoryRow } from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import { selectMontanaCanonicalReports } from "../../../src/pipeline/montanaFinance/montanaReportInventory.js";

const fixtures = new URL("../../fixtures/montanaFinance/", import.meta.url);

function row(overrides: Partial<MontanaCersReportInventoryRow>): MontanaCersReportInventoryRow {
  return {
    reportId: 1,
    entitySubId: 21020,
    formTypeCode: "C5",
    formTypeDescr: null,
    fromDateStr: "01/01/2026",
    toDateStr: "03/15/2026",
    reportTypeDescr: "Periodic",
    statusCode: "FILED",
    statusDescr: "Filed",
    primCashBegCents: 0,
    genCashBegCents: 0,
    receivedDate: 1_000,
    amendedDate: null,
    ...overrides,
  };
}

describe("selectMontanaCanonicalReports", () => {
  it("selects C5s from the fixture and excludes incorporated C7s", async () => {
    const inventory = parseMontanaCersReportInventory(
      await readFile(new URL("report-inventory-sanitized.json", fixtures), "utf8")
    );
    const selection = selectMontanaCanonicalReports(inventory);
    expect(selection.reports.map((report) => report.reportId)).toEqual([75674, 76083, 76535, 77491]);
    expect(selection.diagnostics).toEqual([{ reportId: 79526, reason: "incorporated" }]);
    expect(selection.hasOverlappingPeriods).toBe(false);
  });

  it("picks the amended row over the base when a period ever duplicates", () => {
    const base = row({ reportId: 10, receivedDate: 1_000 });
    const amended = row({ reportId: 11, statusCode: "AMEND", statusDescr: "Amended", amendedDate: 2_000 });
    const selection = selectMontanaCanonicalReports([base, amended]);
    expect(selection.reports.map((report) => report.reportId)).toEqual([11]);
    expect(selection.diagnostics).toEqual([{ reportId: 10, reason: "superseded_duplicate" }]);
  });

  it("breaks duplicate ties by receivedDate then reportId, never a filed-date string", () => {
    const older = row({ reportId: 12, receivedDate: 1_000 });
    const newer = row({ reportId: 13, receivedDate: 2_000 });
    expect(selectMontanaCanonicalReports([newer, older]).reports.map((report) => report.reportId)).toEqual([13]);
    const twinA = row({ reportId: 14 });
    const twinB = row({ reportId: 15 });
    expect(selectMontanaCanonicalReports([twinA, twinB]).reports.map((report) => report.reportId)).toEqual([15]);
  });

  it("excludes non-chain forms and unexpected statuses with diagnostics", () => {
    const c7 = row({ reportId: 20, formTypeCode: "C7", fromDateStr: "05/21/2026", toDateStr: "05/21/2026" });
    const weird = row({ reportId: 21, statusCode: "DRAFT", statusDescr: "Draft" });
    const good = row({ reportId: 22, fromDateStr: "03/16/2026", toDateStr: "04/15/2026" });
    const selection = selectMontanaCanonicalReports([c7, weird, good]);
    expect(selection.reports.map((report) => report.reportId)).toEqual([22]);
    expect(selection.diagnostics).toEqual([
      { reportId: 20, reason: "non_chain_form" },
      { reportId: 21, reason: "unexpected_status" },
    ]);
  });

  it("flags overlapping canonical periods", () => {
    const first = row({ reportId: 30, fromDateStr: "01/01/2026", toDateStr: "03/15/2026" });
    const overlapping = row({ reportId: 31, fromDateStr: "03/10/2026", toDateStr: "04/15/2026" });
    const selection = selectMontanaCanonicalReports([first, overlapping]);
    expect(selection.hasOverlappingPeriods).toBe(true);
  });

  it("sorts canonical reports across a year boundary", () => {
    const december = row({ reportId: 40, fromDateStr: "10/01/2025", toDateStr: "12/31/2025" });
    const january = row({ reportId: 41, fromDateStr: "01/01/2026", toDateStr: "03/15/2026" });
    expect(selectMontanaCanonicalReports([january, december]).reports.map((report) => report.reportId)).toEqual([
      40, 41,
    ]);
  });
});
