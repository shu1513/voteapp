import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  nevadaFilerKey,
  parseNevadaContributionCsv,
  parseNevadaCsvDate,
  parseNevadaCurrencyCents,
  parseNevadaExpenditureCsv,
} from "../../../src/pipeline/nevadaFinance/nevadaAuroraCsv.js";

const FIXTURES = new URL("../../fixtures/nevadaFinance/", import.meta.url);

async function fixture(name: string): Promise<string> {
  return readFile(new URL(name, FIXTURES), "utf8");
}

describe("nevadaAuroraCsv", () => {
  it("parses the Hansen Q2 contribution export to the reconciled fixture sum", async () => {
    const rows = parseNevadaContributionCsv(await fixture("contributions-hansen-q2-2026.csv"));
    expect(rows).toHaveLength(11);
    expect(rows.reduce((sum, row) => sum + row.amountCents, 0)).toBe(18_000_00);
    expect(rows.every((row) => row.filerKey === "ALEXIS M HANSEN")).toBe(true);
    expect(rows.every((row) => !row.isLegalDefenseFund)).toBe(true);
    expect(rows[0]).toMatchObject({
      contributorName: "CCF",
      date: "2026-06-08",
      transactionType: "Monetary Contribution",
      reportName: "2026 CE Report 2",
    });
  });

  it("parses the Hansen Q2 expenditure export to the reconciled fixture sum", async () => {
    const rows = parseNevadaExpenditureCsv(await fixture("expenditures-hansen-q2-2026.csv"));
    expect(rows).toHaveLength(12);
    expect(rows.reduce((sum, row) => sum + row.amountCents, 0)).toBe(23_034_31);
  });

  it("returns no rows for a header-only export (a zero-contribution filer)", async () => {
    expect(parseNevadaContributionCsv(await fixture("contributions-herndon-q2-2026.csv"))).toEqual([]);
  });

  it("handles the statewide sample's doubled-quote payees, amended reports, and in-kind rows", async () => {
    const rows = parseNevadaExpenditureCsv(await fixture("statewide-june-2026-expenditures-sample.csv"));
    expect(rows[0].payeeName).toBe('"Anedot"');
    expect(rows.some((row) => row.reportName === "2026 CE Report 2 (Amended)")).toBe(true);

    const contributions = parseNevadaContributionCsv(
      await fixture("statewide-june-2026-contributions-sample.csv")
    );
    expect(contributions).toHaveLength(40);
    expect(contributions.filter((row) => row.transactionType === "In Kind Contribution")).toHaveLength(10);
    expect(contributions.every((row) => /^2026-06-\d{2}$/.test(row.date))).toBe(true);
  });

  it("normalizes filer keys by case and internal whitespace", () => {
    expect(nevadaFilerKey("Vegas  Chamber")).toBe("VEGAS CHAMBER");
    expect(nevadaFilerKey("  Joseph Lombardo ")).toBe("JOSEPH LOMBARDO");
  });

  it("rejects malformed amounts, dates, and transaction types", () => {
    expect(() => parseNevadaCurrencyCents("1,000.00", "t")).toThrow(/Invalid Nevada AURORA amount/);
    expect(() => parseNevadaCurrencyCents("$1,00.00", "t")).toThrow(/Invalid Nevada AURORA amount/);
    expect(parseNevadaCurrencyCents("$1,000.00", "t")).toBe(100_000);
    expect(parseNevadaCurrencyCents("$ 5,700.50", "t")).toBe(570_050);
    expect(parseNevadaCurrencyCents("($2,500.00)", "t")).toBe(-250_000);
    expect(() => parseNevadaCurrencyCents("($2,500.00", "t")).toThrow(/Invalid Nevada AURORA amount/);
    expect(() => parseNevadaCsvDate("13/1/2026", "t")).toThrow(/Invalid Nevada AURORA date/);
    expect(() => parseNevadaCsvDate("2/30/2026", "t")).toThrow(/Invalid Nevada AURORA date/);
    expect(() => parseNevadaCsvDate("2/29/2026", "t")).toThrow(/Invalid Nevada AURORA date/);
    expect(() => parseNevadaCsvDate("4/31/2026", "t")).toThrow(/Invalid Nevada AURORA date/);
    expect(parseNevadaCsvDate("2/29/2024", "t")).toBe("2024-02-29");
    expect(parseNevadaCsvDate("4/5/2026", "t")).toBe("2026-04-05");
    expect(() =>
      parseNevadaContributionCsv(
        '"Contributor","Date","Amount","Type","Recipient","Report"\r\n"A","1/1/2026","$1.00","Loan","B","R"\r\n'
      )
    ).toThrow(/Unknown Nevada contribution type/);
  });
});
