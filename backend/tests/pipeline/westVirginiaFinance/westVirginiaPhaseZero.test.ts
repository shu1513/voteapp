import { describe, expect, it } from "vitest";

import type { WestVirginiaTransactionRow } from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsClient.js";
import type { WestVirginiaContributionCsvRow } from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsCsv.js";
import {
  apiAmountToCents,
  pdfHasFontMarker,
  reconcileWestVirginiaCommittee,
  summarizeWestVirginiaContributionCsv,
  summarizeWestVirginiaOccupations,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaPhaseZero.js";

function csvRow(overrides: Partial<WestVirginiaContributionCsvRow>): WestVirginiaContributionCsvRow {
  return {
    line: 2,
    registrantId: "1010000001",
    committeeName: "Test Committee",
    candidateName: "Test Candidate",
    transactionType: "Contributions",
    transactionCategory: "Monetary",
    transactionDate: "2026-06-17",
    amountCents: 2_544,
    contributorType: "Individual",
    contributorName: "Jacob Hively",
    employerName: null,
    filedDate: "2026-07-07",
    recovered: false,
    ...overrides,
  };
}

function apiRow(overrides: Partial<WestVirginiaTransactionRow>): WestVirginiaTransactionRow {
  return {
    transactionID: 1,
    entityID: "1010000001",
    orgID: 1,
    committeeName: "Test Committee",
    candidateName: "Test, Candidate",
    transactionAmount: 25.44,
    transactionDate: "2026-06-17T00:00:00",
    filedDate: "2026-07-07T00:00:00",
    entityTypeDesc: "Individual",
    transactionCategoryDesc: "Monetary",
    transactionTypeDesc: "Contributions",
    transactionPurpose: null,
    contributorPayeeName: "Jacob Hively",
    employerName: null,
    employerOccupation: null,
    transactionTotalYTD: null,
    amendedFlag: false,
    reportVersionID: "1",
    reportFileName: "2026 2nd Quarter Report",
    s3ReportFilePath: null,
    stanceDescription: null,
    candidateNameAssocation: null,
    ballotMeasureDescription: null,
    orgType: "State Candidate",
    ...overrides,
  };
}

describe("apiAmountToCents", () => {
  it("converts cent-precise amounts and rejects sub-cent residue", () => {
    expect(apiAmountToCents(25.44)).toBe(2_544);
    expect(apiAmountToCents(897.1)).toBe(89_710);
    expect(() => apiAmountToCents(0.001)).toThrow(/not cent-precise/);
  });
});

describe("summarizeWestVirginiaContributionCsv", () => {
  it("counts byte-identical duplicates without collapsing them", () => {
    const rows = [csvRow({}), csvRow({}), csvRow({ amountCents: 100 })];
    const summary = summarizeWestVirginiaContributionCsv(rows);
    expect(summary.rowCount).toBe(3);
    expect(summary.duplicateRowCount).toBe(1);
    expect(summary.totalCents).toBe(2_544 + 2_544 + 100);
    expect(summary.registrantCount).toBe(1);
  });
});

describe("reconcileWestVirginiaCommittee", () => {
  const categories = new Set(["Monetary", "In-Kind", "Other Income", "Receipt of Transfer of Excess Funds", "Return"]);

  it("matches identical multisets and totals", () => {
    const result = reconcileWestVirginiaCommittee({
      entityId: "1010000001",
      csvRows: [csvRow({}), csvRow({ amountCents: 100, transactionDate: "2026-01-02" })],
      apiRows: [apiRow({}), apiRow({ transactionAmount: 1, transactionDate: "2026-01-02T00:00:00" })],
      contributionCategories: categories,
    });
    expect(result.totalsMatch).toBe(true);
    expect(result.multisetMatch).toBe(true);
    expect(result.apiReports).toHaveLength(1);
  });

  it("surfaces asymmetric rows and excludes non-contribution API categories", () => {
    const result = reconcileWestVirginiaCommittee({
      entityId: "1010000001",
      csvRows: [csvRow({})],
      apiRows: [
        apiRow({ transactionAmount: 99 }),
        apiRow({ transactionCategoryDesc: "Loans", transactionAmount: 500 }),
      ],
      contributionCategories: categories,
    });
    expect(result.totalsMatch).toBe(false);
    expect(result.onlyInCsv).toBe(1);
    expect(result.onlyInApi).toBe(1);
    expect(result.apiRowCount).toBe(1);
  });

  it("ignores rows from other committees", () => {
    const result = reconcileWestVirginiaCommittee({
      entityId: "1010000001",
      csvRows: [csvRow({}), csvRow({ registrantId: "1010000002" })],
      apiRows: [apiRow({}), apiRow({ entityID: "1010000002" })],
      contributionCategories: categories,
    });
    expect(result.csvRowCount).toBe(1);
    expect(result.apiRowCount).toBe(1);
    expect(result.multisetMatch).toBe(true);
  });
});

describe("summarizeWestVirginiaOccupations", () => {
  it("computes single-transaction and YTD views separately", () => {
    const rows = [
      apiRow({ transactionAmount: 300, employerOccupation: "Attorney/Legal", transactionTotalYTD: "300.0000" }),
      apiRow({ transactionAmount: 100, employerOccupation: null, transactionTotalYTD: "351.0000" }),
      apiRow({ transactionAmount: 100, employerOccupation: "Retired", transactionTotalYTD: null }),
      apiRow({ entityTypeDesc: "Political Action Committee", transactionAmount: 5_000 }),
    ];
    const summary = summarizeWestVirginiaOccupations(rows);
    expect(summary.individualRowCount).toBe(3);
    expect(summary.over250SingleTransaction).toEqual({ rowCount: 1, occupationFilled: 1, employerFilled: 0 });
    expect(summary.over250Ytd).toEqual({ rowCount: 2, occupationFilled: 1 });
    expect(summary.distinctOccupations.map((entry) => entry.value)).toEqual(["Attorney/Legal", "Retired"]);
  });
});

describe("pdfHasFontMarker", () => {
  it("detects the /Font marker", () => {
    expect(pdfHasFontMarker(new TextEncoder().encode("%PDF-1.4 ... /Font ..."))).toBe(true);
    expect(pdfHasFontMarker(new TextEncoder().encode("%PDF-1.4 image only"))).toBe(false);
  });
});
