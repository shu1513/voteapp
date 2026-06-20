import { describe, expect, it } from "vitest";

import {
  aggregateCaliforniaDirectContributions,
  type CalAccessReceiptRow,
} from "../../../src/pipeline/californiaFinance/californiaDirectContributionAggregator.js";

function receipt(overrides: Partial<CalAccessReceiptRow> = {}): CalAccessReceiptRow {
  return {
    FILING_ID: "F1",
    FORM_TYPE: "A",
    TRAN_ID: "T1",
    ENTITY_CD: "IND",
    CTRIB_NAML: "DOE",
    CTRIB_NAMF: "JANE",
    CTRIB_CITY: "LOS ANGELES",
    CTRIB_ST: "CA",
    CTRIB_EMP: "ACME INC",
    CTRIB_OCC: "Engineer",
    RCPT_DATE: "2/1/2026 12:00:00 AM",
    AMOUNT: "100.00",
    CMTE_ID: "1456045",
    CAND_NAML: "NEWSOM",
    CAND_NAMF: "GAVIN",
    OFFICE_CD: "GOV",
    OFFIC_DSCR: "Governor",
    SUP_OPP_CD: "",
    ...overrides,
  };
}

describe("californiaDirectContributionAggregator", () => {
  it("aggregates direct receipts by occupation, employer, and contribution size", () => {
    const result = aggregateCaliforniaDirectContributions({
      controlledCommitteeId: "1456045",
      electionYear: 2026,
      receiptRows: [
        receipt({ TRAN_ID: "T1", CTRIB_EMP: "Acme Inc", CTRIB_OCC: "Engineer", AMOUNT: "100.00" }),
        receipt({ TRAN_ID: "T2", CTRIB_EMP: "Acme Inc", CTRIB_OCC: "Engineer", AMOUNT: "$250.00" }),
        receipt({ TRAN_ID: "T3", CTRIB_EMP: "Mega Corp", CTRIB_OCC: "Teacher", AMOUNT: "5,000.00" }),
      ],
      sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 5350,
        sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
        },
        {
          categoryType: "occupation",
          categoryName: "Engineer",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
        },
        {
          categoryType: "employer",
          categoryName: "Mega Corp",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
        },
        {
          categoryType: "employer",
          categoryName: "Acme Inc",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$5,000+",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 100,
          contributorCount: 1,
          sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
        },
      ],
      matchedReceiptRowCount: 3,
      includedReceiptRowCount: 3,
      skippedReceiptRowCount: 0,
    });
  });

  it("matches receipts by filing id when CMTE_ID is blank", () => {
    const result = aggregateCaliforniaDirectContributions({
      controlledCommitteeId: "1456045",
      controlledCommitteeFilingIds: ["F42"],
      electionYear: 2026,
      receiptRows: [
        receipt({ FILING_ID: "F42", CMTE_ID: "", AMOUNT: "300", CTRIB_OCC: "Attorney", CTRIB_EMP: "Law Firm" }),
        receipt({ FILING_ID: "F99", CMTE_ID: "", AMOUNT: "900", CTRIB_OCC: "Doctor", CTRIB_EMP: "Hospital" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(300);
    expect(result.matchedReceiptRowCount).toBe(1);
    expect(result.includedReceiptRowCount).toBe(1);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 300 }),
        expect.objectContaining({ categoryType: "employer", categoryName: "Law Firm", amount: 300 }),
      ])
    );
  });

  it("uses filing id instead of CMTE_ID when controlled filing ids are available", () => {
    const result = aggregateCaliforniaDirectContributions({
      controlledCommitteeId: "1456045",
      controlledCommitteeFilingIds: ["F42"],
      electionYear: 2026,
      receiptRows: [
        receipt({ FILING_ID: "F42", CMTE_ID: "", AMOUNT: "300", CTRIB_OCC: "Attorney", CTRIB_EMP: "Law Firm" }),
        receipt({ FILING_ID: "F99", CMTE_ID: "1456045", AMOUNT: "900", CTRIB_OCC: "Doctor", CTRIB_EMP: "Hospital" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(300);
    expect(result.matchedReceiptRowCount).toBe(1);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 300 }),
        expect.objectContaining({ categoryType: "employer", categoryName: "Law Firm", amount: 300 }),
      ])
    );
    expect(result.directBreakdowns).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryName: "Doctor" }),
        expect.objectContaining({ categoryName: "Hospital" }),
      ])
    );
  });

  it("filters to the California two-year election cycle", () => {
    const result = aggregateCaliforniaDirectContributions({
      controlledCommitteeId: "1456045",
      electionYear: 2026,
      receiptRows: [
        receipt({ RCPT_DATE: "12/31/2024 12:00:00 AM", AMOUNT: "100" }),
        receipt({ RCPT_DATE: "1/1/2025 12:00:00 AM", AMOUNT: "200" }),
        receipt({ RCPT_DATE: "2026-11-01", AMOUNT: "300" }),
        receipt({ RCPT_DATE: "1/1/2027 12:00:00 AM", AMOUNT: "400" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.matchedReceiptRowCount).toBe(4);
    expect(result.includedReceiptRowCount).toBe(2);
    expect(result.skippedReceiptRowCount).toBe(2);
  });

  it("skips malformed, zero, negative, and wrong-committee rows", () => {
    const result = aggregateCaliforniaDirectContributions({
      controlledCommitteeId: "1456045",
      electionYear: 2026,
      receiptRows: [
        receipt({ AMOUNT: "0" }),
        receipt({ AMOUNT: "-10" }),
        receipt({ AMOUNT: "not money" }),
        receipt({ RCPT_DATE: "", AMOUNT: "100" }),
        receipt({ CMTE_ID: "9999999", AMOUNT: "500" }),
        receipt({ AMOUNT: "250" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(250);
    expect(result.matchedReceiptRowCount).toBe(5);
    expect(result.includedReceiptRowCount).toBe(1);
    expect(result.skippedReceiptRowCount).toBe(4);
  });

  it("limits occupation and employer breakdowns without dropping contribution-size buckets", () => {
    const result = aggregateCaliforniaDirectContributions({
      controlledCommitteeId: "1456045",
      electionYear: 2026,
      maxBreakdownsPerCategory: 1,
      receiptRows: [
        receipt({ CTRIB_OCC: "Engineer", CTRIB_EMP: "Acme", AMOUNT: "100" }),
        receipt({ CTRIB_OCC: "Teacher", CTRIB_EMP: "School", AMOUNT: "300" }),
        receipt({ CTRIB_OCC: "Doctor", CTRIB_EMP: "Hospital", AMOUNT: "600" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Doctor", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "employer")).toEqual([
      expect.objectContaining({ categoryName: "Hospital", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("validates required aggregation inputs", () => {
    expect(() =>
      aggregateCaliforniaDirectContributions({
        controlledCommitteeId: " ",
        electionYear: 2026,
        receiptRows: [],
      })
    ).toThrow("California controlled committee id is required");

    expect(() =>
      aggregateCaliforniaDirectContributions({
        controlledCommitteeId: "1456045",
        electionYear: 1899,
        receiptRows: [],
      })
    ).toThrow("Invalid California direct contribution aggregation election year");

    expect(() =>
      aggregateCaliforniaDirectContributions({
        controlledCommitteeId: "1456045",
        electionYear: 2026,
        receiptRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid California direct contribution aggregation maxBreakdownsPerCategory");
  });
});
