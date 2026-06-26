import { describe, expect, it } from "vitest";

import {
  aggregateUtahDirectContributions,
  isUtahDirectDonorSupportReceipt,
  isUtahTotalDisbursement,
  isUtahTotalReceipt,
} from "../../../src/pipeline/utahFinance/utahDirectContributionAggregator.js";
import type { UtahDisclosuresTransactionRow } from "../../../src/pipeline/utahFinance/utahDisclosuresClient.js";

function transaction(overrides: Partial<UtahDisclosuresTransactionRow> = {}): UtahDisclosuresTransactionRow {
  return {
    filed: "01/05/2024",
    entityType: "PCC",
    entityName: "Friends of Jane Doe",
    report: "Year End",
    transactionId: "T100",
    transactionType: "Contribution",
    transactionDate: "01/02/2024",
    amount: 100,
    name: "John Smith",
    address1: "1 Main",
    city: "Salt Lake City",
    state: "UT",
    zip: "84111",
    inKind: false,
    loan: false,
    ...overrides,
  };
}

describe("utahDirectContributionAggregator", () => {
  it("aggregates Utah direct contributions by contribution size only", () => {
    const sourceUrl = "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024";
    const result = aggregateUtahDirectContributions({
      electionYear: 2024,
      committeeName: "Friends of Jane Doe",
      sourceUrl,
      transactions: [
        transaction({ amount: 50, transactionId: "T1" }),
        transaction({ amount: 250, transactionId: "T2", name: "Jane Roe", address1: "2 Main" }),
        transaction({ amount: 5_000, transactionId: "T3", name: "Pat Smith", address1: "3 Main" }),
      ],
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 5300,
        directContributionTotal: 5300,
        totalDisbursements: 0,
        sourceUrl,
      },
      directBreakdowns: [
        {
          categoryType: "contribution_size",
          categoryName: "$5,000+",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$1-$99",
          amount: 50,
          contributorCount: 1,
          sourceUrl,
        },
      ],
      matchedTransactionRowCount: 3,
      includedContributionRowCount: 3,
      skippedTransactionRowCount: 0,
    });
  });

  it("separates receipts, direct support, and disbursements", () => {
    const result = aggregateUtahDirectContributions({
      electionYear: 2024,
      transactions: [
        transaction({ amount: 500, transactionId: "contribution" }),
        transaction({ amount: 1_000, transactionId: "loan", loan: true }),
        transaction({ amount: 250, transactionId: "expense", transactionType: "Expenditure", name: "Printer" }),
        transaction({ amount: -300, transactionId: "negative-expense", transactionType: "Expenditure", name: "Mail" }),
        transaction({ amount: -50, transactionId: "refund" }),
      ],
    });

    expect(result.summary).toEqual({
      totalReceipts: 1500,
      directContributionTotal: 500,
      totalDisbursements: 550,
      sourceUrl: null,
    });
    expect(result.matchedTransactionRowCount).toBe(5);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedTransactionRowCount).toBe(4);
    expect(result.directBreakdowns).toEqual([
      {
        categoryType: "contribution_size",
        categoryName: "$500-$999",
        amount: 500,
        contributorCount: 1,
        sourceUrl: null,
      },
    ]);
  });

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateUtahDirectContributions({
      electionYear: 2024,
      transactions: [
        transaction({ transactionId: "T1", amount: 100, name: "John Smith", address1: "1 Main" }),
        transaction({ transactionId: "T2", amount: 200, name: "John Smith", address1: "1 Main" }),
        transaction({ transactionId: "T3", amount: 300, name: "Jane Roe", address1: "2 Main" }),
      ],
    });

    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryName: "$100-$249", amount: 300, contributorCount: 1 }),
        expect.objectContaining({ categoryName: "$250-$499", amount: 300, contributorCount: 1 }),
      ])
    );
  });

  it("matches committee and election year conservatively", () => {
    const result = aggregateUtahDirectContributions({
      electionYear: 2024,
      committeeName: "Friends of Jane Doe",
      transactions: [
        transaction({ transactionId: "same", amount: 100, transactionDate: "01/01/2024" }),
        transaction({ transactionId: "old", amount: 200, transactionDate: "12/31/2023" }),
        transaction({ transactionId: "other", amount: 300, entityName: "Other Committee" }),
        transaction({ transactionId: "filed", amount: 400, transactionDate: undefined, filed: "02/01/2024" }),
        transaction({ transactionId: "no-date", amount: 500, transactionDate: undefined, filed: undefined }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(1000);
    expect(result.matchedTransactionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(3);
  });

  it("matches Utah folder titles with office/year suffixes to transaction entity names", () => {
    const result = aggregateUtahDirectContributions({
      electionYear: 2024,
      committeeName: "Cox, Spencer (2020 Governor)",
      transactions: [
        transaction({
          entityName: "Cox, Spencer",
          transactionType: "Contribution",
          amount: 100,
          name: "Utah Builders PAC",
        }),
        transaction({
          entityName: "Cox, Spencer",
          transactionType: "Expenditure",
          amount: -25,
          name: "Mailer Vendor",
        }),
      ],
    });

    expect(result.summary).toEqual({
      totalReceipts: 100,
      directContributionTotal: 100,
      totalDisbursements: 25,
      sourceUrl: null,
    });
    expect(result.directBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "contribution_size",
        categoryName: "$100-$249",
        amount: 100,
      }),
    ]);
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateUtahDirectContributions({
      electionYear: 2024,
      transactions: [transaction({ amount: 0.1 }), transaction({ transactionId: "T2", amount: 0.2 })],
    });

    expect(result.summary.totalReceipts).toBe(0.3);
    expect(result.directBreakdowns).toEqual([
      expect.objectContaining({ categoryName: "$1-$99", amount: 0.3, contributorCount: 1 }),
    ]);
  });

  it("classifies Utah total and direct receipts from official transaction fields", () => {
    const valid = transaction({ amount: 250, transactionDate: "01/10/2024", transactionType: "Contribution" });
    expect(isUtahTotalReceipt({ transaction: valid, electionYear: 2024 })).toBe(true);
    expect(isUtahDirectDonorSupportReceipt({ transaction: valid, electionYear: 2024 })).toBe(true);
    expect(isUtahDirectDonorSupportReceipt({ transaction: transaction({ loan: true }), electionYear: 2024 })).toBe(false);
    expect(isUtahTotalDisbursement({ transaction: transaction({ transactionType: "Expenditure" }), electionYear: 2024 })).toBe(true);
    expect(
      isUtahTotalDisbursement({
        transaction: transaction({ amount: -10, transactionType: "Expenditure" }),
        electionYear: 2024,
      })
    ).toBe(true);
    expect(isUtahTotalReceipt({ transaction: transaction({ amount: -10 }), electionYear: 2024 })).toBe(false);
    expect(
      isUtahTotalReceipt({
        transaction: transaction({ entityName: "Other Committee" }),
        electionYear: 2024,
        committeeName: "Friends of Jane Doe",
      })
    ).toBe(false);
  });

  it("validates aggregation inputs", () => {
    expect(() => aggregateUtahDirectContributions({ electionYear: 1997, transactions: [] })).toThrow(
      "Invalid Utah direct contribution aggregation election year"
    );
    expect(() =>
      aggregateUtahDirectContributions({ electionYear: 2024, transactions: [], maxBreakdownsPerCategory: 0 })
    ).toThrow("maxBreakdownsPerCategory");
  });
});
