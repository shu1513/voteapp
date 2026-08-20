import { describe, expect, it } from "vitest";

import type { NewHampshireReceiptRow } from "../../../src/pipeline/newHampshireFinance/newHampshireCfsClient.js";
import {
  aggregateNewHampshireDirectContributions,
  NEW_HAMPSHIRE_DIRECT_CONTRIBUTION_SUBTYPE_CODES,
} from "../../../src/pipeline/newHampshireFinance/newHampshireDirectContributionAggregator.js";

function receipt(overrides: Partial<NewHampshireReceiptRow> = {}): NewHampshireReceiptRow {
  return {
    transactionId: 1,
    transactionVersionId: 1,
    guid: "00000000-0000-4000-8000-000000000001",
    filerReportId: 10,
    filerReportVersionId: 1,
    filerEntityId: 50450,
    filerName: "Example Committee",
    transactionAmount: 100,
    transactionDate: "2026-06-01T00:00:00",
    transactionTypeDescription: "Receipt",
    transactionSubType: "Monetary Contribution",
    transactionSubTypeCode: "MTCB",
    reportName: "2026 R&E Report - 06/17/2026",
    reportVersion: false,
    reportVersionFilter: "RPTFLD",
    reportVersionDescription: "No",
    isAmended: false,
    electionCycle: "2026 Election Cycle",
    employerName: null,
    occupation: null,
    ...overrides,
  };
}

describe("newHampshireDirectContributionAggregator", () => {
  it("selects current reports, retains unitemized money, and emits only size and employer-derived industry amounts", () => {
    const sourceUrl =
      "https://cfsapi.sos.nh.gov/api/PublicTransactionDetails/GetPublicContributionDetails";
    const result = aggregateNewHampshireDirectContributions({
      filingEntityId: 50450,
      electionYear: 2026,
      sourceUrl,
      receiptRows: [
        receipt({ transactionId: 101, filerReportVersionId: 1, transactionAmount: 100, employerName: "Google" }),
        receipt({
          transactionId: 102,
          filerReportVersionId: 1,
          transactionAmount: 40,
          transactionSubType: "Unitemized Monetary Contribution",
          transactionSubTypeCode: "NITMY",
        }),
        receipt({
          transactionId: 201,
          filerReportVersionId: 2,
          transactionAmount: 150,
          employerName: "Google",
          reportVersion: true,
          reportVersionFilter: "RPTAMD",
        }),
        receipt({
          transactionId: 202,
          filerReportVersionId: 2,
          transactionAmount: 50,
          transactionSubType: "Unitemized Monetary Contribution",
          transactionSubTypeCode: "NITMY",
          reportVersion: true,
          reportVersionFilter: "RPTAMD",
        }),
        receipt({
          transactionId: 301,
          filerReportId: 11,
          transactionAmount: 250,
          transactionSubType: "In-Kind (Non-Money) Contribution",
          transactionSubTypeCode: "INKIND",
          employerName: "Concord Hospital",
        }),
        receipt({
          transactionId: 401,
          filerReportId: 12,
          transactionAmount: 25,
          transactionSubType: "Interest Earned",
          transactionSubTypeCode: "ITR",
        }),
        receipt({ transactionId: 501, filerReportId: 13, transactionAmount: 0.1 }),
        receipt({ transactionId: 601, filerReportId: 14, transactionAmount: 0.2 }),
      ],
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 475.3,
        directContributionTotal: 450.3,
        sourceUrl,
      },
      directBreakdowns: [
        {
          categoryType: "industry",
          categoryName: "healthcare",
          amount: 250,
          contributorCount: null,
          sourceUrl,
        },
        {
          categoryType: "industry",
          categoryName: "technology",
          amount: 150,
          contributorCount: null,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: null,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 150,
          contributorCount: null,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$1-$99",
          amount: 50,
          contributorCount: null,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$0.01-$0.99",
          amount: 0.3,
          contributorCount: null,
          sourceUrl,
        },
      ],
      sourceRowCount: 8,
      currentVersionRowCount: 6,
      supersededRowCount: 2,
      directContributionRowCount: 5,
      nonDirectReceiptRowCount: 1,
      nonPositiveRowCount: 0,
    });
    expect(result.directBreakdowns.map((row) => row.categoryType)).not.toContain("occupation");
  });

  it("pins the official direct-donor subtype-code vocabulary", () => {
    expect([...NEW_HAMPSHIRE_DIRECT_CONTRIBUTION_SUBTYPE_CODES].sort()).toEqual([
      "INKIND",
      "ITMC",
      "ITMY",
      "ITNMC",
      "MTCB",
      "NITMC",
      "NITMY",
      "NITNMC",
    ]);
  });

  it("skips non-positive current rows without floating-point drift", () => {
    const result = aggregateNewHampshireDirectContributions({
      filingEntityId: 50450,
      electionYear: 2026,
      receiptRows: [
        receipt({ transactionId: 1, filerReportId: 1, transactionAmount: 0 }),
        receipt({ transactionId: 2, filerReportId: 2, transactionAmount: -10 }),
        receipt({ transactionId: 3, filerReportId: 3, transactionAmount: 0.1 }),
        receipt({ transactionId: 4, filerReportId: 4, transactionAmount: 0.2 }),
      ],
    });

    expect(result.summary).toEqual({
      totalReceipts: 0.3,
      directContributionTotal: 0.3,
      sourceUrl: null,
    });
    expect(result.nonPositiveRowCount).toBe(2);
    expect(result.directBreakdowns).toEqual([
      {
        categoryType: "contribution_size",
        categoryName: "$0.01-$0.99",
        amount: 0.3,
        contributorCount: null,
        sourceUrl: null,
      },
    ]);
  });

  it("fails closed on inexact filer, cycle, transaction type, or unknown subtype", () => {
    const aggregate = (row: NewHampshireReceiptRow) =>
      aggregateNewHampshireDirectContributions({
        filingEntityId: 50450,
        electionYear: 2026,
        receiptRows: [row],
      });

    expect(() => aggregate(receipt({ filerEntityId: 99999 }))).toThrow(
      "expected filer 50450, received 99999"
    );
    expect(() => aggregate(receipt({ electionCycle: "2024 Election Cycle" }))).toThrow(
      "expected 2026 Election Cycle"
    );
    expect(() => aggregate(receipt({ transactionTypeDescription: "Return Receipt" }))).toThrow(
      "returned transaction type"
    );
    expect(() => aggregate(receipt({ transactionSubType: "Mystery", transactionSubTypeCode: "NEW" }))).toThrow(
      "Unknown New Hampshire receipt subtype"
    );
  });

  it("rejects invalid aggregation inputs and unsafe amounts", () => {
    expect(() =>
      aggregateNewHampshireDirectContributions({
        filingEntityId: 0,
        electionYear: 2026,
        receiptRows: [],
      })
    ).toThrow("Invalid New Hampshire direct contribution filingEntityId");
    expect(() =>
      aggregateNewHampshireDirectContributions({
        filingEntityId: 50450,
        electionYear: 2015,
        receiptRows: [],
      })
    ).toThrow("Invalid New Hampshire direct contribution election year");
    expect(() =>
      aggregateNewHampshireDirectContributions({
        filingEntityId: 50450,
        electionYear: 2026,
        receiptRows: [receipt({ transactionAmount: Number.POSITIVE_INFINITY })],
      })
    ).toThrow("Invalid New Hampshire receipt amount");
  });
});
