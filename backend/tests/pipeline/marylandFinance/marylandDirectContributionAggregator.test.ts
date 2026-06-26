import { describe, expect, it } from "vitest";

import {
  aggregateMarylandDirectContributions,
  isMarylandDirectDonorSupportReceipt,
  isMarylandTotalReceipt,
  marylandElectionCycleStartYear,
} from "../../../src/pipeline/marylandFinance/marylandDirectContributionAggregator.js";
import type { MarylandCfsContributionRow } from "../../../src/pipeline/marylandFinance/marylandCfsArtifactReader.js";

function contribution(overrides: Partial<MarylandCfsContributionRow> = {}): MarylandCfsContributionRow {
  return {
    "Filing Entity Id": "16018290",
    "Committee Name": "Gallucci, Justin Friends of",
    "Abbreviated Committee Name": "",
    "Committee Type": "Candidate Committee",
    "Contributor Type": "Individual",
    "Contributor Company Name": "",
    "Contributor Last Name": "Doe",
    "Contributor First Name": "Jane",
    "Contributor Middle Name": "",
    "Contributor Mailing Address1": "100 Main St",
    "Contributor Mailing Address2": "",
    "Contributor City": "Annapolis",
    "Contributor State": "MD",
    "Contributor ZipCode": '="21401"',
    "Contributor County Of Residence": "Anne Arundel",
    "Transaction Type": "Contribution",
    "Transaction Date": "01/10/2026",
    "Transaction Amount": "$250.00",
    "Payment Type": "Check",
    "Fund Type": "Electoral",
    "Number Of People Purchasing Or Making Contributions": "",
    "Price Per Person Or Average Contribution": "",
    "Coordinated In-Kind": "False",
    "Public Funding Requested": "False",
    "Amount Eligible For Public Funding": "$0.00",
    Description: "",
    "Report Name": "2026 Pre-Primary 1 Gubernatorial",
    "Aggregate As Of Download Date": "$250.00",
    ...overrides,
  };
}

describe("marylandDirectContributionAggregator", () => {
  it("aggregates direct receipts by contribution size only", () => {
    const sourceUrl = "https://campaignfinance.maryland.gov/public/cf/downloads";
    const result = aggregateMarylandDirectContributions({
      committeeId: "16018290",
      electionYear: 2026,
      sourceUrl,
      contributionRows: [
        contribution({ "Transaction Amount": "$100.00" }),
        contribution({
          "Transaction Amount": "$250.00",
          "Contributor Last Name": "Roe",
          "Contributor Mailing Address1": "200 State Cir",
        }),
        contribution({
          "Transaction Amount": "$5,000.00",
          "Contributor Last Name": "Smith",
          "Contributor Mailing Address1": "300 West St",
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
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
          categoryName: "$100-$249",
          amount: 100,
          contributorCount: 1,
          sourceUrl,
        },
      ],
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
    });
    expect(result.directBreakdowns.every((row) => row.categoryType === "contribution_size")).toBe(true);
  });

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateMarylandDirectContributions({
      committeeId: "16018290",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Amount": "$100.00" }),
        contribution({ "Transaction Amount": "$200.00" }),
        contribution({
          "Transaction Amount": "$300.00",
          "Contributor Last Name": "Roe",
          "Contributor Mailing Address1": "200 State Cir",
        }),
      ],
    });

    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 300,
          contributorCount: 1,
        }),
        expect.objectContaining({
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 300,
          contributorCount: 1,
        }),
      ])
    );
  });

  it("does not collapse blank-identity contribution rows into one contributor", () => {
    const blankContributor = {
      "Contributor Type": "",
      "Contributor Company Name": "",
      "Contributor Last Name": "",
      "Contributor First Name": "",
      "Contributor Middle Name": "",
      "Contributor Mailing Address1": "",
      "Contributor City": "",
      "Contributor State": "",
      "Contributor ZipCode": "",
      Description: "",
    };
    const result = aggregateMarylandDirectContributions({
      committeeId: "16018290",
      electionYear: 2026,
      contributionRows: [
        contribution({ ...blankContributor, "Transaction Amount": "$100.00" }),
        contribution({ ...blankContributor, "Transaction Amount": "$150.00" }),
      ],
    });

    expect(result.directBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "contribution_size",
        categoryName: "$100-$249",
        amount: 250,
        contributorCount: 2,
      }),
    ]);
  });

  it("matches committee IDs case-insensitively and accepts public financing candidate committees", () => {
    const result = aggregateMarylandDirectContributions({
      committeeId: " md-committee-1 ",
      electionYear: 2026,
      contributionRows: [
        contribution({
          "Filing Entity Id": "MD-COMMITTEE-1",
          "Committee Type": "Public Financing Committee",
          "Transaction Amount": "$300.00",
        }),
        contribution({
          "Filing Entity Id": "OTHER",
          "Transaction Amount": "$900.00",
        }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(300);
    expect(result.summary.directContributionTotal).toBe(300);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateMarylandDirectContributions({
      committeeId: "16018290",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Amount": "$0.10" }),
        contribution({ "Transaction Amount": "$0.20" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(0.3);
    expect(result.summary.directContributionTotal).toBe(0.3);
    expect(result.directBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "contribution_size",
        categoryName: "$1-$99",
        amount: 0.3,
        contributorCount: 1,
      }),
    ]);
  });

  it("filters to the two-year election cycle ending in the election year", () => {
    expect(marylandElectionCycleStartYear(2026)).toBe(2025);

    const result = aggregateMarylandDirectContributions({
      committeeId: "16018290",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Date": "12/31/2024", "Transaction Amount": "$100.00" }),
        contribution({ "Transaction Date": "1/1/2025", "Transaction Amount": "$200.00" }),
        contribution({ "Transaction Date": "2026-11-01", "Transaction Amount": "$300.00" }),
        contribution({ "Transaction Date": "1/1/2027", "Transaction Amount": "$400.00" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.summary.directContributionTotal).toBe(500);
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(2);
  });

  it("classifies total receipts separately from direct donor support receipts", () => {
    const directSupportRows = [
      contribution({ "Transaction Type": "Contribution" }),
      contribution({ "Transaction Type": "Monetary Contribution" }),
      contribution({ "Transaction Type": "In-Kind Contribution" }),
      contribution({ "Transaction Type": "Coordinated In-Kind" }),
    ];
    for (const row of directSupportRows) {
      expect(isMarylandTotalReceipt({ row, electionYear: 2026 })).toBe(true);
      expect(isMarylandDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(true);
    }

    const nonDirectRows = [
      contribution({ "Transaction Type": "Loan" }),
      contribution({ "Transaction Type": "Other Receipt" }),
      contribution({ "Transaction Type": "Refund" }),
    ];
    for (const row of nonDirectRows) {
      expect(isMarylandTotalReceipt({ row, electionYear: 2026 })).toBe(true);
      expect(isMarylandDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(false);
    }
  });

  it("keeps total receipts broad while direct donor support excludes loans and other receipts", () => {
    const result = aggregateMarylandDirectContributions({
      committeeId: "16018290",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Type": "Contribution", "Transaction Amount": "$1,000.00" }),
        contribution({ "Transaction Type": "Loan", "Transaction Amount": "$5,000.00" }),
        contribution({ "Transaction Type": "Other Receipt", "Transaction Amount": "$250.00" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(6250);
    expect(result.summary.directContributionTotal).toBe(1000);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(2);
    expect(result.directBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "contribution_size",
        categoryName: "$1,000-$4,999",
        amount: 1000,
      }),
    ]);
  });

  it("skips malformed, zero, negative, missing-date, non-candidate, and wrong-committee rows", () => {
    const result = aggregateMarylandDirectContributions({
      committeeId: "16018290",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Amount": "$0.00" }),
        contribution({ "Transaction Amount": "($10.00)" }),
        contribution({ "Transaction Amount": "not money" }),
        contribution({ "Transaction Date": "", "Transaction Amount": "$100.00" }),
        contribution({ "Committee Type": "Political Action Committee (PAC)", "Transaction Amount": "$500.00" }),
        contribution({ "Filing Entity Id": "999999", "Transaction Amount": "$600.00" }),
        contribution({ "Transaction Amount": "$250.00" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(250);
    expect(result.summary.directContributionTotal).toBe(250);
    expect(result.matchedContributionRowCount).toBe(6);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(5);
  });

  it("rejects invalid inputs before aggregating", () => {
    expect(() =>
      aggregateMarylandDirectContributions({
        committeeId: " ",
        electionYear: 2026,
        contributionRows: [],
      })
    ).toThrow("Maryland committee id is required");

    expect(() =>
      aggregateMarylandDirectContributions({
        committeeId: "16018290",
        electionYear: 1999,
        contributionRows: [],
      })
    ).toThrow("Invalid Maryland direct contribution aggregation election year");

    expect(() =>
      aggregateMarylandDirectContributions({
        committeeId: "16018290",
        electionYear: 2026,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Maryland direct contribution aggregation maxBreakdownsPerCategory");
  });
});
