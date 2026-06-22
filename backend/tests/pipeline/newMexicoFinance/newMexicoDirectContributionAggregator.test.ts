import { describe, expect, it } from "vitest";

import {
  aggregateNewMexicoDirectContributions,
  isNewMexicoDirectDonorSupportReceipt,
  isNewMexicoTotalReceipt,
  mapNewMexicoContributorSourceType,
  newMexicoElectionCycleStartYear,
} from "../../../src/pipeline/newMexicoFinance/newMexicoDirectContributionAggregator.js";
import type { NewMexicoCfisContributionRow } from "../../../src/pipeline/newMexicoFinance/newMexicoCfisArtifactReader.js";

function contribution(overrides: Partial<NewMexicoCfisContributionRow> = {}): NewMexicoCfisContributionRow {
  return {
    OrgID: "1001",
    "Transaction Amount": "250.00",
    "Transaction Date": "01/10/2026",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Prefix: "",
    Suffix: "",
    "Contributor Address Line 1": "",
    "Contributor Address Line 2": "",
    "Contributor City": "Santa Fe",
    "Contributor State": "NM",
    "Contributor Zip Code": "87501",
    Description: "",
    "Check Number": "",
    "Transaction ID": "T1",
    "Filed Date": "02/01/2026",
    Election: "2026 General",
    "Report Name": "First Report",
    "Start of Period": "01/01/2026",
    "End of Period": "01/31/2026",
    "Contributor Code": "Individual",
    "Contribution Type": "Contributions - Monetary",
    "Report Entity Type": "Candidate",
    "Committee Name": "Doe, Jane for Governor",
    "Candidate Last Name": "Doe",
    "Candidate First Name": "Jane",
    "Candidate Middle Name": "",
    "Candidate Prefix": "",
    "Candidate Suffix": "",
    Amended: "",
    "Contributor Employer": "Acme",
    "Contributor Occupation": "Engineer",
    "Occupation Comment": "",
    "Employment Information Requested": "",
    ...overrides,
  };
}

describe("newMexicoDirectContributionAggregator", () => {
  it("aggregates direct receipts by occupation, source type, and contribution size", () => {
    const sourceUrl =
      "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=CON";
    const result = aggregateNewMexicoDirectContributions({
      committeeId: "1001",
      electionYear: 2026,
      sourceUrl,
      contributionRows: [
        contribution({ "Transaction Amount": "100.00", "Contributor Occupation": "Attorney" }),
        contribution({
          "Transaction Amount": "$250.00",
          "Contributor Occupation": "Attorney",
          "Last Name": "Roe",
        }),
        contribution({
          "Transaction Amount": "5,000.00",
          "Contributor Occupation": "Teacher",
          "Last Name": "Smith",
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
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 350,
          contributorCount: 2,
          sourceUrl,
        },
        {
          categoryType: "contributor_source_type",
          categoryName: "individuals",
          amount: 5350,
          contributorCount: 3,
          sourceUrl,
        },
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
  });

  it("accepts CFIS four-decimal currency amounts", () => {
    const result = aggregateNewMexicoDirectContributions({
      committeeId: "1001",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Amount": "100.0000", "Contributor Occupation": "Attorney" }),
        contribution({
          "Transaction ID": "T2",
          "Transaction Amount": "4.8000",
          "Contributor Occupation": "Teacher",
        }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        totalReceipts: 104.8,
        directContributionTotal: 104.8,
      },
      matchedContributionRowCount: 2,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 0,
    });
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 100 }),
        expect.objectContaining({ categoryType: "occupation", categoryName: "Teacher", amount: 4.8 }),
      ])
    );
  });

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateNewMexicoDirectContributions({
      committeeId: "1001",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction ID": "T1", "Transaction Amount": "100", "Contributor Occupation": "Attorney" }),
        contribution({ "Transaction ID": "T2", "Transaction Amount": "200", "Contributor Occupation": "Attorney" }),
        contribution({
          "Transaction ID": "T3",
          "Transaction Amount": "300",
          "Contributor Occupation": "Attorney",
          "Last Name": "Roe",
        }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Attorney", amount: 600, contributorCount: 2 }),
    ]);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          categoryType: "contributor_source_type",
          categoryName: "individuals",
          amount: 600,
          contributorCount: 2,
        }),
      ])
    );
  });

  it("does not emit employer breakdowns", () => {
    const result = aggregateNewMexicoDirectContributions({
      committeeId: " nm-1 ",
      electionYear: 2026,
      contributionRows: [
        contribution({
          OrgID: "NM-1",
          "Transaction Amount": "300",
          "Contributor Occupation": "Attorney",
          "Contributor Employer": "Law Firm",
        }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(300);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 300 }),
      ])
    );
    expect(
      result.directBreakdowns.every(
        (row) =>
          row.categoryType === "occupation" ||
          row.categoryType === "industry" ||
          row.categoryType === "contribution_size" ||
          row.categoryType === "contributor_source_type"
      )
    ).toBe(true);
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateNewMexicoDirectContributions({
      committeeId: "1001",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Amount": "0.10", "Contributor Occupation": "Engineer" }),
        contribution({ "Transaction Amount": "0.20", "Contributor Occupation": "Engineer" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(0.3);
    expect(result.summary.directContributionTotal).toBe(0.3);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Engineer", amount: 0.3 }),
        expect.objectContaining({ categoryType: "contributor_source_type", categoryName: "individuals", amount: 0.3 }),
        expect.objectContaining({ categoryType: "contribution_size", categoryName: "$1-$99", amount: 0.3 }),
      ])
    );
  });

  it("filters to the two-year election cycle ending in the election year", () => {
    expect(newMexicoElectionCycleStartYear(2026)).toBe(2025);

    const result = aggregateNewMexicoDirectContributions({
      committeeId: "1001",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Date": "12/31/2024", "Transaction Amount": "100" }),
        contribution({ "Transaction Date": "1/1/2025", "Transaction Amount": "200" }),
        contribution({ "Transaction Date": "2026-11-01", "Transaction Amount": "300" }),
        contribution({ "Transaction Date": "1/1/2027", "Transaction Amount": "400" }),
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
      contribution({ "Contribution Type": "Contributions - Monetary" }),
      contribution({ "Contribution Type": "Contributions - In-Kind" }),
      contribution({ "Contribution Type": "Monetary Contribution" }),
      contribution({ "Contribution Type": "In-Kind Contribution" }),
    ];
    for (const row of directSupportRows) {
      expect(isNewMexicoTotalReceipt({ row, electionYear: 2026 })).toBe(true);
      expect(isNewMexicoDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(true);
    }

    const nonDirectRows = [
      contribution({ "Contribution Type": "Loan" }),
      contribution({ "Contribution Type": "Interest Income" }),
      contribution({ "Contribution Type": "Other Receipts" }),
      contribution({ "Contribution Type": "Refund" }),
    ];
    for (const row of nonDirectRows) {
      expect(isNewMexicoTotalReceipt({ row, electionYear: 2026 })).toBe(true);
      expect(isNewMexicoDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(false);
    }
  });

  it("keeps total receipts broad while direct donor support excludes loans and other receipts", () => {
    const result = aggregateNewMexicoDirectContributions({
      committeeId: "1001",
      electionYear: 2026,
      contributionRows: [
        contribution({
          "Contribution Type": "Contributions - Monetary",
          "Contributor Code": "Business",
          "Transaction Amount": "1000",
          "Contributor Occupation": "",
        }),
        contribution({
          "Contribution Type": "Loan",
          "Contributor Code": "Self",
          "Transaction Amount": "5000",
          "Contributor Occupation": "",
        }),
        contribution({
          "Contribution Type": "Other Receipts",
          "Contributor Code": "",
          "Transaction Amount": "250",
          "Contributor Occupation": "",
        }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(6250);
    expect(result.summary.directContributionTotal).toBe(1000);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(2);
    expect(result.directBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "contributor_source_type",
        categoryName: "business_nonprofit_entities",
        amount: 1000,
      }),
      expect.objectContaining({
        categoryType: "contribution_size",
        categoryName: "$1,000-$4,999",
        amount: 1000,
      }),
    ]);
  });

  it("maps contributor source types conservatively", () => {
    expect(mapNewMexicoContributorSourceType("Individual")).toBe("individuals");
    expect(mapNewMexicoContributorSourceType("Business")).toBe("business_nonprofit_entities");
    expect(mapNewMexicoContributorSourceType("Political Committee")).toBe("pac_independent");
    expect(mapNewMexicoContributorSourceType("Political Party")).toBe("party_committee");
    expect(mapNewMexicoContributorSourceType("Political Party Committee")).toBe("party_committee");
    expect(mapNewMexicoContributorSourceType("Self Candidate")).toBe("candidate_self");
    expect(mapNewMexicoContributorSourceType("Unknown")).toBe("other");
  });

  it("emits deterministic direct industry breakdowns only above the state threshold", () => {
    const result = aggregateNewMexicoDirectContributions({
      committeeId: "1001",
      electionYear: 2026,
      contributionRows: [
        contribution({
          "Contributor Code": "Business",
          "Last Name": "Prairie Farm LLC",
          "First Name": "",
          "Transaction Amount": "20000",
          "Contributor Occupation": "",
        }),
        contribution({
          "Contributor Code": "Business",
          "Last Name": "Nebraska Farm Co",
          "First Name": "",
          "Transaction Amount": "5000",
          "Contributor Occupation": "",
        }),
        contribution({
          "Contributor Code": "Business",
          "Last Name": "Santa Fe Construction Company",
          "First Name": "",
          "Transaction Amount": "24999",
          "Contributor Occupation": "",
        }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "industry")).toEqual([
      expect.objectContaining({ categoryName: "agriculture_and_food", amount: 25000, contributorCount: 2 }),
    ]);
  });

  it("skips malformed, zero, negative, missing-date, non-candidate, and wrong-committee rows", () => {
    const result = aggregateNewMexicoDirectContributions({
      committeeId: "1001",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Transaction Amount": "0" }),
        contribution({ "Transaction Amount": "-10" }),
        contribution({ "Transaction Amount": "not money" }),
        contribution({ "Transaction Date": "" }),
        contribution({ "Report Entity Type": "PAC - Independent Expenditure", "Transaction Amount": "900" }),
        contribution({ OrgID: "OTHER", "Transaction Amount": "1200" }),
      ],
    });

    expect(result.summary).toEqual({
      totalReceipts: 0,
      directContributionTotal: 0,
      sourceUrl: null,
    });
    expect(result.matchedContributionRowCount).toBe(5);
    expect(result.includedContributionRowCount).toBe(0);
    expect(result.skippedContributionRowCount).toBe(5);
    expect(result.directBreakdowns).toEqual([]);
  });

  it("rejects invalid inputs", () => {
    expect(() =>
      aggregateNewMexicoDirectContributions({
        committeeId: "   ",
        electionYear: 2026,
        contributionRows: [],
      })
    ).toThrow("New Mexico committee id is required");
    expect(() =>
      aggregateNewMexicoDirectContributions({
        committeeId: "1001",
        electionYear: 2019,
        contributionRows: [],
      })
    ).toThrow("Invalid New Mexico direct contribution aggregation election year");
    expect(() =>
      aggregateNewMexicoDirectContributions({
        committeeId: "1001",
        electionYear: 2026,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid New Mexico direct contribution aggregation maxBreakdownsPerCategory");
  });
});
