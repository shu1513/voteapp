import { describe, expect, it } from "vitest";

import {
  aggregateNebraskaDirectContributions,
  isNebraskaDirectDonorSupportReceipt,
  isNebraskaTotalReceipt,
  mapNebraskaContributorSourceType,
  nebraskaElectionCycleStartYear,
} from "../../../src/pipeline/nebraskaFinance/nebraskaDirectContributionAggregator.js";
import type { NebraskaNadcContributionRow } from "../../../src/pipeline/nebraskaFinance/nebraskaNadcArtifactReader.js";

function contribution(overrides: Partial<NebraskaNadcContributionRow> = {}): NebraskaNadcContributionRow {
  return {
    "Receipt ID": "110654",
    "Org ID": "7569",
    "Filer Type": "Candidate Committee",
    "Filer Name": "VOTE VEST",
    "Candidate Name": "RICK VEST",
    "Receipt Transaction/Contribution Type": "Contribution",
    "Other Funds Type": "",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "250.00",
    Description: "",
    "Contributor or Transaction Source Type": "Individual",
    "Contributor or Source Name (Individual Last Name)": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Lincoln",
    State: "NE",
    Zip: "68508",
    "Filed Date": "02/01/2026",
    Amended: "",
    Employer: "Acme",
    Occupation: "Engineer",
    ...overrides,
  };
}

describe("nebraskaDirectContributionAggregator", () => {
  it("aggregates direct receipts by occupation and contribution size", () => {
    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      contributionRows: [
        contribution({ "Receipt Amount": "100.00", Occupation: "Attorney" }),
        contribution({
          "Receipt Amount": "$250.00",
          Occupation: "Attorney",
          "Contributor or Source Name (Individual Last Name)": "Roe",
        }),
        contribution({
          "Receipt Amount": "5,000.00",
          Occupation: "Teacher",
          "Contributor or Source Name (Individual Last Name)": "Smith",
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
        sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
        {
          categoryType: "occupation",
          categoryName: "Attorney",
          amount: 350,
          contributorCount: 2,
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
        {
          categoryType: "contributor_source_type",
          categoryName: "individuals",
          amount: 5350,
          contributorCount: 3,
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$5,000+",
          amount: 5000,
          contributorCount: 1,
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$250-$499",
          amount: 250,
          contributorCount: 1,
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 100,
          contributorCount: 1,
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        },
      ],
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
    });
  });

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Receipt ID": "R1", "Receipt Amount": "100", Occupation: "Attorney" }),
        contribution({ "Receipt ID": "R2", "Receipt Amount": "200", Occupation: "Attorney" }),
        contribution({
          "Receipt ID": "R3",
          "Receipt Amount": "300",
          Occupation: "Attorney",
          "Contributor or Source Name (Individual Last Name)": "Roe",
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

  it("matches committee IDs case-insensitively and does not emit employer breakdowns", () => {
    const result = aggregateNebraskaDirectContributions({
      committeeId: " abc123 ",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Org ID": "ABC123", "Receipt Amount": "300", Occupation: "Attorney", Employer: "Law Firm" }),
        contribution({ "Org ID": "OTHER", "Receipt Amount": "900", Occupation: "Doctor", Employer: "Hospital" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(300);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
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
    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Receipt Amount": "0.10", Occupation: "Engineer" }),
        contribution({ "Receipt Amount": "0.20", Occupation: "Engineer" }),
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
    expect(nebraskaElectionCycleStartYear(2026)).toBe(2025);

    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Receipt Date": "12/31/2024", "Receipt Amount": "100" }),
        contribution({ "Receipt Date": "1/1/2025", "Receipt Amount": "200" }),
        contribution({ "Receipt Date": "2026-11-01", "Receipt Amount": "300" }),
        contribution({ "Receipt Date": "1/1/2027", "Receipt Amount": "400" }),
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
      contribution({ "Receipt Transaction/Contribution Type": "Monetary" }),
      contribution({ "Receipt Transaction/Contribution Type": "In-Kind Contribution" }),
      contribution({ "Receipt Transaction/Contribution Type": "Earmarked Monetary" }),
      contribution({ "Receipt Transaction/Contribution Type": "Earmarked In-Kind" }),
      contribution({ "Receipt Transaction/Contribution Type": "Pledge Payment Received" }),
      contribution({ "Receipt Transaction/Contribution Type": "Monetary Contribution" }),
    ];
    for (const row of directSupportRows) {
      expect(isNebraskaTotalReceipt({ row, electionYear: 2026 })).toBe(true);
      expect(isNebraskaDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(true);
    }

    const nonDirectRows = [
      contribution({ "Receipt Transaction/Contribution Type": "Loan" }),
      contribution({
        "Receipt Transaction/Contribution Type": "Other Funds Received (Miscellaneous Receipts)",
        "Other Funds Type": "Interest Income from Campaign Checking Account",
      }),
      contribution({ "Receipt Transaction/Contribution Type": "Debt Forgiveness" }),
      contribution({ "Receipt Transaction/Contribution Type": "Loan Forgiveness Received" }),
      contribution({ "Receipt Transaction/Contribution Type": "Pledge" }),
    ];
    for (const row of nonDirectRows) {
      expect(isNebraskaTotalReceipt({ row, electionYear: 2026 })).toBe(true);
      expect(isNebraskaDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(false);
    }
  });

  it("rejects invalid receipts before direct donor support classification", () => {
    const invalidRows = [
      contribution({ "Receipt Amount": "0" }),
      contribution({ "Receipt Amount": "-10" }),
      contribution({ "Receipt Amount": "not money" }),
      contribution({ "Receipt Date": "12/31/2024" }),
      contribution({ "Filer Type": "PAC-Independent" }),
    ];
    for (const row of invalidRows) {
      expect(isNebraskaTotalReceipt({ row, electionYear: 2026 })).toBe(false);
      expect(isNebraskaDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(false);
    }
  });

  it("maps Nebraska contributor source labels into stable source-type buckets", () => {
    expect(mapNebraskaContributorSourceType("Individual")).toBe("individuals");
    expect(mapNebraskaContributorSourceType("Business (For-Profit and Non-Profit entities)")).toBe(
      "business_nonprofit_entities"
    );
    expect(mapNebraskaContributorSourceType("PAC-Independent")).toBe("pac_independent");
    expect(mapNebraskaContributorSourceType("Political Party Committee")).toBe("party_committee");
    expect(mapNebraskaContributorSourceType("Self (Candidate)")).toBe("candidate_self");
  });

  it("maps unclassified contributor source labels to other without guessing", () => {
    expect(mapNebraskaContributorSourceType("PAC-Separate Segregated Political Fund")).toBe("other");
    expect(mapNebraskaContributorSourceType("Federal PAC")).toBe("other");
    expect(mapNebraskaContributorSourceType("Candidate Committee")).toBe("other");
    expect(mapNebraskaContributorSourceType("")).toBe("other");
  });

  it("emits direct business donor industries only once the state-level threshold is met", () => {
    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      contributionRows: [
        contribution({
          "Contributor or Transaction Source Type": "Business (For-Profit and Non-Profit entities)",
          "Contributor or Source Name (Individual Last Name)": "Prairie Farm LLC",
          "Receipt Amount": "20000",
          Occupation: "",
        }),
        contribution({
          "Contributor or Transaction Source Type": "Business (For-Profit and Non-Profit entities)",
          "Contributor or Source Name (Individual Last Name)": "Nebraska Farm Co",
          "Receipt Amount": "5000",
          Occupation: "",
        }),
        contribution({
          "Contributor or Transaction Source Type": "Business (For-Profit and Non-Profit entities)",
          "Contributor or Source Name (Individual Last Name)": "Lincoln Construction Company",
          "Receipt Amount": "24999",
          Occupation: "",
        }),
        contribution({
          "Contributor or Transaction Source Type": "Individual",
          "Contributor or Source Name (Individual Last Name)": "Doe",
          "Receipt Amount": "500",
          Occupation: "Engineer",
        }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "industry")).toEqual([
      expect.objectContaining({ categoryName: "agriculture_and_food", amount: 25000, contributorCount: 2 }),
    ]);
  });

  it("keeps total receipts broad while direct donor support excludes loans and other funds", () => {
    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      contributionRows: [
        contribution({
          "Receipt Transaction/Contribution Type": "Monetary",
          "Contributor or Transaction Source Type": "Business (For-Profit and Non-Profit entities)",
          "Receipt Amount": "1000",
          Occupation: "",
        }),
        contribution({
          "Receipt Transaction/Contribution Type": "Loan",
          "Contributor or Transaction Source Type": "Self (Candidate)",
          "Receipt Amount": "5000",
          Occupation: "",
        }),
        contribution({
          "Receipt Transaction/Contribution Type": "Other Funds Received (Miscellaneous Receipts)",
          "Other Funds Type": "Interest Income from Campaign Checking Account",
          "Contributor or Transaction Source Type": "",
          "Receipt Amount": "250",
          Occupation: "",
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

  it("handles a realistic mixed Nebraska receipt snapshot without counting loans or other funds as donor support", () => {
    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      contributionRows: [
        contribution({
          "Receipt ID": "R1",
          "Receipt Transaction/Contribution Type": "Monetary",
          "Contributor or Transaction Source Type": "Business (For-Profit and Non-Profit entities)",
          "Receipt Amount": "25000",
          Occupation: "",
        }),
        contribution({
          "Receipt ID": "R2",
          "Receipt Transaction/Contribution Type": "Monetary",
          "Contributor or Transaction Source Type": "PAC-Independent",
          "Receipt Amount": "5000",
          Occupation: "",
        }),
        contribution({
          "Receipt ID": "R3",
          "Receipt Transaction/Contribution Type": "Monetary",
          "Contributor or Transaction Source Type": "Candidate Committee",
          "Receipt Amount": "10000",
          Occupation: "",
        }),
        contribution({
          "Receipt ID": "R4",
          "Receipt Transaction/Contribution Type": "Monetary",
          "Contributor or Transaction Source Type": "Individual",
          "Receipt Amount": "500",
          Occupation: "PROPRIETOR",
        }),
        contribution({
          "Receipt ID": "R5",
          "Receipt Transaction/Contribution Type": "Loan",
          "Contributor or Transaction Source Type": "Self (Candidate)",
          "Receipt Amount": "1000",
          Occupation: "",
        }),
        contribution({
          "Receipt ID": "R6",
          "Receipt Transaction/Contribution Type": "Other Funds Received (Miscellaneous Receipts)",
          "Other Funds Type": "CD Interest",
          "Contributor or Transaction Source Type": "",
          "Receipt Amount": "491.74",
          Occupation: "",
        }),
      ],
    });

    expect(result.summary).toEqual({
      totalReceipts: 41991.74,
      directContributionTotal: 40500,
      sourceUrl: null,
    });
    expect(result.matchedContributionRowCount).toBe(6);
    expect(result.includedContributionRowCount).toBe(4);
    expect(result.skippedContributionRowCount).toBe(2);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "PROPRIETOR", amount: 500, contributorCount: 1 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contributor_source_type")).toEqual([
      expect.objectContaining({ categoryName: "business_nonprofit_entities", amount: 25000, contributorCount: 1 }),
      expect.objectContaining({ categoryName: "other", amount: 10000, contributorCount: 1 }),
      expect.objectContaining({ categoryName: "pac_independent", amount: 5000, contributorCount: 1 }),
      expect.objectContaining({ categoryName: "individuals", amount: 500, contributorCount: 1 }),
    ]);
    expect(result.directBreakdowns).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ categoryName: "candidate_self" })])
    );
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toEqual([
      expect.objectContaining({ categoryName: "$5,000+", amount: 40000, contributorCount: 3 }),
      expect.objectContaining({ categoryName: "$500-$999", amount: 500, contributorCount: 1 }),
    ]);
  });

  it("skips malformed, zero, negative, missing-date, non-candidate, and wrong-committee rows", () => {
    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Receipt Amount": "0" }),
        contribution({ "Receipt Amount": "-10" }),
        contribution({ "Receipt Amount": "not money" }),
        contribution({ "Receipt Date": "", "Receipt Amount": "100" }),
        contribution({ "Filer Type": "PAC-Independent", "Receipt Amount": "500" }),
        contribution({ "Org ID": "999999", "Receipt Amount": "600" }),
        contribution({ "Receipt Amount": "250" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(250);
    expect(result.summary.directContributionTotal).toBe(250);
    expect(result.matchedContributionRowCount).toBe(6);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(5);
  });

  it("limits occupation breakdowns without dropping contribution-size buckets", () => {
    const result = aggregateNebraskaDirectContributions({
      committeeId: "7569",
      electionYear: 2026,
      maxBreakdownsPerCategory: 1,
      contributionRows: [
        contribution({ Occupation: "Engineer", "Receipt Amount": "100" }),
        contribution({ Occupation: "Teacher", "Receipt Amount": "300" }),
        contribution({ Occupation: "Doctor", "Receipt Amount": "600" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Doctor", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contributor_source_type")).toEqual([
      expect.objectContaining({ categoryName: "individuals", amount: 1000 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("validates required aggregation inputs", () => {
    expect(() =>
      aggregateNebraskaDirectContributions({
        committeeId: " ",
        electionYear: 2026,
        contributionRows: [],
      })
    ).toThrow("Nebraska committee id is required");

    expect(() =>
      aggregateNebraskaDirectContributions({
        committeeId: "7569",
        electionYear: 2020,
        contributionRows: [],
      })
    ).toThrow("Invalid Nebraska direct contribution aggregation election year");

    expect(() =>
      aggregateNebraskaDirectContributions({
        committeeId: "7569",
        electionYear: 2026,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Nebraska direct contribution aggregation maxBreakdownsPerCategory");
  });
});
