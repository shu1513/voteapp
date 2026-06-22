import { describe, expect, it } from "vitest";

import {
  aggregateOklahomaDirectContributions,
  isOklahomaDirectDonorSupportReceipt,
  isOklahomaTotalReceipt,
  oklahomaElectionCycleStartYear,
} from "../../../src/pipeline/oklahomaFinance/oklahomaDirectContributionAggregator.js";
import type { OklahomaGuardianContributionRow } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianContributionReader.js";

function contribution(overrides: Partial<OklahomaGuardianContributionRow> = {}): OklahomaGuardianContributionRow {
  return {
    "Receipt ID": "1001",
    "Org ID": "11954",
    "Receipt Type": "Contribution",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "250.00",
    Description: "",
    "Receipt Source Type": "Individual",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Oklahoma City",
    State: "OK",
    Zip: "73102",
    "Filed Date": "02/01/2026",
    "Committee Type": "Candidate Committee",
    "Committee Name": "Dishman for Senate",
    "Candidate Name": "C. BRENT DISHMAN",
    Amended: "",
    Employer: "Acme",
    Occupation: "Engineer",
    ...overrides,
  };
}

describe("oklahomaDirectContributionAggregator", () => {
  it("aggregates direct donor support by occupation and contribution size", () => {
    const sourceUrl = "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip";
    const result = aggregateOklahomaDirectContributions({
      committeeId: "11954",
      electionYear: 2026,
      sourceUrl,
      contributionRows: [
        contribution({ "Receipt Amount": "100.00", Occupation: "Attorney" }),
        contribution({
          "Receipt ID": "1002",
          "Receipt Amount": "$250.00",
          Occupation: "Attorney",
          "Last Name": "Roe",
        }),
        contribution({
          "Receipt ID": "1003",
          "Receipt Amount": "5,000.00",
          Occupation: "Teacher",
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

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateOklahomaDirectContributions({
      committeeId: "11954",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Receipt ID": "R1", "Receipt Amount": "100", Occupation: "Attorney" }),
        contribution({ "Receipt ID": "R2", "Receipt Amount": "200", Occupation: "Attorney" }),
        contribution({
          "Receipt ID": "R3",
          "Receipt Amount": "300",
          Occupation: "Attorney",
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

  it("matches committee IDs case-insensitively and does not emit employer or source-type breakdowns", () => {
    const result = aggregateOklahomaDirectContributions({
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
        (row) => row.categoryType === "occupation" || row.categoryType === "contribution_size"
      )
    ).toBe(true);
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateOklahomaDirectContributions({
      committeeId: "11954",
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
        expect.objectContaining({ categoryType: "contribution_size", categoryName: "$1-$99", amount: 0.3 }),
      ])
    );
  });

  it("filters to the two-year election cycle ending in the election year", () => {
    expect(oklahomaElectionCycleStartYear(2026)).toBe(2025);

    const result = aggregateOklahomaDirectContributions({
      committeeId: "11954",
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
      contribution({ "Receipt Type": "Contribution" }),
      contribution({ "Receipt Type": "In-Kind Contribution" }),
      contribution({ "Receipt Type": "Monetary Contribution" }),
    ];
    for (const row of directSupportRows) {
      expect(isOklahomaTotalReceipt({ row, electionYear: 2026 })).toBe(true);
      expect(isOklahomaDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(true);
    }

    const nonDirectRows = [
      contribution({ "Receipt Type": "Loan" }),
      contribution({ "Receipt Type": "Other Funds Received" }),
      contribution({ "Receipt Type": "Interest Income" }),
      contribution({ "Receipt Type": "Refund" }),
    ];
    for (const row of nonDirectRows) {
      expect(isOklahomaTotalReceipt({ row, electionYear: 2026 })).toBe(true);
      expect(isOklahomaDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(false);
    }
  });

  it("keeps total receipts broad while direct donor support excludes loans and other receipts", () => {
    const result = aggregateOklahomaDirectContributions({
      committeeId: "11954",
      electionYear: 2026,
      contributionRows: [
        contribution({
          "Receipt Type": "Contribution",
          "Receipt Amount": "1000",
          Occupation: "Attorney",
        }),
        contribution({
          "Receipt Type": "Loan",
          "Receipt Amount": "5000",
          Occupation: "Candidate",
        }),
        contribution({
          "Receipt Type": "Other Funds Received",
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
        categoryType: "occupation",
        categoryName: "Attorney",
        amount: 1000,
      }),
      expect.objectContaining({
        categoryType: "contribution_size",
        categoryName: "$1,000-$4,999",
        amount: 1000,
      }),
    ]);
  });

  it("skips malformed, zero, negative, missing-date, non-candidate, and wrong-committee rows", () => {
    const result = aggregateOklahomaDirectContributions({
      committeeId: "11954",
      electionYear: 2026,
      contributionRows: [
        contribution({ "Receipt Amount": "0" }),
        contribution({ "Receipt Amount": "-10" }),
        contribution({ "Receipt Amount": "not money" }),
        contribution({ "Receipt Date": "", "Receipt Amount": "100" }),
        contribution({ "Committee Type": "Political Action Committee", "Receipt Amount": "500" }),
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
    const result = aggregateOklahomaDirectContributions({
      committeeId: "11954",
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
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("validates required aggregation inputs", () => {
    expect(() =>
      aggregateOklahomaDirectContributions({
        committeeId: " ",
        electionYear: 2026,
        contributionRows: [],
      })
    ).toThrow("Oklahoma committee id is required");

    expect(() =>
      aggregateOklahomaDirectContributions({
        committeeId: "11954",
        electionYear: 2013,
        contributionRows: [],
      })
    ).toThrow("Invalid Oklahoma direct contribution aggregation election year");

    expect(() =>
      aggregateOklahomaDirectContributions({
        committeeId: "11954",
        electionYear: 2026,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Oklahoma direct contribution aggregation maxBreakdownsPerCategory");
  });
});
