import { describe, expect, it } from "vitest";

import {
  aggregateTexasDirectContributions,
  isTexasDirectDonorSupportReceipt,
  isTexasTotalReceipt,
  texasElectionCycleStartYear,
} from "../../../src/pipeline/texasFinance/texasDirectContributionAggregator.js";
import type { TexasTecContributionRow } from "../../../src/pipeline/texasFinance/texasTecCsvDatabaseReader.js";

function contribution(overrides: Partial<TexasTecContributionRow> = {}): TexasTecContributionRow {
  return {
    recordType: "CONTRIB",
    formTypeCd: "COH",
    schedFormTypeCd: "A1",
    reportInfoIdent: "9001",
    receivedDt: "20261001",
    infoOnlyFlag: "",
    filerIdent: "00012345",
    filerTypeCd: "COH",
    filerName: "ABBOTT, GREG",
    contributionInfoId: "1001",
    contributionDt: "20260915",
    contributionAmount: "250.00",
    contributionDescr: "",
    contributorPersentTypeCd: "INDIVIDUAL",
    contributorNameOrganization: "",
    contributorNameLast: "DOE",
    contributorNameFirst: "JANE",
    contributorStreetStateCd: "TX",
    contributorEmployer: "ACME",
    contributorOccupation: "Engineer",
    contributorJobTitle: "Engineer",
    ...overrides,
  };
}

describe("texasDirectContributionAggregator", () => {
  it("aggregates direct donor support by occupation and contribution size", () => {
    const sourceUrl = "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip";
    const result = aggregateTexasDirectContributions({
      committeeId: "00012345",
      electionYear: 2026,
      sourceUrl,
      contributionRows: [
        contribution({ contributionAmount: "100.00", contributorOccupation: "Attorney" }),
        contribution({
          contributionInfoId: "1002",
          contributionAmount: "$250.00",
          contributorOccupation: "Attorney",
          contributorNameLast: "ROE",
        }),
        contribution({
          contributionInfoId: "1003",
          contributionAmount: "5,000.00",
          contributorOccupation: "Teacher",
          contributorNameLast: "SMITH",
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
    const result = aggregateTexasDirectContributions({
      committeeId: "00012345",
      electionYear: 2026,
      contributionRows: [
        contribution({ contributionInfoId: "R1", contributionAmount: "100", contributorOccupation: "Attorney" }),
        contribution({ contributionInfoId: "R2", contributionAmount: "200", contributorOccupation: "Attorney" }),
        contribution({
          contributionInfoId: "R3",
          contributionAmount: "300",
          contributorOccupation: "Attorney",
          contributorNameLast: "ROE",
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

  it("matches committee IDs case-insensitively and does not emit employer breakdowns", () => {
    const result = aggregateTexasDirectContributions({
      committeeId: " abc123 ",
      electionYear: 2026,
      contributionRows: [
        contribution({
          filerIdent: "ABC123",
          contributionAmount: "300",
          contributorOccupation: "Attorney",
          contributorEmployer: "Law Firm",
        }),
        contribution({
          filerIdent: "OTHER",
          contributionAmount: "900",
          contributorOccupation: "Doctor",
          contributorEmployer: "Hospital",
        }),
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
      result.directBreakdowns.every((row) => row.categoryType === "occupation" || row.categoryType === "contribution_size")
    ).toBe(true);
  });

  it("aggregates direct receipts from additional safe receipt committee IDs", () => {
    const result = aggregateTexasDirectContributions({
      committeeId: "00012345",
      committeeIds: ["00051153"],
      electionYear: 2026,
      contributionRows: [
        contribution({
          filerIdent: "00012345",
          contributionAmount: "100",
          contributorOccupation: "Attorney",
        }),
        contribution({
          filerIdent: "00051153",
          filerTypeCd: "SPAC",
          contributionAmount: "900",
          contributorOccupation: "Engineer",
          contributorNameLast: "ROE",
        }),
        contribution({
          filerIdent: "00099991",
          contributionAmount: "5000",
          contributorOccupation: "Doctor",
        }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(1000);
    expect(result.summary.directContributionTotal).toBe(1000);
    expect(result.matchedContributionRowCount).toBe(2);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Engineer", amount: 900 }),
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 100 }),
      ])
    );
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateTexasDirectContributions({
      committeeId: "00012345",
      electionYear: 2026,
      contributionRows: [
        contribution({ contributionAmount: "0.10", contributorOccupation: "Engineer" }),
        contribution({ contributionAmount: "0.20", contributorOccupation: "Engineer" }),
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
    expect(texasElectionCycleStartYear(2026)).toBe(2025);

    const result = aggregateTexasDirectContributions({
      committeeId: "00012345",
      electionYear: 2026,
      contributionRows: [
        contribution({ contributionDt: "20241231", contributionAmount: "100" }),
        contribution({ contributionDt: "20250101", contributionAmount: "200" }),
        contribution({ contributionDt: "2026-11-01", contributionAmount: "300" }),
        contribution({ contributionDt: "1/1/2027", contributionAmount: "400" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.summary.directContributionTotal).toBe(500);
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(2);
  });

  it("classifies Texas candidate-officeholder contribution rows as total and direct receipts", () => {
    const row = contribution({ filerTypeCd: "COH", contributionAmount: "250" });
    expect(isTexasTotalReceipt({ row, electionYear: 2026 })).toBe(true);
    expect(isTexasDirectDonorSupportReceipt({ row, electionYear: 2026 })).toBe(true);

    const excludedRows = [
      contribution({ filerTypeCd: "SPAC" }),
      contribution({ contributionAmount: "0" }),
      contribution({ contributionAmount: "-10" }),
      contribution({ contributionAmount: "not a number" }),
      contribution({ infoOnlyFlag: "Y" }),
      contribution({ contributionDt: "20240101" }),
    ];
    for (const excludedRow of excludedRows) {
      expect(isTexasTotalReceipt({ row: excludedRow, electionYear: 2026 })).toBe(false);
      expect(isTexasDirectDonorSupportReceipt({ row: excludedRow, electionYear: 2026 })).toBe(false);
    }
  });

  it("keeps total receipts aligned with direct donor support for Texas contribution rows", () => {
    const result = aggregateTexasDirectContributions({
      committeeId: "00012345",
      electionYear: 2026,
      contributionRows: [
        contribution({ contributionAmount: "1000", contributorOccupation: "Attorney" }),
        contribution({ contributionAmount: "5000", infoOnlyFlag: "Y", contributorOccupation: "Candidate" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(1000);
    expect(result.summary.directContributionTotal).toBe(1000);
    expect(result.directBreakdowns).toEqual([
      {
        categoryType: "occupation",
        categoryName: "Attorney",
        amount: 1000,
        contributorCount: 1,
        sourceUrl: null,
      },
      {
        categoryType: "contribution_size",
        categoryName: "$1,000-$4,999",
        amount: 1000,
        contributorCount: 1,
        sourceUrl: null,
      },
    ]);
    expect(result.skippedContributionRowCount).toBe(1);
  });

  it("skips invalid rows and blank occupations while keeping contribution-size totals", () => {
    const result = aggregateTexasDirectContributions({
      committeeId: "00012345",
      electionYear: 2026,
      contributionRows: [
        contribution({ contributionAmount: "250", contributorOccupation: "" }),
        contribution({ contributionAmount: "-100", contributorOccupation: "Attorney" }),
        contribution({ contributionAmount: "bad", contributorOccupation: "Doctor" }),
        contribution({ filerTypeCd: "SPAC", contributionAmount: "500", contributorOccupation: "Engineer" }),
        contribution({ contributionDt: "20240101", contributionAmount: "600", contributorOccupation: "Teacher" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(250);
    expect(result.summary.directContributionTotal).toBe(250);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(4);
    expect(result.directBreakdowns).toEqual([
      {
        categoryType: "contribution_size",
        categoryName: "$250-$499",
        amount: 250,
        contributorCount: 1,
        sourceUrl: null,
      },
    ]);
  });

  it("limits occupation breakdowns without trimming contribution-size buckets", () => {
    const result = aggregateTexasDirectContributions({
      committeeId: "00012345",
      electionYear: 2026,
      maxBreakdownsPerCategory: 2,
      contributionRows: [
        contribution({ contributionAmount: "10", contributorOccupation: "A", contributorNameLast: "A" }),
        contribution({ contributionAmount: "100", contributorOccupation: "B", contributorNameLast: "B" }),
        contribution({ contributionAmount: "250", contributorOccupation: "C", contributorNameLast: "C" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "C", amount: 250 }),
      expect.objectContaining({ categoryName: "B", amount: 100 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("rejects invalid inputs", () => {
    expect(() =>
      aggregateTexasDirectContributions({
        committeeId: " ",
        electionYear: 2026,
        contributionRows: [],
      })
    ).toThrow("Texas committee id is required");

    expect(() =>
      aggregateTexasDirectContributions({
        committeeId: "00012345",
        electionYear: 2013,
        contributionRows: [],
      })
    ).toThrow("Invalid Texas direct contribution aggregation election year");

    expect(() =>
      aggregateTexasDirectContributions({
        committeeId: "00012345",
        electionYear: 2026,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Texas direct contribution aggregation maxBreakdownsPerCategory");
  });
});
