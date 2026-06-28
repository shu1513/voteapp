import { describe, expect, it } from "vitest";

import { aggregateLouisianaDirectContributions } from "../../../src/pipeline/louisianaFinance/louisianaDirectContributionAggregator.js";
import type { LouisianaCampaignFinanceCsvRow } from "../../../src/pipeline/louisianaFinance/louisianaCampaignFinanceArtifactReader.js";

function contribution(overrides: Partial<LouisianaCampaignFinanceCsvRow> = {}): LouisianaCampaignFinanceCsvRow {
  return {
    FilerNumber: "12345",
    FilerLastName: "Edwards",
    FilerFirstName: "John Bel",
    ReportCode: "10-G",
    ReportType: "10-G",
    ReportNumber: "1",
    ContributorTypeCode: "IND",
    ContributorName: "Doe, Jane",
    ContributorAddr1: "100 Main St",
    ContributorAddr2: "",
    ContributorCity: "Baton Rouge",
    ContributorrState: "LA",
    ContributorZip: "70801",
    ContributionType: "MONETARY",
    ContributionDescription: "",
    ContributionDate: "01/15/2027",
    ContributionAmt: "100.00",
    ContributionDesignatedElectionAdditionInfo: "",
    ...overrides,
  };
}

describe("louisianaDirectContributionAggregator", () => {
  it("aggregates candidate money by contribution size and contributor type only", () => {
    const sourceUrl = "https://www.ethics.la.gov/";
    const result = aggregateLouisianaDirectContributions({
      filerNumber: "12345",
      electionYear: 2027,
      sourceUrl,
      contributionRows: [
        contribution({ ContributionAmt: "100.00", ContributorTypeCode: "IND", ContributorName: "Doe, Jane" }),
        contribution({
          ContributionAmt: "250.00",
          ContributorTypeCode: "IND",
          ContributorName: "Roe, John",
          ContributorAddr1: "200 Main St",
        }),
        contribution({
          ContributionAmt: "5,000.00",
          ContributorTypeCode: "PAC",
          ContributorName: "Better Louisiana PAC",
          ContributorAddr1: "300 Main St",
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
          categoryType: "contributor_type",
          categoryName: "PAC",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contributor_type",
          categoryName: "IND",
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
    expect(result.directBreakdowns.map((row) => row.categoryType)).not.toContain("occupation");
  });

  it("filters wrong filers, dates outside the cycle, and invalid amounts", () => {
    const result = aggregateLouisianaDirectContributions({
      filerNumber: "12345",
      electionYear: 2027,
      contributionRows: [
        contribution({ ContributionAmt: "500.00" }),
        contribution({ FilerNumber: "99999", ContributionAmt: "1000.00" }),
        contribution({ ContributionDate: "12/31/2025", ContributionAmt: "1000.00" }),
        contribution({ ContributionDate: "", ContributionAmt: "1000.00" }),
        contribution({ ContributionAmt: "0.00" }),
        contribution({ ContributionAmt: "not money" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.matchedContributionRowCount).toBe(5);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(4);
  });

  it("counts distinct contributors and limits contributor type categories", () => {
    const result = aggregateLouisianaDirectContributions({
      filerNumber: "12345",
      electionYear: 2027,
      maxBreakdownsPerCategory: 1,
      contributionRows: [
        contribution({ ContributionAmt: "600.00", ContributorTypeCode: "IND", ContributorName: "Doe, Jane" }),
        contribution({ ContributionAmt: "200.00", ContributorTypeCode: "IND", ContributorName: "Doe, Jane" }),
        contribution({
          ContributionAmt: "300.00",
          ContributorTypeCode: "BUS",
          ContributorName: "Acme LLC",
        }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "contributor_type")).toEqual([
      expect.objectContaining({ categoryName: "IND", amount: 800, contributorCount: 1 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("rejects invalid inputs", () => {
    expect(() =>
      aggregateLouisianaDirectContributions({
        filerNumber: "",
        electionYear: 2027,
        contributionRows: [],
      })
    ).toThrow("Louisiana filer number is required");
    expect(() =>
      aggregateLouisianaDirectContributions({
        filerNumber: "12345",
        electionYear: 1999,
        contributionRows: [],
      })
    ).toThrow("Invalid Louisiana direct contribution aggregation election year");
    expect(() =>
      aggregateLouisianaDirectContributions({
        filerNumber: "12345",
        electionYear: 2027,
        maxBreakdownsPerCategory: 0,
        contributionRows: [],
      })
    ).toThrow("maxBreakdownsPerCategory");
  });
});
