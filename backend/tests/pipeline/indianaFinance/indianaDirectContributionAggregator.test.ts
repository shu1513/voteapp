import { describe, expect, it } from "vitest";

import {
  aggregateIndianaDirectContributions,
  indianaElectionCycleStartYear,
  isIndianaDirectDonorSupportReceipt,
  isIndianaTotalReceipt,
} from "../../../src/pipeline/indianaFinance/indianaDirectContributionAggregator.js";
import type { IndianaCampaignFinanceContributionRow } from "../../../src/pipeline/indianaFinance/indianaCampaignFinanceReader.js";

function contribution(overrides: Partial<IndianaCampaignFinanceContributionRow> = {}): IndianaCampaignFinanceContributionRow {
  return {
    FileNumber: "422",
    CommitteeType: "Candidate",
    Committee: "Diego for Indiana",
    CandidateName: "Cesar Diego Morales",
    ContributorType: "Individual",
    Name: "Jane Doe",
    Address: "100 Main St",
    City: "Indianapolis",
    State: "IN",
    Zip: "46204",
    Occupation: "Attorney/Legal",
    Type: "Direct",
    Description: "",
    Amount: "250.0000",
    ContributionDate: "2026-02-17 00:00:00",
    Received_By: "Treasurer",
    Amended: "0",
    ...overrides,
  };
}

describe("indianaDirectContributionAggregator", () => {
  it("aggregates direct donor support by occupation and contribution size", () => {
    const sourceUrl = "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip";
    const result = aggregateIndianaDirectContributions({
      committeeId: "422",
      electionYear: 2026,
      sourceUrl,
      contributionRows: [
        contribution({ Amount: "100.0000", Occupation: "Attorney/Legal" }),
        contribution({ Name: "John Roe", Amount: "$250.0000", Occupation: "Attorney/Legal" }),
        contribution({ Name: "Pat Smith", Amount: "5,000.0000", Occupation: "Teacher/Education" }),
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
          categoryName: "Teacher/Education",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "occupation",
          categoryName: "Attorney/Legal",
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
    const result = aggregateIndianaDirectContributions({
      committeeId: "422",
      electionYear: 2026,
      contributionRows: [
        contribution({ Amount: "100", Occupation: "Science/Technology" }),
        contribution({ Amount: "200", Occupation: "Science/Technology" }),
        contribution({ Name: "John Roe", Amount: "300", Occupation: "Science/Technology" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Science/Technology", amount: 600, contributorCount: 2 }),
    ]);
  });

  it("aggregates organization donor industries behind PACs that directly contributed to the candidate", () => {
    const result = aggregateIndianaDirectContributions({
      committeeId: "422",
      electionYear: 2026,
      contributionRows: [
        contribution({
          ContributorType: "PAC",
          Name: "Hoosier Progress PAC",
          Amount: "1000",
        }),
        contribution({
          FileNumber: "PAC-1",
          CommitteeType: "Political Action",
          Committee: "Hoosier Progress PAC",
          ContributorType: "Business",
          Name: "Eli Lilly and Company",
          Address: "1 Lilly Corporate Center",
          Amount: "2500",
        }),
        contribution({
          FileNumber: "PAC-1",
          CommitteeType: "Political Action",
          Committee: "Hoosier Progress PAC",
          ContributorType: "Organization",
          Name: "Google LLC",
          Address: "1600 Amphitheatre Pkwy",
          Amount: "500",
        }),
        contribution({
          FileNumber: "PAC-1",
          CommitteeType: "Political Action",
          Committee: "Hoosier Progress PAC",
          ContributorType: "Individual",
          Name: "Individual Donor",
          Amount: "9000",
        }),
        contribution({
          FileNumber: "PAC-2",
          CommitteeType: "Political Action",
          Committee: "Unmatched PAC",
          ContributorType: "Business",
          Name: "Google LLC",
          Amount: "7000",
        }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "pac_backed_industry")).toEqual([
      expect.objectContaining({
        categoryName: "pharmaceuticals",
        amount: 2500,
        contributorCount: 1,
      }),
      expect.objectContaining({
        categoryName: "technology",
        amount: 500,
        contributorCount: 1,
      }),
    ]);
    expect(result.summary.directContributionTotal).toBe(1000);
  });

  it("filters to candidate committee positive receipts in the two-year cycle", () => {
    expect(indianaElectionCycleStartYear(2026)).toBe(2025);

    const result = aggregateIndianaDirectContributions({
      committeeId: "422",
      electionYear: 2026,
      contributionRows: [
        contribution({ ContributionDate: "2024-12-31 00:00:00", Amount: "100" }),
        contribution({ ContributionDate: "2025-01-01 00:00:00", Amount: "200" }),
        contribution({ ContributionDate: "2026-11-01 00:00:00", Amount: "300" }),
        contribution({ ContributionDate: "2027-01-01 00:00:00", Amount: "400" }),
        contribution({ CommitteeType: "Political Action", Amount: "500" }),
        contribution({ Amount: "-10" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([expect.objectContaining({ categoryType: "contribution_size", categoryName: "$250-$499", amount: 300 })])
    );
    expect(result.matchedContributionRowCount).toBe(6);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(4);
  });

  it("tracks total receipts separately from included direct support types", () => {
    expect(isIndianaTotalReceipt({ row: contribution({ Type: "Interest" }), electionYear: 2026 })).toBe(true);
    expect(isIndianaDirectDonorSupportReceipt({ row: contribution({ Type: "Interest" }), electionYear: 2026 })).toBe(false);
    expect(isIndianaDirectDonorSupportReceipt({ row: contribution({ Type: "Unitemized" }), electionYear: 2026 })).toBe(true);

    const result = aggregateIndianaDirectContributions({
      committeeId: "422",
      electionYear: 2026,
      contributionRows: [
        contribution({ Type: "Direct", Amount: "100" }),
        contribution({ Type: "Unitemized", Amount: "200" }),
        contribution({ Type: "Interest", Amount: "50" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(350);
    expect(result.summary.directContributionTotal).toBe(300);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(1);
  });

  it("keeps amended rows in the aggregation instead of silently dropping them", () => {
    const result = aggregateIndianaDirectContributions({
      committeeId: "422",
      electionYear: 2026,
      contributionRows: [
        contribution({ Amount: "100", Amended: "0" }),
        contribution({ Name: "Amended Donor", Amount: "200", Amended: "1" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(300);
    expect(result.summary.directContributionTotal).toBe(300);
    expect(result.includedContributionRowCount).toBe(2);
  });

  it("limits occupation breakdowns without dropping contribution-size buckets", () => {
    const result = aggregateIndianaDirectContributions({
      committeeId: "422",
      electionYear: 2026,
      maxBreakdownsPerCategory: 1,
      contributionRows: [
        contribution({ Occupation: "Engineer", Amount: "100" }),
        contribution({ Name: "Two", Occupation: "Teacher", Amount: "300" }),
        contribution({ Name: "Three", Occupation: "Doctor", Amount: "600" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Doctor", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });
});
