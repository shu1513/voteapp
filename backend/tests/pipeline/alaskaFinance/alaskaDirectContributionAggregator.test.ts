import { describe, expect, it } from "vitest";

import { aggregateAlaskaDirectContributions } from "../../../src/pipeline/alaskaFinance/alaskaDirectContributionAggregator.js";
import type { AlaskaApocCampaignIncomeRow } from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

function income(overrides: Partial<AlaskaApocCampaignIncomeRow> = {}): AlaskaApocCampaignIncomeRow {
  return {
    reportYear: 2026,
    filerId: "1001",
    filerName: "Jane Doe",
    filerType: "Candidate",
    name: "Jane Doe",
    date: "10/01/2026",
    type: "Income",
    contributor: "Smith, Pat",
    address: "1 Main St",
    city: "Juneau",
    state: "AK",
    zip: "99801",
    country: "USA",
    paymentType: "Check",
    paymentDetail: "1001",
    occupation: "Attorney",
    employer: "Law Firm",
    purpose: "Contribution",
    amount: 250,
    submitted: "10/02/2026",
    status: "Complete, Not Amended",
    sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
    ...overrides,
  };
}

describe("alaskaDirectContributionAggregator", () => {
  it("aggregates APOC candidate income into top occupations and contribution size buckets", () => {
    const sourceUrl = "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx";
    const result = aggregateAlaskaDirectContributions({
      candidateName: "Jane Doe",
      electionYear: 2026,
      candidateFilerId: "1001",
      sourceUrl,
      incomeRows: [
        income({ amount: 100, occupation: "Attorney", contributor: "Smith, Pat" }),
        income({ amount: 250, occupation: "Attorney", contributor: "Roe, Alex" }),
        income({ amount: 5_000, occupation: "Teacher", contributor: "Adams, Robin" }),
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

  it("filters non-cycle, rejected, non-positive, and unmatched rows", () => {
    const result = aggregateAlaskaDirectContributions({
      candidateName: "Jane Doe",
      electionYear: 2026,
      candidateFilerName: "Jane Doe",
      incomeRows: [
        income({ amount: 100 }),
        income({ amount: 200, date: "01/01/2024", reportYear: 2024 }),
        income({ amount: 300, status: "Rejected" }),
        income({ amount: -50 }),
        income({ amount: 999, filerId: "9999", filerName: "Other Candidate", name: "Other Candidate" }),
      ],
    });

    expect(result.summary).toEqual({ totalReceipts: 100, directContributionTotal: 100, sourceUrl: null });
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(3);
  });

  it("counts distinct contributors by identity rather than row count", () => {
    const result = aggregateAlaskaDirectContributions({
      candidateName: "Jane Doe",
      electionYear: 2026,
      candidateFilerId: "1001",
      incomeRows: [
        income({ contributor: "Smith, Pat", amount: 100, occupation: "Attorney" }),
        income({ contributor: "Smith, Pat", amount: 200, occupation: "Attorney" }),
        income({ contributor: "Roe, Alex", amount: 300, occupation: "Attorney" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Attorney", amount: 600, contributorCount: 2 }),
    ]);
  });
});
