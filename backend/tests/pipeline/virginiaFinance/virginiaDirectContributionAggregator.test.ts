import { describe, expect, it } from "vitest";

import {
  aggregateVirginiaDirectContributions,
  isVirginiaDirectDonorSupportContribution,
  virginiaElectionCycleStartYear,
} from "../../../src/pipeline/virginiaFinance/virginiaDirectContributionAggregator.js";
import type { VirginiaScheduleAContribution } from "../../../src/pipeline/virginiaFinance/virginiaCampaignFinanceClient.js";

function contribution(overrides: Partial<VirginiaScheduleAContribution> = {}): VirginiaScheduleAContribution {
  return {
    contributorName: "Jane Doe",
    isIndividual: true,
    employer: "Acme",
    occupationOrTypeOfBusiness: "Attorney",
    transactionDate: "01/10/2025",
    amount: 250,
    totalToDate: 500,
    ...overrides,
  };
}

describe("virginiaDirectContributionAggregator", () => {
  it("aggregates individual Schedule A contributions by occupation and contribution size", () => {
    const sourceUrl = "https://cfreports.elections.virginia.gov/Report/ReportXML/479054";
    const result = aggregateVirginiaDirectContributions({
      electionYear: 2025,
      sourceUrl,
      contributions: [
        contribution({ amount: 100, occupationOrTypeOfBusiness: "Attorney" }),
        contribution({
          contributorName: "John Roe",
          amount: 250,
          occupationOrTypeOfBusiness: "Attorney",
        }),
        contribution({
          contributorName: "Pat Smith",
          amount: 5_000,
          occupationOrTypeOfBusiness: "Teacher",
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

  it("excludes PAC and organization rows from direct donor occupation output", () => {
    const result = aggregateVirginiaDirectContributions({
      electionYear: 2025,
      contributions: [
        contribution({ amount: 500, occupationOrTypeOfBusiness: "Attorney" }),
        contribution({
          contributorName: "Advancing Democracy and Mobilization PAC",
          isIndividual: false,
          employer: null,
          occupationOrTypeOfBusiness: "Federal PAC",
          amount: 5_000,
        }),
        contribution({
          contributorName: "Unknown IsIndividual Row",
          isIndividual: null,
          occupationOrTypeOfBusiness: "CEO",
          amount: 1_000,
        }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(2);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 500 }),
      ])
    );
    expect(result.directBreakdowns.some((row) => row.categoryName === "Federal PAC")).toBe(false);
    expect(result.directBreakdowns.some((row) => row.categoryName === "CEO")).toBe(false);
  });

  it("does not emit employer breakdowns", () => {
    const result = aggregateVirginiaDirectContributions({
      electionYear: 2025,
      contributions: [
        contribution({
          amount: 300,
          employer: "Acme Law",
          occupationOrTypeOfBusiness: "Attorney",
        }),
      ],
    });

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

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateVirginiaDirectContributions({
      electionYear: 2025,
      contributions: [
        contribution({ contributorName: "Jane Doe", amount: 100, occupationOrTypeOfBusiness: "Attorney" }),
        contribution({ contributorName: "Jane Doe", amount: 200, occupationOrTypeOfBusiness: "Attorney" }),
        contribution({ contributorName: "John Roe", amount: 300, occupationOrTypeOfBusiness: "Attorney" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Attorney", amount: 600, contributorCount: 2 }),
    ]);
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateVirginiaDirectContributions({
      electionYear: 2025,
      contributions: [
        contribution({ amount: 0.1, occupationOrTypeOfBusiness: "Engineer" }),
        contribution({ amount: 0.2, occupationOrTypeOfBusiness: "Engineer" }),
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

  it("filters to the two-year cycle ending in the election year", () => {
    expect(virginiaElectionCycleStartYear(2025)).toBe(2024);

    const result = aggregateVirginiaDirectContributions({
      electionYear: 2025,
      contributions: [
        contribution({ transactionDate: "12/31/2023", amount: 100 }),
        contribution({ transactionDate: "1/1/2024", amount: 200 }),
        contribution({ transactionDate: "2025-11-01", amount: 300 }),
        contribution({ transactionDate: "1/1/2026", amount: 400 }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.summary.directContributionTotal).toBe(500);
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(2);
  });

  it("treats only positive individual in-cycle Schedule A rows as direct donor support", () => {
    const valid = contribution({ amount: 250, transactionDate: "01/10/2025", isIndividual: true });
    expect(isVirginiaDirectDonorSupportContribution({ contribution: valid, electionYear: 2025 })).toBe(true);

    expect(
      isVirginiaDirectDonorSupportContribution({
        contribution: contribution({ amount: -10 }),
        electionYear: 2025,
      })
    ).toBe(false);
    expect(
      isVirginiaDirectDonorSupportContribution({
        contribution: contribution({ isIndividual: false }),
        electionYear: 2025,
      })
    ).toBe(false);
    expect(
      isVirginiaDirectDonorSupportContribution({
        contribution: contribution({ transactionDate: "01/10/2023" }),
        electionYear: 2025,
      })
    ).toBe(false);
  });

  it("limits occupation breakdowns without dropping contribution-size buckets", () => {
    const result = aggregateVirginiaDirectContributions({
      electionYear: 2025,
      maxBreakdownsPerCategory: 1,
      contributions: [
        contribution({ occupationOrTypeOfBusiness: "Engineer", amount: 100 }),
        contribution({ occupationOrTypeOfBusiness: "Teacher", amount: 300 }),
        contribution({ occupationOrTypeOfBusiness: "Doctor", amount: 600 }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Doctor", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("rejects invalid inputs", () => {
    expect(() => aggregateVirginiaDirectContributions({ electionYear: 1999, contributions: [] })).toThrow(
      "Invalid Virginia direct contribution aggregation election year"
    );
    expect(() =>
      aggregateVirginiaDirectContributions({
        electionYear: 2025,
        contributions: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("maxBreakdownsPerCategory");
  });
});
