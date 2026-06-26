import { describe, expect, it } from "vitest";

import {
  aggregateNewJerseyDirectContributions,
  isNewJerseyDirectDonorSupportContribution,
} from "../../../src/pipeline/newJerseyFinance/newJerseyDirectContributionAggregator.js";
import type { NewJerseyElecContributionRow } from "../../../src/pipeline/newJerseyFinance/newJerseyElecClient.js";

function contribution(overrides: Partial<NewJerseyElecContributionRow> = {}): NewJerseyElecContributionRow {
  return {
    contribS: 1001,
    entityS: 473742,
    electionYear: 2025,
    recipientName: "SHERRILL, MIKIE",
    contributorName: "Jane Doe",
    contributorFirstName: "Jane",
    contributorLastName: "Doe",
    contributorNonIndividualName: null,
    isIndividual: true,
    contributorType: "Individual",
    contributionType: "Monetary",
    contributionDate: "06/01/2025",
    amount: 250,
    employerName: "Acme Law",
    occupationCode: "1100",
    occupationName: "Attorney",
    sourceUrl: "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742",
    ...overrides,
  };
}

describe("newJerseyDirectContributionAggregator", () => {
  it("aggregates individual ELEC contributions by occupation, employer, and size", () => {
    const sourceUrl = "https://www.njelecefilesearch.com/SearchContributionToEntity?eid=473742";
    const result = aggregateNewJerseyDirectContributions({
      entityS: 473742,
      electionYear: 2025,
      sourceUrl,
      contributions: [
        contribution({ amount: 100, occupationName: "Attorney", employerName: "Acme Law" }),
        contribution({
          contribS: 1002,
          contributorName: "John Roe",
          amount: 250,
          occupationName: "Attorney",
          employerName: "Acme Law",
        }),
        contribution({
          contribS: 1003,
          contributorName: "Pat Smith",
          amount: 5_000,
          occupationName: "Teacher",
          employerName: "Public School",
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
          categoryType: "employer",
          categoryName: "Public School",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "employer",
          categoryName: "Acme Law",
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

  it("excludes PAC, negative, wrong year, and wrong entity rows from direct donor support", () => {
    const result = aggregateNewJerseyDirectContributions({
      entityS: 473742,
      electionYear: 2025,
      contributions: [
        contribution({ amount: 500, occupationName: "Attorney" }),
        contribution({
          contribS: 1002,
          contributorName: "Support PAC",
          isIndividual: false,
          occupationName: "Federal PAC",
          amount: 5_000,
        }),
        contribution({ contribS: 1003, amount: -10, occupationName: "CEO" }),
        contribution({ contribS: 1004, electionYear: 2021, amount: 100, occupationName: "Doctor" }),
        contribution({ contribS: 1005, entityS: 999999, amount: 250, occupationName: "Engineer" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(3);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 500 }),
      ])
    );
    expect(result.directBreakdowns.some((row) => row.categoryName === "Federal PAC")).toBe(false);
    expect(result.directBreakdowns.some((row) => row.categoryName === "Engineer")).toBe(false);
  });

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateNewJerseyDirectContributions({
      entityS: 473742,
      electionYear: 2025,
      contributions: [
        contribution({ contributorName: "Jane Doe", amount: 100, occupationName: "Attorney" }),
        contribution({ contributorName: "Jane Doe", amount: 200, occupationName: "Attorney" }),
        contribution({ contributorName: "John Roe", amount: 300, occupationName: "Attorney" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Attorney", amount: 600, contributorCount: 2 }),
    ]);
  });

  it("normalizes aggregate keys without replacing the display label", () => {
    const result = aggregateNewJerseyDirectContributions({
      entityS: 473742,
      electionYear: 2025,
      contributions: [
        contribution({
          contributorName: "Jane Doe",
          amount: 100,
          occupationName: "Attorney",
          employerName: "Acme & Co.",
        }),
        contribution({
          contribS: 1002,
          contributorName: "John Roe",
          amount: 200,
          occupationName: "ATTORNEY",
          employerName: "ACME AND CO",
        }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Attorney", amount: 300, contributorCount: 2 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "employer")).toEqual([
      expect.objectContaining({ categoryName: "Acme & Co.", amount: 300, contributorCount: 2 }),
    ]);
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateNewJerseyDirectContributions({
      entityS: 473742,
      electionYear: 2025,
      contributions: [
        contribution({ amount: 0.1, occupationName: "Engineer" }),
        contribution({ amount: 0.2, occupationName: "Engineer" }),
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

  it("treats only positive individual rows for the requested entity and year as direct donor support", () => {
    expect(
      isNewJerseyDirectDonorSupportContribution({
        contribution: contribution({ amount: 250, isIndividual: true }),
        entityS: 473742,
        electionYear: 2025,
      })
    ).toBe(true);
    expect(
      isNewJerseyDirectDonorSupportContribution({
        contribution: contribution({ amount: -10 }),
        entityS: 473742,
        electionYear: 2025,
      })
    ).toBe(false);
    expect(
      isNewJerseyDirectDonorSupportContribution({
        contribution: contribution({ isIndividual: false }),
        entityS: 473742,
        electionYear: 2025,
      })
    ).toBe(false);
    expect(
      isNewJerseyDirectDonorSupportContribution({
        contribution: contribution({ entityS: 999999 }),
        entityS: 473742,
        electionYear: 2025,
      })
    ).toBe(false);
  });

  it("limits occupation and employer breakdowns without dropping size buckets", () => {
    const result = aggregateNewJerseyDirectContributions({
      entityS: 473742,
      electionYear: 2025,
      maxBreakdownsPerCategory: 1,
      contributions: [
        contribution({ occupationName: "Engineer", employerName: "Firm A", amount: 100 }),
        contribution({ occupationName: "Teacher", employerName: "Firm B", amount: 300 }),
        contribution({ occupationName: "Doctor", employerName: "Firm C", amount: 600 }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Doctor", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "employer")).toEqual([
      expect.objectContaining({ categoryName: "Firm C", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });
});
