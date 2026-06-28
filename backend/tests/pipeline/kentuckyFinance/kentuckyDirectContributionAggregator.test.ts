import { describe, expect, it } from "vitest";

import { aggregateKentuckyDirectContributions } from "../../../src/pipeline/kentuckyFinance/kentuckyDirectContributionAggregator.js";
import type { KentuckyKrefContributionRecord } from "../../../src/pipeline/kentuckyFinance/kentuckyKrefClient.js";

function contribution(overrides: Partial<KentuckyKrefContributionRecord> = {}): KentuckyKrefContributionRecord {
  return {
    recipientName: "Andy Beshear",
    candidateName: "Andy Beshear",
    office: "GOVERNOR",
    location: "STATEWIDE",
    electionDate: "11/7/2023",
    electionYear: 2023,
    electionType: "GENERAL",
    contributorName: "Jane Doe",
    contributorType: "INDIVIDUAL",
    contributionMode: "DIRECT",
    occupation: "Attorney",
    employer: "Law Firm",
    amount: 250,
    receiptDate: "10/1/2023",
    ...overrides,
  };
}

describe("kentuckyDirectContributionAggregator", () => {
  it("aggregates structured candidate contribution rows by occupation and contribution size", () => {
    const sourceUrl = "https://secure.kentucky.gov/kref/publicsearch/ExportContributors";
    const result = aggregateKentuckyDirectContributions({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeName: "Governor",
      location: "Statewide",
      sourceUrl,
      contributionRecords: [
        contribution({ amount: 100, occupation: "Attorney" }),
        contribution({ contributorName: "John Roe", amount: 250, occupation: "Attorney" }),
        contribution({ contributorName: "Pat Smith", amount: 5_000, occupation: "Teacher" }),
      ],
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 5350,
        directContributionTotal: 5350,
        sourceUrl,
      },
      directBreakdowns: [
        { categoryType: "occupation", categoryName: "Teacher", amount: 5000, contributorCount: 1, sourceUrl },
        { categoryType: "occupation", categoryName: "Attorney", amount: 350, contributorCount: 2, sourceUrl },
        { categoryType: "contribution_size", categoryName: "$5,000+", amount: 5000, contributorCount: 1, sourceUrl },
        { categoryType: "contribution_size", categoryName: "$250-$499", amount: 250, contributorCount: 1, sourceUrl },
        { categoryType: "contribution_size", categoryName: "$100-$249", amount: 100, contributorCount: 1, sourceUrl },
      ],
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
    });
  });

  it("separates total receipts from individual direct support", () => {
    const result = aggregateKentuckyDirectContributions({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeName: "Governor",
      location: "Statewide",
      contributionRecords: [
        contribution({ amount: 500, contributorType: "INDIVIDUAL", contributionMode: "DIRECT", occupation: "Attorney" }),
        contribution({ amount: 1000, contributorType: "KYPAC", contributionMode: "DIRECT", occupation: "" }),
        contribution({ amount: 250, contributorType: "INDIVIDUAL", contributionMode: "EVENT_FUNDRAISING", occupation: "Teacher" }),
        contribution({ amount: 0, contributorType: "INDIVIDUAL", contributionMode: "DIRECT", occupation: "Attorney" }),
      ],
    });

    expect(result.summary).toEqual({ totalReceipts: 1750, directContributionTotal: 500, sourceUrl: null });
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(3);
  });

  it("requires structured candidate, election, office, and location matches", () => {
    const result = aggregateKentuckyDirectContributions({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeName: "Governor",
      location: "Statewide",
      contributionRecords: [
        contribution({ candidateName: "Other Candidate", amount: 100 }),
        contribution({ electionDate: "5/16/2023", amount: 200 }),
        contribution({ office: "Attorney General", amount: 300 }),
        contribution({ location: "Jefferson", amount: 400 }),
        contribution({ amount: 500 }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
  });

  it("matches comma-form candidate input against KREF first-last names", () => {
    const result = aggregateKentuckyDirectContributions({
      candidateName: "Beshear, Andy",
      electionDate: "2023-11-07",
      officeName: "Governor",
      location: "Statewide",
      contributionRecords: [contribution({ candidateName: "Andy Beshear", amount: 250 })],
    });

    expect(result.summary.directContributionTotal).toBe(250);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
  });

  it("matches app state-legislative office names against KREF office labels", () => {
    const result = aggregateKentuckyDirectContributions({
      candidateName: "Kim Banta",
      electionDate: "5/19/2026",
      officeName: "State Lower Chamber Legislator",
      location: "63",
      contributionRecords: [
        contribution({
          candidateName: "Kim Banta",
          electionDate: "5/19/2026",
          office: "STATE REPRESENTATIVE",
          location: "63RD DISTRICT",
          amount: 500,
          occupation: "Owner",
        }),
      ],
    });

    expect(result.summary.directContributionTotal).toBe(500);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
  });

  it("counts distinct contributors and sums cents without drift", () => {
    const result = aggregateKentuckyDirectContributions({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeName: "Governor",
      contributionRecords: [
        contribution({ contributorName: "Jane Doe", amount: 0.1, occupation: "Engineer" }),
        contribution({ contributorName: "Jane Doe", amount: 0.2, occupation: "Engineer" }),
        contribution({ contributorName: "John Roe", amount: 0.3, occupation: "Engineer" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(0.6);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Engineer", amount: 0.6, contributorCount: 2 }),
        expect.objectContaining({ categoryType: "contribution_size", categoryName: "$0.01-$0.99", amount: 0.6 }),
      ])
    );
  });

  it("validates required inputs", () => {
    expect(() =>
      aggregateKentuckyDirectContributions({
        candidateName: " ",
        electionDate: "11/7/2023",
        officeName: "Governor",
        contributionRecords: [],
      })
    ).toThrow("Kentucky candidate name is required");
    expect(() =>
      aggregateKentuckyDirectContributions({
        candidateName: "Andy Beshear",
        electionDate: "bad",
        officeName: "Governor",
        contributionRecords: [],
      })
    ).toThrow("Kentucky election date must use");
    expect(() =>
      aggregateKentuckyDirectContributions({
        candidateName: "Andy Beshear",
        electionDate: "11/7/2023",
        officeName: "Governor",
        contributionRecords: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("maxBreakdownsPerCategory");
  });
});
