import { describe, expect, it } from "vitest";

import { aggregateTennesseeOutsideGroupContributions } from "../../../src/pipeline/tennesseeFinance/tennesseeOutsideGroupContributionAggregator.js";
import type { TennesseeCampContributionRecord } from "../../../src/pipeline/tennesseeFinance/tennesseeCampClient.js";

function contribution(overrides: Partial<TennesseeCampContributionRecord> = {}): TennesseeCampContributionRecord {
  return {
    type: "Monetary",
    adjustment: "N",
    amount: 25000,
    date: "08/01/2022",
    electionYear: 2022,
    reportName: "Pre-General",
    recipientName: "RIGHT TENNESSEE",
    contributorName: "GREEN ENERGY LLC",
    contributorOccupation: null,
    contributorEmployer: null,
    ...overrides,
  };
}

describe("tennesseeOutsideGroupContributionAggregator", () => {
  it("aggregates organization donors and rule-based industries for matched outside groups", () => {
    const result = aggregateTennesseeOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [
        {
          committeeKey: "RIGHT TENNESSEE",
          committeeName: "RIGHT TENNESSEE",
          supportOppose: "support",
          amount: 100000,
          expenditureCount: 1,
          sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm",
        },
      ],
      contributionRecords: [
        contribution(),
        contribution({ amount: 50000, contributorName: "TENNESSEE BANK PAC" }),
        contribution({
          amount: 30000,
          contributorName: "DOE, JANE",
          contributorEmployer: "HCA Hospital",
          contributorOccupation: "Physician",
        }),
        contribution({
          amount: 20000,
          contributorName: "SMITH, JOHN",
          contributorEmployer: "HCA Hospital",
          contributorOccupation: "Physician",
        }),
        contribution({ recipientName: "OTHER PAC", contributorName: "ENERGY TRANSFER" }),
      ],
      sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?export=1",
      maxBreakdownsPerCategory: 5,
      minIndustryAmount: 10000,
    });

    expect(result).toMatchObject({
      matchedContributionRowCount: 4,
      includedContributionRowCount: 4,
      skippedContributionRowCount: 0,
    });
    expect(result.outsideGroupBreakdowns).toEqual([
      {
        committeeKey: "RIGHT TENNESSEE",
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "TENNESSEE BANK PAC",
        amount: 50000,
        contributorCount: 1,
        sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?export=1",
      },
      {
        committeeKey: "RIGHT TENNESSEE",
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "GREEN ENERGY LLC",
        amount: 25000,
        contributorCount: 1,
        sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?export=1",
      },
      {
        committeeKey: "RIGHT TENNESSEE",
        supportOppose: "support",
        categoryType: "employer",
        categoryName: "HCA Hospital",
        amount: 50000,
        contributorCount: 2,
        sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?export=1",
      },
      {
        committeeKey: "RIGHT TENNESSEE",
        supportOppose: "support",
        categoryType: "occupation",
        categoryName: "Physician",
        amount: 50000,
        contributorCount: 2,
        sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?export=1",
      },
      {
        committeeKey: "RIGHT TENNESSEE",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "finance_investment",
        amount: 50000,
        contributorCount: 1,
        sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?export=1",
      },
      {
        committeeKey: "RIGHT TENNESSEE",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "healthcare",
        amount: 50000,
        contributorCount: 2,
        sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?export=1",
      },
      {
        committeeKey: "RIGHT TENNESSEE",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "oil_gas_energy",
        amount: 25000,
        contributorCount: 1,
        sourceUrl: "https://apps.tn.gov/tncamp/public/ceresults.htm?export=1",
      },
    ]);
  });

  it("skips stale, adjusted, non-monetary, and individual-looking rows", () => {
    const result = aggregateTennesseeOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [
        {
          committeeKey: "RIGHT TENNESSEE",
          committeeName: "RIGHT TENNESSEE",
          supportOppose: "support",
          amount: 100000,
          expenditureCount: 1,
          sourceUrl: null,
        },
      ],
      contributionRecords: [
        contribution({ date: "01/01/2020" }),
        contribution({ adjustment: "Y" }),
        contribution({ type: "In-Kind" }),
        contribution({ contributorName: "SMITH, JOHN" }),
      ],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 4,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 4,
    });
  });
});
