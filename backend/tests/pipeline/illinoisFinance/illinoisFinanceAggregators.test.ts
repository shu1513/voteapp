import { describe, expect, it } from "vitest";

import {
  aggregateIllinoisDirectContributions,
  aggregateIllinoisOutsideGroupContributions,
  aggregateIllinoisOutsideSpending,
} from "../../../src/pipeline/illinoisFinance/illinoisFinanceAggregators.js";
import type {
  IllinoisSbeContributionRecord,
  IllinoisSbeExpenditureRecord,
} from "../../../src/pipeline/illinoisFinance/illinoisSbeClient.js";

function contribution(overrides: Partial<IllinoisSbeContributionRecord>): IllinoisSbeContributionRecord {
  return {
    contributorName: "Jane Donor",
    contributorAddress: "1 Main St",
    occupation: "Attorney",
    employer: "Law LLP",
    amount: 100,
    receivedDate: "3/1/2022",
    reportReceivedDate: null,
    contributionType: "Individual Contributions",
    recipientCommitteeName: "Friends of Jane",
    description: null,
    vendorName: null,
    vendorAddress: null,
    sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx",
    ...overrides,
  };
}

function expenditure(overrides: Partial<IllinoisSbeExpenditureRecord>): IllinoisSbeExpenditureRecord {
  return {
    payeeName: "Vendor",
    payeeAddress: null,
    amount: 1000,
    expendedDate: "10/1/2022",
    reportReceivedDate: null,
    expenditureType: "Independent Expenditures",
    expendingCommitteeName: "Illinois Conservation Action",
    purpose: "Mail",
    candidateName: "Jane Doe",
    officeDistrict: "Governor",
    supportOppose: "support",
    sourceUrl: "https://www.elections.il.gov/CampaignDisclosure/ExpenditureSearchByAllExpenditures.aspx",
    ...overrides,
  };
}

describe("illinoisFinanceAggregators", () => {
  it("aggregates direct occupations and contribution-size buckets with cycle filtering", () => {
    const result = aggregateIllinoisDirectContributions({
      electionYear: 2022,
      maxBreakdownsPerCategory: 5,
      contributionRecords: [
        contribution({ contributorName: "Alice", occupation: "Attorney", amount: 1000, receivedDate: "3/1/2022" }),
        contribution({ contributorName: "Bob", occupation: "Attorney", amount: 500, receivedDate: "4/1/2022" }),
        contribution({ contributorName: "Cara", occupation: "Construction", amount: 50, receivedDate: "5/1/2021" }),
        contribution({ contributorName: "Refund", occupation: "Attorney", amount: -25, receivedDate: "6/1/2022" }),
        contribution({ contributorName: "Old", occupation: "Attorney", amount: 10000, receivedDate: "12/31/2020" }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(5);
    expect(result.includedContributionRowCount).toBe(3);
    expect(result.skippedContributionRowCount).toBe(2);
    expect(result.summary.directContributionTotal).toBe(1550);
    expect(result.directBreakdowns).toEqual([
      {
        categoryType: "occupation",
        categoryName: "Attorney",
        amount: 1500,
        contributorCount: 2,
        sourceUrl: null,
      },
      {
        categoryType: "occupation",
        categoryName: "Construction",
        amount: 50,
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
      {
        categoryType: "contribution_size",
        categoryName: "$500-$999",
        amount: 500,
        contributorCount: 1,
        sourceUrl: null,
      },
      {
        categoryType: "contribution_size",
        categoryName: "$1-$99",
        amount: 50,
        contributorCount: 1,
        sourceUrl: null,
      },
    ]);
  });

  it("skips direct contribution rows outside the resolved target committee", () => {
    const result = aggregateIllinoisDirectContributions({
      electionYear: 2022,
      committeeKey: "Friends of Jane",
      contributionRecords: [
        contribution({
          contributorName: "Alice",
          occupation: "Attorney",
          amount: 1000,
          recipientCommitteeName: "Friends of Jane",
        }),
        contribution({
          contributorName: "Wrong Committee Donor",
          occupation: "Developer",
          amount: 5000,
          recipientCommitteeName: "Friends of Janet",
        }),
      ],
    });

    expect(result).toMatchObject({
      matchedContributionRowCount: 2,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 1,
      summary: {
        totalReceipts: 1000,
        directContributionTotal: 1000,
      },
    });
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
  });

  it("aggregates supporting and opposing outside groups from included expenditures", () => {
    const result = aggregateIllinoisOutsideSpending({
      electionYear: 2022,
      maxGroups: 10,
      expenditureRecords: [
        expenditure({ expendingCommitteeName: "Illinois Conservation Action", amount: 10000, supportOppose: "support" }),
        expenditure({ expendingCommitteeName: "Illinois Conservation Action", amount: 2500, supportOppose: "support" }),
        expenditure({ expendingCommitteeName: "People Against Jane", amount: 7000, supportOppose: "oppose" }),
        expenditure({ expendingCommitteeName: "People Against Jane", amount: 99, supportOppose: "oppose", expendedDate: "1/1/2020" }),
        expenditure({ expendingCommitteeName: "Bad Refund", amount: -50, supportOppose: "support" }),
      ],
    });

    expect(result.includedExpenditureRowCount).toBe(3);
    expect(result.skippedExpenditureRowCount).toBe(2);
    expect(result.summary).toMatchObject({
      supportTotal: 12500,
      opposeTotal: 7000,
      groups: [
        {
          committeeKey: "ILLINOIS CONSERVATION ACTION",
          committeeName: "Illinois Conservation Action",
          supportOppose: "support",
          amount: 12500,
          expenditureCount: 2,
        },
        {
          committeeKey: "PEOPLE AGAINST JANE",
          committeeName: "People Against Jane",
          supportOppose: "oppose",
          amount: 7000,
          expenditureCount: 1,
        },
      ],
    });
  });

  it("aggregates outside group donors and industries by supporting/opposing group", () => {
    const outsideGroups = [
      {
        committeeKey: "ILLINOIS CONSERVATION ACTION",
        committeeName: "Illinois Conservation Action",
        supportOppose: "support" as const,
        amount: 12500,
        expenditureCount: 2,
        sourceUrl: null,
      },
    ];
    const result = aggregateIllinoisOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups,
      minIndustryAmount: 25_000,
      maxBreakdownsPerCategory: 10,
      contributionRecords: [
        contribution({
          contributorName: "Sierra Club",
          contributorAddress: null,
          occupation: null,
          amount: 30000,
          recipientCommitteeName: "Illinois Conservation Action",
          contributionType: "Transfers In",
          receivedDate: "9/1/2022",
        }),
        contribution({
          contributorName: "Small Environmental Donor",
          contributorAddress: null,
          occupation: null,
          amount: 1000,
          recipientCommitteeName: "Illinois Conservation Action",
          contributionType: "Transfers In",
          receivedDate: "9/2/2022",
        }),
        contribution({
          contributorName: "Old Donor",
          contributorAddress: null,
          occupation: null,
          amount: 50000,
          recipientCommitteeName: "Illinois Conservation Action",
          contributionType: "Transfers In",
          receivedDate: "1/1/2020",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([
      {
        committeeKey: "ILLINOIS CONSERVATION ACTION",
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "Sierra Club",
        amount: 30000,
        contributorCount: 1,
        sourceUrl: null,
      },
      {
        committeeKey: "ILLINOIS CONSERVATION ACTION",
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "Small Environmental Donor",
        amount: 1000,
        contributorCount: 1,
        sourceUrl: null,
      },
      {
        committeeKey: "ILLINOIS CONSERVATION ACTION",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "environmental_group",
        amount: 30000,
        contributorCount: 1,
        sourceUrl: null,
      },
    ]);
  });

  it("skips outside group funders for committees that appear on both support and oppose sides", () => {
    const result = aggregateIllinoisOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [
        {
          committeeKey: "ILLINOIS CONSERVATION ACTION",
          committeeName: "Illinois Conservation Action",
          supportOppose: "support",
          amount: 10000,
          expenditureCount: 1,
          sourceUrl: null,
        },
        {
          committeeKey: "ILLINOIS CONSERVATION ACTION",
          committeeName: "Illinois Conservation Action",
          supportOppose: "oppose",
          amount: 2500,
          expenditureCount: 1,
          sourceUrl: null,
        },
      ],
      contributionRecords: [
        contribution({
          contributorName: "Sierra Club",
          contributorAddress: null,
          occupation: null,
          amount: 30000,
          recipientCommitteeName: "Illinois Conservation Action",
          contributionType: "Transfers In",
          receivedDate: "9/1/2022",
        }),
      ],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 1,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 1,
    });
  });
});
