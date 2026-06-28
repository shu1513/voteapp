import { describe, expect, it } from "vitest";

import {
  aggregateTennesseeDirectContributions,
  isTennesseeDirectDonorSupportContribution,
  tennesseeElectionCycleStartYear,
} from "../../../src/pipeline/tennesseeFinance/tennesseeDirectContributionAggregator.js";
import { normalizeTennesseeCandidateNameKeys } from "../../../src/pipeline/tennesseeFinance/tennesseeCandidateCommitteeResolver.js";
import type { TennesseeCampContributionRecord } from "../../../src/pipeline/tennesseeFinance/tennesseeCampClient.js";

function contribution(overrides: Partial<TennesseeCampContributionRecord> = {}): TennesseeCampContributionRecord {
  return {
    type: "Monetary",
    adjustment: "N",
    amount: 250,
    date: "02/18/2022",
    electionYear: 2022,
    reportName: "1st Quarter",
    recipientName: "LEE, BILL",
    contributorName: "DOE, JANE",
    contributorOccupation: "Attorney",
    contributorEmployer: "Acme",
    ...overrides,
  };
}

describe("tennesseeDirectContributionAggregator", () => {
  it("aggregates Tennessee CAMP individual contributions by occupation and contribution size", () => {
    const sourceUrl = "https://apps.tn.gov/tncamp/public/ceresults.htm?d-1341904-e=1&6578706f7274=1";
    const result = aggregateTennesseeDirectContributions({
      candidate: { ownerName: "LEE, BILL", candidateName: "Bill Lee" },
      electionYear: 2022,
      sourceUrl,
      contributions: [
        contribution({ amount: 100, contributorOccupation: "Attorney" }),
        contribution({ contributorName: "ROE, JOHN", amount: 250, contributorOccupation: "Attorney" }),
        contribution({ contributorName: "SMITH, PAT", amount: 5_000, contributorOccupation: "Teacher" }),
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

  it("filters to the two-year cycle and exact recipient candidate", () => {
    expect(tennesseeElectionCycleStartYear(2022)).toBe(2021);
    const result = aggregateTennesseeDirectContributions({
      candidate: { ownerName: "LEE, BILL", candidateName: "Bill Lee" },
      electionYear: 2022,
      contributions: [
        contribution({ date: "12/31/2020", amount: 100 }),
        contribution({ date: "1/1/2021", amount: 200 }),
        contribution({ date: "2022-11-01", amount: 300 }),
        contribution({ recipientName: "LEE, REBECCA V.", amount: 400 }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(1);
  });

  it("requires monetary non-adjustment rows", () => {
    const candidateNameKeys = normalizeTennesseeCandidateNameKeys("Bill Lee");
    expect(
      isTennesseeDirectDonorSupportContribution({
        contribution: contribution(),
        candidateNameKeys,
        electionYear: 2022,
      })
    ).toBe(true);
    expect(
      isTennesseeDirectDonorSupportContribution({
        contribution: contribution({ type: "In-Kind" }),
        candidateNameKeys,
        electionYear: 2022,
      })
    ).toBe(false);
    expect(
      isTennesseeDirectDonorSupportContribution({
        contribution: contribution({ adjustment: "Y" }),
        candidateNameKeys,
        electionYear: 2022,
      })
    ).toBe(false);
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateTennesseeDirectContributions({
        candidate: { ownerName: "LEE, BILL", candidateName: "Bill Lee" },
        electionYear: 1999,
        contributions: [],
      })
    ).toThrow("Invalid Tennessee direct contribution aggregation election year");
  });
});
