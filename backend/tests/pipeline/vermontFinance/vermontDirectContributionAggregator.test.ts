import { describe, expect, it } from "vitest";

import { aggregateVermontDirectContributions } from "../../../src/pipeline/vermontFinance/vermontDirectContributionAggregator.js";
import type { VermontContributionRow } from "../../../src/pipeline/vermontFinance/vermontCampaignFinanceClient.js";

function contribution(overrides: Partial<VermontContributionRow> = {}): VermontContributionRow {
  return {
    transactionId: 1,
    transactionVersionId: 1,
    guid: "contribution-guid-1",
    filerRegistrationGuid: "candidate-guid",
    filerName: "SCOTT, PHIL",
    transactionAmount: 100,
    transactionDate: "03/01/2024",
    sourceName: "DOE, JANE",
    sourceFirstName: "JANE",
    sourceLastName: "DOE",
    sourceMiddleName: null,
    transactionSource: "Individual",
    transactionSourceTypeCode: "TIND",
    transactionSubTypeCode: "ITMY",
    transactionSubTypeDescription: "Monetary Contribution",
    filerTypeCode: "CAN",
    filerTypeDescription: "Candidate",
    electionYear: 2024,
    electionCycle: "2024 General",
    electionId: 35,
    officeId: 19,
    officeType: "OTSTW",
    entityId: 33545,
    reportName: "07/01/2024 - GENERAL",
    candidateFirstName: "PHIL",
    candidateLastName: "SCOTT",
    candidateMiddleName: null,
    occupation: "Attorney",
    employer: "Acme Law",
    filingYear: 2024,
    addressLine1: "1 Main St",
    addressLine2: null,
    city: "Montpelier",
    stateCode: "VT",
    zipCode: "05602",
    ...overrides,
  };
}

describe("vermontDirectContributionAggregator", () => {
  it("aggregates candidate money by contributor source type and contribution size only", () => {
    const sourceUrl = "https://campaignfinance.vermont.gov/";
    const result = aggregateVermontDirectContributions({
      filerRegistrationGuid: "candidate-guid",
      electionYear: 2024,
      sourceUrl,
      contributionRows: [
        contribution({ transactionAmount: 100, occupation: "Attorney" }),
        contribution({
          transactionId: 2,
          guid: "contribution-guid-2",
          sourceName: "ROE, JOHN",
          sourceFirstName: "JOHN",
          sourceLastName: "ROE",
          transactionAmount: 250,
          occupation: "Teacher",
        }),
        contribution({
          transactionId: 3,
          guid: "contribution-guid-3",
          sourceName: "ACME LLC",
          transactionSource: "Business/Group/Organization",
          transactionSourceTypeCode: "TBSN",
          transactionAmount: 5000,
          occupation: "CEO",
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
          categoryType: "contributor_source_type",
          categoryName: "Business/Group/Organization",
          amount: 5000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contributor_source_type",
          categoryName: "Individual",
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

  it("filters wrong filers, wrong years, non-candidate filers, and invalid amounts", () => {
    const result = aggregateVermontDirectContributions({
      filerRegistrationGuid: "candidate-guid",
      electionYear: 2024,
      contributionRows: [
        contribution({ transactionAmount: 500 }),
        contribution({ filerRegistrationGuid: "other-guid", transactionAmount: 1000 }),
        contribution({ electionYear: 2022, transactionAmount: 1000 }),
        contribution({ filerTypeCode: "PAC", filerTypeDescription: "Political Committee", transactionAmount: 1000 }),
        contribution({ transactionAmount: 0 }),
        contribution({ transactionAmount: Number.NaN }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.matchedContributionRowCount).toBe(5);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(4);
  });

  it("counts distinct contributors and limits non-size categories", () => {
    const result = aggregateVermontDirectContributions({
      filerRegistrationGuid: "candidate-guid",
      electionYear: 2024,
      maxBreakdownsPerCategory: 1,
      contributionRows: [
        contribution({ transactionAmount: 600, sourceName: "DOE, JANE", transactionSource: "Individual" }),
        contribution({ transactionId: 2, guid: "row-2", transactionAmount: 200, sourceName: "DOE, JANE", transactionSource: "Individual" }),
        contribution({
          transactionId: 3,
          guid: "row-3",
          transactionAmount: 300,
          sourceName: "ACME LLC",
          transactionSource: "Business/Group/Organization",
          transactionSourceTypeCode: "TBSN",
        }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "contributor_source_type")).toEqual([
      expect.objectContaining({ categoryName: "Individual", amount: 800, contributorCount: 1 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("rejects invalid inputs", () => {
    expect(() =>
      aggregateVermontDirectContributions({
        filerRegistrationGuid: "",
        electionYear: 2024,
        contributionRows: [],
      })
    ).toThrow("Vermont filer registration guid is required");
    expect(() =>
      aggregateVermontDirectContributions({
        filerRegistrationGuid: "candidate-guid",
        electionYear: 1999,
        contributionRows: [],
      })
    ).toThrow("Invalid Vermont direct contribution aggregation election year");
    expect(() =>
      aggregateVermontDirectContributions({
        filerRegistrationGuid: "candidate-guid",
        electionYear: 2024,
        maxBreakdownsPerCategory: 0,
        contributionRows: [],
      })
    ).toThrow("maxBreakdownsPerCategory");
  });
});
