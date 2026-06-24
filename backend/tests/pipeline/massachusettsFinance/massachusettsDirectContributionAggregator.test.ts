import { describe, expect, it } from "vitest";

import {
  aggregateMassachusettsDirectContributions,
  isMassachusettsDirectDonorSupportReceipt,
  isMassachusettsTotalReceipt,
} from "../../../src/pipeline/massachusettsFinance/massachusettsDirectContributionAggregator.js";
import type { MassachusettsOcpfContributionItem } from "../../../src/pipeline/massachusettsFinance/massachusettsOcpfClient.js";

function item(overrides: Partial<MassachusettsOcpfContributionItem> = {}): MassachusettsOcpfContributionItem {
  return {
    itemId: "4406595",
    reportId: 812510,
    cpfId: "15710",
    filerName: "Healey, Maura T.",
    contributorName: "Doe, Jane",
    contributorType: "Individual",
    occupation: "Attorney",
    employer: "Law Firm",
    recordTypeDescription: "Individual",
    amount: 250,
    date: "1/3/2022",
    sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=812510",
    ...overrides,
  };
}

describe("massachusettsDirectContributionAggregator", () => {
  it("aggregates positive individual OCPF rows by occupation and contribution size", () => {
    const sourceUrl = "https://www.ocpf.us/Reports/SearchItems?cpfId=15710";
    const result = aggregateMassachusettsDirectContributions({
      candidateCpfId: "15710",
      electionYear: 2022,
      sourceUrl,
      contributionItems: [
        item({ amount: 100, occupation: "Attorney" }),
        item({ itemId: "2", contributorName: "Roe, John", amount: 250, occupation: "Attorney" }),
        item({ itemId: "3", contributorName: "Smith, Pat", amount: 5_000, occupation: "Teacher" }),
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

  it("separates total receipts from direct individual donor support", () => {
    const result = aggregateMassachusettsDirectContributions({
      candidateCpfId: "15710",
      electionYear: 2022,
      contributionItems: [
        item({ amount: 500, recordTypeDescription: "Individual", occupation: "Attorney" }),
        item({ itemId: "committee", amount: 1_000, recordTypeDescription: "Committee", occupation: "" }),
        item({ itemId: "unitemized", amount: 250, recordTypeDescription: "Aggregated Unitemized Receipts", occupation: "" }),
        item({ itemId: "refund", amount: -50, recordTypeDescription: "Individual", occupation: "Attorney" }),
      ],
    });

    expect(result.summary).toEqual({ totalReceipts: 1750, directContributionTotal: 500, sourceUrl: null });
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(3);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 500 }),
      ])
    );
    expect(result.directBreakdowns.some((row) => row.categoryName === "Committee")).toBe(false);
  });

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateMassachusettsDirectContributions({
      candidateCpfId: "15710",
      electionYear: 2022,
      contributionItems: [
        item({ itemId: "1", contributorName: "Doe, Jane", amount: 100, occupation: "Attorney" }),
        item({ itemId: "2", contributorName: "Doe, Jane", amount: 200, occupation: "Attorney" }),
        item({ itemId: "3", contributorName: "Roe, John", amount: 300, occupation: "Attorney" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Attorney", amount: 600, contributorCount: 2 }),
    ]);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "contribution_size", categoryName: "$100-$249", amount: 300 }),
        expect.objectContaining({ categoryType: "contribution_size", categoryName: "$250-$499", amount: 300 }),
      ])
    );
  });

  it("matches candidate CPF and election year strictly", () => {
    const result = aggregateMassachusettsDirectContributions({
      candidateCpfId: "15710",
      electionYear: 2022,
      contributionItems: [
        item({ cpfId: "15710", date: "12/31/2021", amount: 100 }),
        item({ cpfId: "15710", date: "1/1/2022", amount: 200 }),
        item({ cpfId: "15710", date: "2022-11-01", amount: 300 }),
        item({ cpfId: "15710", date: "1/1/2023", amount: 400 }),
        item({ cpfId: "99999", date: "1/1/2022", amount: 900 }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.summary.directContributionTotal).toBe(500);
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(2);
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateMassachusettsDirectContributions({
      candidateCpfId: "15710",
      electionYear: 2022,
      contributionItems: [item({ amount: 0.1, occupation: "Engineer" }), item({ amount: 0.2, occupation: "Engineer" })],
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

  it("does not emit employer breakdowns", () => {
    const result = aggregateMassachusettsDirectContributions({
      candidateCpfId: "15710",
      electionYear: 2022,
      contributionItems: [item({ amount: 300, employer: "Acme Law", occupation: "Attorney" })],
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

  it("limits occupation breakdowns without dropping contribution-size buckets", () => {
    const result = aggregateMassachusettsDirectContributions({
      candidateCpfId: "15710",
      electionYear: 2022,
      maxBreakdownsPerCategory: 1,
      contributionItems: [
        item({ occupation: "Engineer", amount: 100 }),
        item({ occupation: "Teacher", amount: 300 }),
        item({ occupation: "Doctor", amount: 600 }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Doctor", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("classifies only positive individual same-year CPF rows as direct donor support", () => {
    const valid = item({ amount: 250, date: "01/10/2022", recordTypeDescription: "Individual" });
    expect(isMassachusettsTotalReceipt({ item: valid, candidateCpfId: "15710", electionYear: 2022 })).toBe(true);
    expect(isMassachusettsDirectDonorSupportReceipt({ item: valid, candidateCpfId: "15710", electionYear: 2022 })).toBe(true);
    expect(
      isMassachusettsDirectDonorSupportReceipt({
        item: item({ amount: 250, recordTypeDescription: "Committee" }),
        candidateCpfId: "15710",
        electionYear: 2022,
      })
    ).toBe(false);
    expect(
      isMassachusettsTotalReceipt({ item: item({ amount: -10 }), candidateCpfId: "15710", electionYear: 2022 })
    ).toBe(false);
    expect(
      isMassachusettsTotalReceipt({ item: item({ cpfId: "99999" }), candidateCpfId: "15710", electionYear: 2022 })
    ).toBe(false);
  });

  it("validates required aggregation inputs", () => {
    expect(() =>
      aggregateMassachusettsDirectContributions({ candidateCpfId: " ", electionYear: 2022, contributionItems: [] })
    ).toThrow("Massachusetts candidate CPF ID is required");
    expect(() =>
      aggregateMassachusettsDirectContributions({ candidateCpfId: "15710", electionYear: 1999, contributionItems: [] })
    ).toThrow("Invalid Massachusetts direct contribution aggregation election year");
    expect(() =>
      aggregateMassachusettsDirectContributions({
        candidateCpfId: "15710",
        electionYear: 2022,
        contributionItems: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("maxBreakdownsPerCategory");
  });
});
