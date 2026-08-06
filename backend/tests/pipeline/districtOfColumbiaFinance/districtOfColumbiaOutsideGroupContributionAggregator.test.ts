import { describe, expect, it } from "vitest";

import { aggregateDistrictOfColumbiaOutsideGroupContributions } from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaOutsideGroupContributionAggregator.js";
import type { DistrictOfColumbiaOcfContributionRecord } from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaOcfClient.js";
import type { DistrictOfColumbiaOutsideSpendingGroup } from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaOutsideSpendingAggregator.js";

function contribution(
  overrides: Partial<DistrictOfColumbiaOcfContributionRecord> = {}
): DistrictOfColumbiaOcfContributionRecord {
  return {
    committeeName: "DCCSA IEC",
    committeeKey: "DCCSA IEC",
    contributorName: "Guzman Construction Solutions LLC",
    contributorType: "Business Entity",
    amount: 25_000,
    date: "03/12/2022",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<DistrictOfColumbiaOutsideSpendingGroup> = {}): DistrictOfColumbiaOutsideSpendingGroup {
  return {
    committeeKey: "DCCSA IEC",
    committeeName: "DCCSA IEC",
    supportOppose: "support",
    amount: 100_000,
    sourceUrl: "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV",
    ...overrides,
  };
}

describe("districtOfColumbiaOutsideGroupContributionAggregator", () => {
  it("backtraces outside spender organization contributions into donor and industry breakdowns", () => {
    const sourceUrl = "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV";
    const result = aggregateDistrictOfColumbiaOutsideGroupContributions({
      electionYear: 2022,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRecords: [
        contribution(),
        contribution({ amount: 10_000, contributorName: "Guzman Construction Solutions LLC" }),
        contribution({
          contributorName: "IBEW Voluntary PAC",
          contributorType: "Political Committee",
          amount: 30_000,
        }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeKey: "DCCSA IEC",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Guzman Construction Solutions LLC",
          amount: 35_000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeKey: "DCCSA IEC",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Voluntary PAC",
          amount: 30_000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeKey: "DCCSA IEC",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "construction",
          amount: 35_000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeKey: "DCCSA IEC",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 30_000,
          contributorCount: 1,
          sourceUrl,
        },
      ],
    });
  });

  it("duplicates outside donor totals across support and oppose groups for the same committee", () => {
    const result = aggregateDistrictOfColumbiaOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose" })],
      contributionRecords: [contribution({ amount: 50_000 })],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ committeeKey: "DCCSA IEC", supportOppose: "support", categoryType: "donor", amount: 50_000 }),
        expect.objectContaining({ committeeKey: "DCCSA IEC", supportOppose: "oppose", categoryType: "donor", amount: 50_000 }),
        expect.objectContaining({ committeeKey: "DCCSA IEC", supportOppose: "support", categoryType: "industry", amount: 50_000 }),
        expect.objectContaining({ committeeKey: "DCCSA IEC", supportOppose: "oppose", categoryType: "industry", amount: 50_000 }),
      ])
    );
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
  });

  it("only classifies organization donors above the state threshold", () => {
    const result = aggregateDistrictOfColumbiaOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      contributionRecords: [
        contribution({ amount: 24_999.99, contributorName: "Guzman Construction Solutions LLC" }),
        contribution({ amount: 50_000, contributorName: "Pat Person", contributorType: "Individual" }),
        contribution({ amount: 50_000, contributorName: "Old Construction Company", date: "12/31/2020" }),
        contribution({ amount: 50_000, contributorName: "Unknown Foundation", contributorType: "" }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(2);
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "donor",
        categoryName: "Unknown Foundation",
        amount: 50_000,
      }),
      expect.objectContaining({
        categoryType: "donor",
        categoryName: "Guzman Construction Solutions LLC",
        amount: 24_999.99,
      }),
    ]);
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryType === "industry")).toBe(false);
  });

  it("matches committee keys case-insensitively and can derive them from committee name", () => {
    const result = aggregateDistrictOfColumbiaOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup({ committeeKey: " dccsa iec " })],
      contributionRecords: [
        contribution({ committeeKey: undefined, committeeName: "DCCSA IEC", amount: 25_000 }),
        contribution({ committeeKey: "OTHER", committeeName: "Other IEC", amount: 90_000 }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeKey: "DCCSA IEC",
          categoryType: "donor",
          categoryName: "Guzman Construction Solutions LLC",
          amount: 25_000,
        }),
        expect.objectContaining({
          committeeKey: "DCCSA IEC",
          categoryType: "industry",
          categoryName: "construction",
          amount: 25_000,
        }),
      ])
    );
  });

  it("skips bad amount, missing contributor, non-cycle, and individual rows", () => {
    const result = aggregateDistrictOfColumbiaOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      contributionRecords: [
        contribution({ amount: 0 }),
        contribution({ amount: -10 }),
        contribution({ amount: Number.NaN }),
        contribution({ contributorName: "" }),
        contribution({ date: undefined, electionYear: undefined }),
        contribution({ contributorName: "Jane Doe", contributorType: "Person" }),
        contribution({ amount: 25_000 }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(7);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(6);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "donor", amount: 25_000 }),
        expect.objectContaining({ categoryType: "industry", amount: 25_000 }),
      ])
    );
  });

  it("uses explicit election_year when present", () => {
    const result = aggregateDistrictOfColumbiaOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      contributionRecords: [
        contribution({ date: "01/01/2020", electionYear: 2022, amount: 100 }),
        contribution({ date: "01/01/2022", electionYear: 2020, amount: 200 }),
        contribution({ date: undefined, electionYear: 2022, amount: 300 }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([expect.objectContaining({ categoryType: "donor", amount: 400 })])
    );
  });

  it("returns every donor uncapped, sorted by amount", () => {
    // The display cap lives in the SYNC layer, after classification —
    // capping here would drop tail donors from rebuilt industry totals.
    const result = aggregateDistrictOfColumbiaOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      contributionRecords: Array.from({ length: 4 }, (_, index) =>
        contribution({ contributorName: `Donor ${index} LLC`, amount: (index + 1) * 1_000 })
      ),
    });

    const donors = result.outsideGroupBreakdowns.filter((breakdown) => breakdown.categoryType === "donor");
    expect(donors.map((donor) => donor.categoryName)).toEqual([
      "Donor 3 LLC",
      "Donor 2 LLC",
      "Donor 1 LLC",
      "Donor 0 LLC",
    ]);
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateDistrictOfColumbiaOutsideGroupContributions({
        electionYear: 1999,
        outsideGroups: [],
        contributionRecords: [],
      })
    ).toThrow("Invalid D.C. outside group contribution election year");
    expect(() =>
      aggregateDistrictOfColumbiaOutsideGroupContributions({
        electionYear: 2022,
        outsideGroups: [],
        contributionRecords: [],
        minIndustryAmount: -1,
      })
    ).toThrow("Invalid D.C. outside group contribution minIndustryAmount");
  });
});
