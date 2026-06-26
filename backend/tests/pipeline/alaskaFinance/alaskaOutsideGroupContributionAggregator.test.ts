import { describe, expect, it } from "vitest";

import {
  aggregateAlaskaOutsideGroupContributions,
  classifyAlaskaOutsideGroupContributionRow,
} from "../../../src/pipeline/alaskaFinance/alaskaOutsideGroupContributionAggregator.js";
import type { AlaskaApocIndependentContributionRow } from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";
import type { AlaskaOutsideSpendingGroup } from "../../../src/pipeline/alaskaFinance/alaskaOutsideSpendingAggregator.js";

function contribution(overrides: Partial<AlaskaApocIndependentContributionRow> = {}): AlaskaApocIndependentContributionRow {
  return {
    reportYear: 2026,
    filerId: "8001",
    filerName: "Alaska Future PAC",
    filerType: "Group",
    businessPhone: "907-555-0100",
    businessType: "Super PAC",
    type: "Contribution",
    date: "09/01/2026",
    contributor: "Energy Transfer LLC",
    contributorAddress: "2 Energy Rd",
    contributorCity: "Dallas",
    contributorState: "TX",
    contributorZip: "75001",
    contributorCountry: "USA",
    employer: "",
    occupation: "",
    reportType: "24-hour",
    election: "General",
    officers: "",
    amount: 30_000,
    submitted: "09/02/2026",
    status: "Complete",
    sourceUrl: "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEContributions.aspx",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<AlaskaOutsideSpendingGroup> = {}): AlaskaOutsideSpendingGroup {
  return {
    committeeId: "8001",
    committeeName: "Alaska Future PAC",
    supportOppose: "support",
    amount: 50_000,
    sourceUrl: "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEExpenditures.aspx",
    ...overrides,
  };
}

describe("alaskaOutsideGroupContributionAggregator", () => {
  it("backtraces APOC outside spender contributions into donor and industry breakdowns", () => {
    const sourceUrl = "https://aws.state.ak.us/ApocReports/IndependentExpenditures/IEContributions.aspx";
    const result = aggregateAlaskaOutsideGroupContributions({
      electionYear: 2026,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution(),
        contribution({ contributor: "Energy Transfer LLC", amount: 10_000 }),
        contribution({ contributor: "IBEW Voluntary PAC", amount: 35_000 }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeId: "8001",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 40000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "8001",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Voluntary PAC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "8001",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 40000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "8001",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
      ],
    });
  });

  it("uses employer or occupation classification when donor name is not classifiable", () => {
    expect(
      classifyAlaskaOutsideGroupContributionRow({
        contributor: "Pat Smith",
        employer: "North Slope Energy",
        occupation: "Executive",
      }).industrySlug
    ).toBe("oil_gas_energy");
    expect(
      classifyAlaskaOutsideGroupContributionRow({
        contributor: "Alex Roe",
        employer: "",
        occupation: "Attorney",
      }).industrySlug
    ).toBe("lawyers_and_legal_services");
  });

  it("keeps support and opposition groups separate for the same spender", () => {
    const result = aggregateAlaskaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose" })],
      contributionRows: [contribution({ amount: 50_000 })],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ supportOppose: "support", categoryType: "donor", amount: 50000 }),
        expect.objectContaining({ supportOppose: "oppose", categoryType: "donor", amount: 50000 }),
        expect.objectContaining({ supportOppose: "support", categoryType: "industry", amount: 50000 }),
        expect.objectContaining({ supportOppose: "oppose", categoryType: "industry", amount: 50000 }),
      ])
    );
  });

  it("skips invalid rows and only classifies industries above the threshold", () => {
    const result = aggregateAlaskaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({ amount: 24_999.99 }),
        contribution({ amount: 50_000, date: "01/01/2024", reportYear: 2024 }),
        contribution({ amount: 50_000, status: "Rejected" }),
        contribution({ amount: 50_000, contributor: "" }),
        contribution({ amount: 50_000, filerId: "OTHER" }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(3);
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({ categoryType: "donor", categoryName: "Energy Transfer LLC", amount: 24999.99 }),
    ]);
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryType === "industry")).toBe(false);
  });

  it("classifies industries from included contribution rows only", () => {
    const result = aggregateAlaskaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({
          contributor: "Pat Smith",
          employer: "North Slope Energy",
          occupation: "Executive",
          amount: 50_000,
          date: "01/01/2024",
          reportYear: 2024,
        }),
        contribution({
          contributor: "Pat Smith",
          employer: "",
          occupation: "Attorney",
          amount: 30_000,
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          categoryType: "industry",
          categoryName: "lawyers_and_legal_services",
          amount: 30000,
        }),
      ])
    );
    expect(
      result.outsideGroupBreakdowns.some(
        (row) => row.categoryType === "industry" && row.categoryName === "oil_gas_energy"
      )
    ).toBe(false);
  });
});
