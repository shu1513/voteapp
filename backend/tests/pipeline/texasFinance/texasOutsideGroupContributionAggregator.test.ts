import { describe, expect, it } from "vitest";

import { aggregateTexasOutsideGroupContributions } from "../../../src/pipeline/texasFinance/texasOutsideGroupContributionAggregator.js";
import type { TexasOutsideSpendingGroup } from "../../../src/pipeline/texasFinance/texasOutsideSpendingAggregator.js";
import type { TexasTecContributionRow } from "../../../src/pipeline/texasFinance/texasTecCsvDatabaseReader.js";

function contribution(overrides: Partial<TexasTecContributionRow> = {}): TexasTecContributionRow {
  return {
    recordType: "CONTRIB",
    formTypeCd: "SPAC",
    schedFormTypeCd: "A1",
    reportInfoIdent: "9001",
    receivedDt: "20261001",
    infoOnlyFlag: "",
    filerIdent: "7001",
    filerTypeCd: "SPAC",
    filerName: "Texans for Example",
    contributionInfoId: "1001",
    contributionDt: "20260915",
    contributionAmount: "25000.00",
    contributionDescr: "",
    contributorPersentTypeCd: "ENTITY",
    contributorNameOrganization: "Energy Transfer LLC",
    contributorNameLast: "",
    contributorNameFirst: "",
    contributorStreetStateCd: "TX",
    contributorEmployer: "",
    contributorOccupation: "",
    contributorJobTitle: "",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<TexasOutsideSpendingGroup> = {}): TexasOutsideSpendingGroup {
  return {
    committeeId: "7001",
    committeeName: "Texans for Example",
    supportOppose: "support",
    amount: 100000,
    sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
    ...overrides,
  };
}

describe("texasOutsideGroupContributionAggregator", () => {
  it("backtraces outside spender contributions into donor and industry breakdowns", () => {
    const sourceUrl = "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip";
    const result = aggregateTexasOutsideGroupContributions({
      electionYear: 2026,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution(),
        contribution({
          contributionInfoId: "1002",
          contributionAmount: "10000.00",
          contributorNameOrganization: "Energy Transfer LLC",
        }),
        contribution({
          contributionInfoId: "1003",
          contributionAmount: "30000.00",
          contributorNameOrganization: "IBEW Voluntary PAC",
        }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Voluntary PAC",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
      ],
    });
  });

  it("keeps support and opposition groups separate for the same spender committee", () => {
    const result = aggregateTexasOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [
        outsideGroup({ supportOppose: "support" }),
        outsideGroup({ supportOppose: "oppose" }),
      ],
      contributionRows: [contribution({ contributionAmount: "50000.00" })],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "donor",
          amount: 50000,
        }),
        expect.objectContaining({
          committeeId: "7001",
          supportOppose: "oppose",
          categoryType: "donor",
          amount: 50000,
        }),
        expect.objectContaining({
          committeeId: "7001",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 50000,
        }),
        expect.objectContaining({
          committeeId: "7001",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 50000,
        }),
      ])
    );
  });

  it("only classifies organization donors above the state threshold", () => {
    const result = aggregateTexasOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({
          contributionInfoId: "T1",
          contributionAmount: "24999.99",
          contributorNameOrganization: "Energy Transfer LLC",
        }),
        contribution({
          contributionInfoId: "T2",
          contributionAmount: "50000.00",
          contributorPersentTypeCd: "INDIVIDUAL",
          contributorNameOrganization: "",
          contributorNameLast: "PERSON",
          contributorNameFirst: "PAT",
        }),
        contribution({
          contributionInfoId: "T3",
          contributionAmount: "50000.00",
          contributorNameOrganization: "Old Energy Company",
          contributionDt: "20241231",
        }),
        contribution({
          contributionInfoId: "T4",
          contributionAmount: "50000.00",
          contributorNameOrganization: "Candidate Committee",
          filerTypeCd: "COH",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(3);
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "donor",
        categoryName: "Energy Transfer LLC",
        amount: 24999.99,
      }),
    ]);
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryType === "industry")).toBe(false);
  });

  it("skips invalid outside donor receipts", () => {
    const result = aggregateTexasOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({ contributionAmount: "0" }),
        contribution({ contributionAmount: "-100" }),
        contribution({ contributionAmount: "bad" }),
        contribution({ infoOnlyFlag: "Y" }),
        contribution({ contributorNameOrganization: "" }),
        contribution({ filerIdent: "OTHER", contributionAmount: "50000" }),
      ],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 5,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 5,
    });
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateTexasOutsideGroupContributions({
        electionYear: 2013,
        outsideGroups: [],
        contributionRows: [],
      })
    ).toThrow("Invalid Texas outside group contribution election year");
    expect(() =>
      aggregateTexasOutsideGroupContributions({
        electionYear: 2026,
        outsideGroups: [],
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Texas outside group contribution maxBreakdownsPerCategory");
    expect(() =>
      aggregateTexasOutsideGroupContributions({
        electionYear: 2026,
        outsideGroups: [],
        contributionRows: [],
        minIndustryAmount: -1,
      })
    ).toThrow("Invalid Texas outside group contribution minIndustryAmount");
  });
});
