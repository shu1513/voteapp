import { describe, expect, it } from "vitest";

import {
  aggregateFloridaOutsideGroupContributions,
  type FloridaOutsideFinanceGroup,
} from "../../../src/pipeline/floridaFinance/floridaOutsideGroupContributionAggregator.js";
import type { FloridaContributionRow } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceRows.js";

function contribution(overrides: Partial<FloridaContributionRow> = {}): FloridaContributionRow {
  return {
    recipientName: "Floridians for Jane Doe",
    contributionDate: "9/15/2026",
    amount: "25000.00",
    transactionType: "CHE",
    contributorName: "Energy Transfer LLC",
    address: "1 Main St",
    city: "Tallahassee",
    state: "FL",
    zip: "32301",
    occupation: "",
    inKindDescription: "",
    electionCode: "20261103-GEN",
    sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<FloridaOutsideFinanceGroup> = {}): FloridaOutsideFinanceGroup {
  return {
    committeeId: "FLORIDIANS_FOR_JANE_DOE",
    committeeName: "Floridians for Jane Doe",
    supportOppose: "support",
    amount: 100000,
    sourceUrl: "https://dos.elections.myflorida.com/cgi-bin/contrib.exe",
    ...overrides,
  };
}

describe("floridaOutsideGroupContributionAggregator", () => {
  it("backtraces trusted outside groups into donor and industry breakdowns", () => {
    const sourceUrl = "https://dos.elections.myflorida.com/cgi-bin/contrib.exe";
    const result = aggregateFloridaOutsideGroupContributions({
      electionYear: 2026,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution(),
        contribution({
          amount: "10000.00",
          contributorName: "Energy Transfer LLC",
        }),
        contribution({
          amount: "30000.00",
          contributorName: "IBEW Voluntary PAC",
        }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Voluntary PAC",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
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

  it("keeps support and opposition groups separate for the same committee", () => {
    const result = aggregateFloridaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [
        outsideGroup({ supportOppose: "support" }),
        outsideGroup({ supportOppose: "oppose" }),
      ],
      contributionRows: [contribution({ amount: "50000.00" })],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "support",
          categoryType: "donor",
          amount: 50000,
        }),
        expect.objectContaining({
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "oppose",
          categoryType: "donor",
          amount: 50000,
        }),
        expect.objectContaining({
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 50000,
        }),
        expect.objectContaining({
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 50000,
        }),
      ])
    );
  });

  it("returns every donor uncapped, sorted by amount within each outside group and support side", () => {
    // The display cap lives in the SYNC layer, after classification —
    // capping here would drop tail donors from rebuilt industry totals.
    const result = aggregateFloridaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [
        outsideGroup({
          committeeId: "FLORIDIANS_FOR_JANE_DOE",
          committeeName: "Floridians for Jane Doe",
          supportOppose: "support",
        }),
        outsideGroup({
          committeeId: "SUNSHINE_ACCOUNTABILITY_PAC",
          committeeName: "Sunshine Accountability PAC",
          supportOppose: "oppose",
        }),
      ],
      contributionRows: [
        contribution({
          recipientName: "Floridians for Jane Doe",
          amount: "60000.00",
          contributorName: "Energy Transfer LLC",
        }),
        contribution({
          recipientName: "Floridians for Jane Doe",
          amount: "50000.00",
          contributorName: "IBEW Voluntary PAC",
        }),
        contribution({
          recipientName: "Sunshine Accountability PAC",
          amount: "40000.00",
          contributorName: "Sunshine Realty LLC",
        }),
        contribution({
          recipientName: "Sunshine Accountability PAC",
          amount: "30000.00",
          contributorName: "Midland Energy LLC",
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns.filter((row) => row.categoryType === "donor")).toEqual([
      expect.objectContaining({
        committeeId: "FLORIDIANS_FOR_JANE_DOE",
        supportOppose: "support",
        categoryName: "Energy Transfer LLC",
        amount: 60000,
      }),
      expect.objectContaining({
        committeeId: "FLORIDIANS_FOR_JANE_DOE",
        supportOppose: "support",
        categoryName: "IBEW Voluntary PAC",
        amount: 50000,
      }),
      expect.objectContaining({
        committeeId: "SUNSHINE_ACCOUNTABILITY_PAC",
        supportOppose: "oppose",
        categoryName: "Sunshine Realty LLC",
        amount: 40000,
      }),
      expect.objectContaining({
        committeeId: "SUNSHINE_ACCOUNTABILITY_PAC",
        supportOppose: "oppose",
        categoryName: "Midland Energy LLC",
        amount: 30000,
      }),
    ]);
    expect(result.outsideGroupBreakdowns.filter((row) => row.categoryType === "industry")).toEqual([
      expect.objectContaining({
        committeeId: "FLORIDIANS_FOR_JANE_DOE",
        supportOppose: "support",
        categoryName: "oil_gas_energy",
        amount: 60000,
      }),
      expect.objectContaining({
        committeeId: "FLORIDIANS_FOR_JANE_DOE",
        supportOppose: "support",
        categoryName: "labor_unions",
        amount: 50000,
      }),
      expect.objectContaining({
        committeeId: "SUNSHINE_ACCOUNTABILITY_PAC",
        supportOppose: "oppose",
        categoryName: "real_estate",
        amount: 40000,
      }),
      expect.objectContaining({
        committeeId: "SUNSHINE_ACCOUNTABILITY_PAC",
        supportOppose: "oppose",
        categoryName: "oil_gas_energy",
        amount: 30000,
      }),
    ]);
  });

  it("matches alternate committee names and skips invalid donor receipts", () => {
    const result = aggregateFloridaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [
        outsideGroup({
          committeeName: "Floridians for Jane Doe, Inc.",
          committeeNames: ["Floridians for Jane Doe"],
        }),
      ],
      contributionRows: [
        contribution({ amount: "24999.99", contributorName: "Energy Transfer LLC" }),
        contribution({ amount: "0", contributorName: "IBEW Voluntary PAC" }),
        contribution({ transactionType: "INK", amount: "50000", contributorName: "Old Energy Company" }),
        contribution({ contributionDate: "12/31/2024", amount: "50000", contributorName: "Old Energy Company" }),
        contribution({ recipientName: "Other Committee", amount: "50000", contributorName: "Old Energy Company" }),
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

  it("validates inputs", () => {
    expect(() =>
      aggregateFloridaOutsideGroupContributions({
        electionYear: 1995,
        outsideGroups: [],
        contributionRows: [],
      })
    ).toThrow("Invalid Florida outside group contribution election year");
    expect(() =>
      aggregateFloridaOutsideGroupContributions({
        electionYear: 2026,
        outsideGroups: [],
        contributionRows: [],
        minIndustryAmount: -1,
      })
    ).toThrow("Invalid Florida outside group contribution minIndustryAmount");
  });
});
