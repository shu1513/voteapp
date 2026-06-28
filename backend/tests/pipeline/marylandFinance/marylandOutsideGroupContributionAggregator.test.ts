import { describe, expect, it } from "vitest";

import { aggregateMarylandOutsideGroupContributions } from "../../../src/pipeline/marylandFinance/marylandOutsideGroupContributionAggregator.js";
import type { MarylandCfsContributionRow } from "../../../src/pipeline/marylandFinance/marylandCfsArtifactReader.js";
import type { MarylandFinanceOutsideGroupInput } from "../../../src/pipeline/marylandFinance/marylandFinanceWriter.js";

function contribution(overrides: Partial<MarylandCfsContributionRow> = {}): MarylandCfsContributionRow {
  return {
    "Filing Entity Id": "16020184",
    "Committee Name": "Momentum Maryland PAC",
    "Abbreviated Committee Name": "Momentum MD",
    "Committee Type": "Political Action Committee",
    "Contributor Type": "Business Entity",
    "Contributor Company Name": "Old Construction Company LLC",
    "Contributor Last Name": "",
    "Contributor First Name": "",
    "Contributor Middle Name": "",
    "Contributor Mailing Address1": "100 Main St",
    "Contributor Mailing Address2": "",
    "Contributor City": "Annapolis",
    "Contributor State": "MD",
    "Contributor ZipCode": '="21401"',
    "Contributor County Of Residence": "Anne Arundel",
    "Transaction Type": "Contribution",
    "Transaction Date": "02/10/2026",
    "Transaction Amount": "$25,000.00",
    "Payment Type": "Check",
    "Fund Type": "Electoral",
    "Number Of People Purchasing Or Making Contributions": "",
    "Price Per Person Or Average Contribution": "",
    "Coordinated In-Kind": "False",
    "Public Funding Requested": "False",
    "Amount Eligible For Public Funding": "$0.00",
    Description: "",
    "Report Name": "2026 Pre-Primary",
    "Aggregate As Of Download Date": "$25,000.00",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<MarylandFinanceOutsideGroupInput> = {}): MarylandFinanceOutsideGroupInput {
  return {
    committeeId: "16020184",
    committeeName: "Momentum Maryland PAC",
    supportOppose: "support",
    amount: 75000,
    sourceUrl: "https://campaignfinance.maryland.gov/public/cf/downloads",
    ...overrides,
  };
}

describe("marylandOutsideGroupContributionAggregator", () => {
  it("backtraces outside spender contributions into organization donor and industry breakdowns", () => {
    const sourceUrl = "https://campaignfinance.maryland.gov/public/cf/downloads";
    const result = aggregateMarylandOutsideGroupContributions({
      electionYear: 2026,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution(),
        contribution({
          "Transaction Amount": "$10,000.00",
          "Contributor Company Name": "Old Construction Company LLC",
        }),
        contribution({
          "Transaction Amount": "$30,000.00",
          "Contributor Company Name": "IBEW Local 26 PAC",
          "Contributor Type": "Political Committee",
        }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeId: "16020184",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Old Construction Company LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "16020184",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Local 26 PAC",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "16020184",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "construction",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "16020184",
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

  it("uses organization-like last names only when contributor type is organizational", () => {
    const result = aggregateMarylandOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({
          "Contributor Company Name": "",
          "Contributor Last Name": "Maryland Real Estate PAC",
          "Contributor Type": "Political Committee",
          "Transaction Amount": "$5,000.00",
        }),
        contribution({
          "Contributor Company Name": "",
          "Contributor Last Name": "Public",
          "Contributor First Name": "Pat",
          "Contributor Type": "Individual",
          "Transaction Amount": "$5,000.00",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(2);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          categoryType: "donor",
          categoryName: "Maryland Real Estate PAC",
          amount: 5000,
        }),
        expect.objectContaining({
          categoryType: "industry",
          categoryName: "real_estate",
          amount: 5000,
        }),
      ])
    );
  });

  it("applies one outside group committee's donors to each support/opposition target for that group", () => {
    const result = aggregateMarylandOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose", amount: 2000 })],
      contributionRows: [contribution({ "Transaction Amount": "$1,000.00" })],
    });

    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeId: "16020184",
          supportOppose: "support",
          categoryType: "donor",
          amount: 1000,
        }),
        expect.objectContaining({
          committeeId: "16020184",
          supportOppose: "oppose",
          categoryType: "donor",
          amount: 1000,
        }),
      ])
    );
  });

  it("respects max breakdowns per outside group and minimum industry amount", () => {
    const result = aggregateMarylandOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      maxBreakdownsPerCategory: 1,
      minIndustryAmount: 10000,
      contributionRows: [
        contribution({
          "Contributor Company Name": "Old Construction Company LLC",
          "Transaction Amount": "$9,999.99",
        }),
        contribution({
          "Contributor Company Name": "IBEW Local 26 PAC",
          "Contributor Type": "Political Committee",
          "Transaction Amount": "$12,000.00",
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "donor",
        categoryName: "IBEW Local 26 PAC",
        amount: 12000,
      }),
      expect.objectContaining({
        categoryType: "industry",
        categoryName: "labor_unions",
        amount: 12000,
      }),
    ]);
  });

  it("skips malformed, nonpositive, non-donor, individual-only, wrong-year, and wrong-committee rows", () => {
    const result = aggregateMarylandOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({ "Transaction Amount": "not money" }),
        contribution({ "Transaction Amount": "$0.00" }),
        contribution({ "Transaction Amount": "($10.00)" }),
        contribution({ "Transaction Type": "Loan" }),
        contribution({
          "Contributor Company Name": "",
          "Contributor Last Name": "Person",
          "Contributor First Name": "Pat",
          "Contributor Type": "Individual",
        }),
        contribution({ "Transaction Date": "12/31/2024" }),
        contribution({ "Filing Entity Id": "999999" }),
        contribution({ "Transaction Amount": "$250.00" }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(7);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(6);
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "donor",
        amount: 250,
      }),
      expect.objectContaining({
        categoryType: "industry",
        amount: 250,
      }),
    ]);
  });

  it("handles empty outside groups and validates inputs", () => {
    expect(
      aggregateMarylandOutsideGroupContributions({
        electionYear: 2026,
        outsideGroups: [],
        contributionRows: [contribution()],
      })
    ).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    });

    expect(() =>
      aggregateMarylandOutsideGroupContributions({
        electionYear: 1999,
        outsideGroups: [],
        contributionRows: [],
      })
    ).toThrow("Invalid Maryland outside group contribution election year");
    expect(() =>
      aggregateMarylandOutsideGroupContributions({
        electionYear: 2026,
        outsideGroups: [],
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Maryland outside group contribution maxBreakdownsPerCategory");
    expect(() =>
      aggregateMarylandOutsideGroupContributions({
        electionYear: 2026,
        outsideGroups: [],
        contributionRows: [],
        minIndustryAmount: -1,
      })
    ).toThrow("Invalid Maryland outside group contribution minIndustryAmount");
  });
});
