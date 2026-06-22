import { describe, expect, it } from "vitest";

import { aggregateOklahomaOutsideGroupContributions } from "../../../src/pipeline/oklahomaFinance/oklahomaOutsideGroupContributionAggregator.js";
import type { OklahomaGuardianContributionRow } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianContributionReader.js";

function contribution(overrides: Partial<OklahomaGuardianContributionRow> = {}): OklahomaGuardianContributionRow {
  return {
    "Receipt ID": "R1",
    "Org ID": "99999",
    "Receipt Type": "Contribution",
    "Receipt Date": "06/01/2022",
    "Receipt Amount": "100000.00",
    Description: "",
    "Receipt Source Type": "Business",
    "Last Name": "Energy Transfer",
    "First Name": "",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Oklahoma City",
    State: "OK",
    Zip: "73102",
    "Filed Date": "06/10/2022",
    "Committee Type": "Independent Expenditure Committee",
    "Committee Name": "THE OKLAHOMA PROJECT",
    "Candidate Name": "",
    Amended: "",
    Employer: "",
    Occupation: "",
    ...overrides,
  };
}

describe("Oklahoma outside group contribution aggregator", () => {
  it("backtraces organization donors to exact-matched outside groups and industries", () => {
    const result = aggregateOklahomaOutsideGroupContributions({
      electionYear: 2022,
      sourceUrl: "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2022_ContributionLoanExtract.csv.zip",
      outsideGroups: [
        {
          committeeId: "THE OKLAHOMA PROJECT",
          committeeName: "The Oklahoma Project",
          supportOppose: "oppose",
          amount: 61597.12,
        },
      ],
      contributionRows: [
        contribution({ "Receipt Amount": "100000.00", "Last Name": "Energy Transfer" }),
        contribution({
          "Receipt ID": "R2",
          "Receipt Amount": "50000.00",
          "Last Name": "Continental Resources",
        }),
        contribution({
          "Receipt ID": "R3",
          "Receipt Amount": "250.00",
          "Receipt Source Type": "Individual",
          "Last Name": "Doe",
          "First Name": "Jane",
        }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 2,
      skippedContributionRowCount: 1,
      outsideGroupBreakdowns: [
        {
          committeeId: "THE OKLAHOMA PROJECT",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Energy Transfer",
          amount: 100000,
          contributorCount: 1,
          sourceUrl: "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2022_ContributionLoanExtract.csv.zip",
        },
        {
          committeeId: "THE OKLAHOMA PROJECT",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Continental Resources",
          amount: 50000,
          contributorCount: 1,
          sourceUrl: "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2022_ContributionLoanExtract.csv.zip",
        },
        {
          committeeId: "THE OKLAHOMA PROJECT",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 150000,
          contributorCount: 2,
          sourceUrl: "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2022_ContributionLoanExtract.csv.zip",
        },
      ],
    });
  });

  it("keeps support and opposition groups separate for the same spender", () => {
    const result = aggregateOklahomaOutsideGroupContributions({
      electionYear: 2022,
      minIndustryAmount: 0,
      outsideGroups: [
        {
          committeeId: "EXAMPLE PAC",
          committeeName: "Example PAC",
          supportOppose: "support",
          amount: 10,
        },
        {
          committeeId: "EXAMPLE PAC",
          committeeName: "Example PAC",
          supportOppose: "oppose",
          amount: 20,
        },
      ],
      contributionRows: [contribution({ "Committee Name": "Example PAC", "Last Name": "Google", "Receipt Amount": "500" })],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ committeeId: "EXAMPLE PAC", supportOppose: "support", categoryType: "donor" }),
        expect.objectContaining({ committeeId: "EXAMPLE PAC", supportOppose: "oppose", categoryType: "donor" }),
        expect.objectContaining({ committeeId: "EXAMPLE PAC", supportOppose: "support", categoryType: "industry" }),
        expect.objectContaining({ committeeId: "EXAMPLE PAC", supportOppose: "oppose", categoryType: "industry" }),
      ])
    );
  });

  it("requires exact normalized outside committee-name matches", () => {
    const result = aggregateOklahomaOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [
        {
          committeeId: "THE OKLAHOMA PROJECT",
          committeeName: "The Oklahoma Project",
          supportOppose: "oppose",
          amount: 10,
        },
      ],
      contributionRows: [
        contribution({ "Committee Name": "Different Oklahoma Project", "Last Name": "Energy Transfer" }),
      ],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    });
  });

  it("preserves committee-name identity words when matching outside groups", () => {
    const result = aggregateOklahomaOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [
        {
          committeeId: "THE OKLAHOMA PROJECT",
          committeeName: "The Oklahoma Project",
          supportOppose: "oppose",
          amount: 10,
        },
      ],
      contributionRows: [contribution({ "Committee Name": "Oklahoma Project", "Last Name": "Energy Transfer" })],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    });
  });

  it("validates options", () => {
    expect(() =>
      aggregateOklahomaOutsideGroupContributions({
        electionYear: 2013,
        outsideGroups: [],
        contributionRows: [],
      })
    ).toThrow("Invalid Oklahoma outside group contribution election year");
    expect(() =>
      aggregateOklahomaOutsideGroupContributions({
        electionYear: 2022,
        maxBreakdownsPerCategory: 0,
        outsideGroups: [],
        contributionRows: [],
      })
    ).toThrow("Invalid Oklahoma outside group contribution maxBreakdownsPerCategory");
  });
});
