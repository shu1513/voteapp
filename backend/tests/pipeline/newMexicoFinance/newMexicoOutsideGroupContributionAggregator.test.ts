import { describe, expect, it } from "vitest";

import { aggregateNewMexicoOutsideGroupContributions } from "../../../src/pipeline/newMexicoFinance/newMexicoOutsideGroupContributionAggregator.js";
import type { NewMexicoCfisContributionRow } from "../../../src/pipeline/newMexicoFinance/newMexicoCfisArtifactReader.js";
import type { NewMexicoFinanceOutsideGroupInput } from "../../../src/pipeline/newMexicoFinance/newMexicoFinanceWriter.js";

function contribution(overrides: Partial<NewMexicoCfisContributionRow> = {}): NewMexicoCfisContributionRow {
  return {
    OrgID: "9001",
    "Transaction Amount": "25000.00",
    "Transaction Date": "03/12/2026",
    "Last Name": "Guzman Construction Solutions LLC",
    "First Name": "",
    "Middle Name": "",
    Prefix: "",
    Suffix: "",
    "Contributor Address Line 1": "",
    "Contributor Address Line 2": "",
    "Contributor City": "Albuquerque",
    "Contributor State": "NM",
    "Contributor Zip Code": "87101",
    Description: "",
    "Check Number": "",
    "Transaction ID": "T1",
    "Filed Date": "03/20/2026",
    Election: "2026 General",
    "Report Name": "First Report",
    "Start of Period": "03/01/2026",
    "End of Period": "03/31/2026",
    "Contributor Code": "Other (e.g. business entity)",
    "Contribution Type": "Contributions - Monetary",
    "Report Entity Type": "PAC - Independent Expenditure",
    "Committee Name": "Accountable New Mexico",
    "Candidate Last Name": "",
    "Candidate First Name": "",
    "Candidate Middle Name": "",
    "Candidate Prefix": "",
    "Candidate Suffix": "",
    Amended: "",
    "Contributor Employer": "",
    "Contributor Occupation": "",
    "Occupation Comment": "",
    "Employment Information Requested": "",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<NewMexicoFinanceOutsideGroupInput> = {}): NewMexicoFinanceOutsideGroupInput {
  return {
    committeeId: "9001",
    committeeName: "Accountable New Mexico",
    supportOppose: "oppose",
    amount: 100000,
    sourceUrl: "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?transactionType=EXP",
    ...overrides,
  };
}

describe("newMexicoOutsideGroupContributionAggregator", () => {
  it("backtraces outside spender contributions into donor and industry breakdowns", () => {
    const sourceUrl =
      "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=CON";
    const result = aggregateNewMexicoOutsideGroupContributions({
      electionYear: 2026,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution(),
        contribution({
          "Transaction ID": "T2",
          "Transaction Amount": "10000.00",
          "Last Name": "Guzman Construction Solutions LLC",
        }),
        contribution({
          "Transaction ID": "T3",
          "Transaction Amount": "30000.00",
          "Last Name": "IBEW Voluntary PAC",
          "Contributor Code": "Political Committee",
        }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeId: "9001",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Guzman Construction Solutions LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "9001",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "IBEW Voluntary PAC",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "9001",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "construction",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "9001",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
      ],
    });
  });

  it("returns every donor row uncapped so the sync can classify all of them", () => {
    const donorCount = 60;
    const result = aggregateNewMexicoOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      contributionRows: Array.from({ length: donorCount }, (_unused, index) =>
        contribution({
          "Transaction ID": `T${index}`,
          "Transaction Amount": `${100_000 - index * 100}.00`,
          "Last Name": `Guzman Construction Solutions ${index} LLC`,
        })
      ),
    });

    const donors = result.outsideGroupBreakdowns.filter((row) => row.categoryType === "donor");
    expect(donors).toHaveLength(donorCount);
    expect(donors[0]?.amount).toBe(100_000);
    expect(donors.at(-1)?.amount).toBe(100_000 - (donorCount - 1) * 100);
  });

  it("only classifies organization donors above the state threshold", () => {
    const result = aggregateNewMexicoOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({
          "Transaction ID": "T1",
          "Transaction Amount": "24999.99",
          "Last Name": "Guzman Construction Solutions LLC",
        }),
        contribution({
          "Transaction ID": "T2",
          "Transaction Amount": "50000.00",
          "Last Name": "Person",
          "First Name": "Pat",
          "Contributor Code": "Individual",
        }),
        contribution({
          "Transaction ID": "T3",
          "Transaction Amount": "50000.00",
          "Last Name": "Old Construction Company",
          "Transaction Date": "12/31/2024",
        }),
        contribution({
          "Transaction ID": "T4",
          "Transaction Amount": "50000.00",
          "Last Name": "Candidate Committee",
          "Report Entity Type": "Candidate",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(3);
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "donor",
        categoryName: "Guzman Construction Solutions LLC",
        amount: 24999.99,
      }),
    ]);
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryType === "industry")).toBe(false);
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateNewMexicoOutsideGroupContributions({
        electionYear: 2019,
        outsideGroups: [],
        contributionRows: [],
      })
    ).toThrow("Invalid New Mexico outside group contribution election year");
    expect(() =>
      aggregateNewMexicoOutsideGroupContributions({
        electionYear: 2026,
        outsideGroups: [],
        contributionRows: [],
        minIndustryAmount: -1,
      })
    ).toThrow("Invalid New Mexico outside group contribution minIndustryAmount");
  });
});
