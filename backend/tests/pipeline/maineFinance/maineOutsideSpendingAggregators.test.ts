import { describe, expect, it } from "vitest";

import { aggregateMaineOutsideGroupContributions } from "../../../src/pipeline/maineFinance/maineOutsideGroupContributionAggregator.js";
import { normalizeMaineCandidateNameKeys } from "../../../src/pipeline/maineFinance/maineCandidateCommitteeResolver.js";
import {
  aggregateMaineOutsideSpending,
  type MaineOutsideSpendingGroup,
} from "../../../src/pipeline/maineFinance/maineOutsideSpendingAggregator.js";
import type {
  MaineCfisContributionRow,
  MaineCfisExpenditureRow,
} from "../../../src/pipeline/maineFinance/maineCfisArtifactReader.js";

function expenditure(overrides: Partial<MaineCfisExpenditureRow> = {}): MaineCfisExpenditureRow {
  return {
    "Election Year": "2024",
    OrgID: "242",
    LegacyID: "611",
    "Committee Type": "Political Action Committee",
    "Committee Name": "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
    "Candidate Name": "",
    Jurisdiction: "STATE",
    Office: "",
    District: "",
    Party: "",
    IncumbentStatus: "",
    "Financing Type": "",
    "Payee Last Name": "MEDIA VENDOR LLC",
    "Payee First Name": "",
    "Payee Middle Name": "",
    Suffix: "",
    Address1: "100 Main St",
    Address2: "",
    City: "Augusta",
    State: "ME",
    Zip: "04330",
    "Expenditure ID": "E-1",
    "Expenditure Date": "10/03/2024",
    "Expenditure Purpose": "Independent Expenditure",
    "Expenditure Amount": "1582.5000",
    Explanation: "Digital ads",
    "Date Filed": "10/04/2024",
    Amended: "N",
    "IE Report": "Y",
    "24-Hour Report": "Y",
    "Report Name": "2024 24-Hour IE",
    "Operating Expense": "N",
    "Support/Oppose Ballot Question": "",
    "Support/Oppose Candidate": "Support",
    "Ballot Question Number": "",
    "Ballot Question Description/Title": "",
    Candidate: "Paul, Reagan LeeAnn",
    "Candidate ID": "481737",
    "Candidate Jurisdiction": "STATE",
    "Candidate Office": "Representative",
    "Candidate District": "37",
    "Candidate Party": "Republican",
    "Candidate IncumbentStatus": "",
    "Candidate Financing Type": "",
    ...overrides,
  };
}

function contribution(overrides: Partial<MaineCfisContributionRow> = {}): MaineCfisContributionRow {
  return {
    OrgID: "242",
    LegacyID: "611",
    "Committee Name": "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
    "Candidate Name": "",
    "Receipt Amount": "25000.0000",
    "Receipt Date": "03/11/2024",
    Office: "",
    District: "",
    "Last Name": "OLD CONSTRUCTION COMPANY LLC",
    "First Name": "",
    "Middle Name": "",
    Suffix: "",
    Address1: "100 Main St",
    Address2: "",
    City: "Augusta",
    State: "ME",
    Zip: "04330",
    Description: "",
    "Receipt ID": "R-1",
    "Filed Date": "03/15/2024",
    "Report Name": "2024 Pre-General",
    "Receipt Source Type": "Business/Organization",
    "Receipt Type": "Monetary (Itemized)",
    "Committee Type": "Political Action Committee",
    Amended: "N",
    Employer: "",
    Occupation: "",
    "Occupation Comment": "",
    "Employment Information Requested": "N",
    "Forgiven Loan": "N",
    ElectionType: "General",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<MaineOutsideSpendingGroup> = {}): MaineOutsideSpendingGroup {
  return {
    committeeId: "242",
    committeeName: "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
    supportOppose: "support",
    amount: 1600,
    sourceUrl: "https://mainecampaignfinance.com/",
    ...overrides,
  };
}

describe("maineOutsideSpendingAggregator", () => {
  it("normalizes direct and comma-form candidate names", () => {
    expect(normalizeMaineCandidateNameKeys("Paul, Reagan LeeAnn")).toContain("REAGAN LEEANN PAUL");
    expect(normalizeMaineCandidateNameKeys("Reagan LeeAnn Paul")).toContain("REAGAN LEEANN PAUL");
  });

  it("aggregates outside support and opposition groups for a candidate target", () => {
    const sourceUrl = "https://mainecampaignfinance.com/";
    const result = aggregateMaineOutsideSpending({
      candidateName: "Reagan LeeAnn Paul",
      candidateId: "481737",
      officeName: "Representative",
      district: "37",
      electionYear: 2024,
      sourceUrl,
      expenditureRows: [
        expenditure(),
        expenditure({
          "Expenditure ID": "E-2",
          "Expenditure Amount": "17.5000",
          "Support/Oppose Candidate": "For",
          Candidate: "Reagan LeeAnn Paul",
        }),
        expenditure({
          OrgID: "999",
          "Committee Name": "MAINE FUTURE FUND",
          "Expenditure ID": "E-3",
          "Expenditure Amount": "200.0000",
          "Support/Oppose Candidate": "Against",
        }),
        expenditure({
          "Expenditure ID": "E-4",
          "Candidate Office": "Senator",
          "Expenditure Amount": "9999.0000",
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 1600,
        opposeTotal: 200,
        groups: [
          {
            committeeId: "242",
            committeeName: "ASSOCIATED BUILDERS AND CONTRACTORS OF MAINE PAC",
            supportOppose: "support",
            amount: 1600,
            sourceUrl,
          },
          {
            committeeId: "999",
            committeeName: "MAINE FUTURE FUND",
            supportOppose: "oppose",
            amount: 200,
            sourceUrl,
          },
        ],
        sourceUrl,
      },
      matchedExpenditureRowCount: 3,
      includedExpenditureRowCount: 3,
      skippedExpenditureRowCount: 0,
    });
  });

  it("skips malformed outside expenditure rows and validates inputs", () => {
    const result = aggregateMaineOutsideSpending({
      candidateName: "Reagan LeeAnn Paul",
      electionYear: 2024,
      expenditureRows: [
        expenditure({ "IE Report": "N" }),
        expenditure({ "Committee Type": "Candidate Committee" }),
        expenditure({ "Support/Oppose Candidate": "Neutral" }),
        expenditure({ "Expenditure Amount": "not-money" }),
        expenditure({ "Expenditure Date": "12/31/2022" }),
        expenditure({ OrgID: "" }),
        expenditure({ "Committee Name": "" }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 7,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 7,
    });

    expect(() =>
      aggregateMaineOutsideSpending({
        candidateName: "Reagan LeeAnn Paul",
        electionYear: 1999,
        expenditureRows: [],
      })
    ).toThrow("Invalid Maine outside spending aggregation election year");
    expect(() =>
      aggregateMaineOutsideSpending({
        candidateName: "Reagan LeeAnn Paul",
        electionYear: 2024,
        expenditureRows: [],
        maxGroups: 0,
      })
    ).toThrow("Invalid Maine outside spending aggregation maxGroups");
  });
});

describe("maineOutsideGroupContributionAggregator", () => {
  it("backtraces supporting PAC receipts into organization donor and industry breakdowns", () => {
    const sourceUrl = "https://mainecampaignfinance.com/";
    const result = aggregateMaineOutsideGroupContributions({
      electionYear: 2024,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution(),
        contribution({
          "Receipt ID": "R-2",
          "Receipt Amount": "10000.0000",
          "Last Name": "OLD CONSTRUCTION COMPANY LLC",
        }),
        contribution({
          "Receipt ID": "R-3",
          "Receipt Amount": "30000.0000",
          "Last Name": "IBEW LOCAL 26 PAC",
          "Receipt Source Type": "Political Action Committee",
        }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
      outsideGroupBreakdowns: [
        {
          committeeId: "242",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "OLD CONSTRUCTION COMPANY LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "242",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW LOCAL 26 PAC",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "242",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "construction",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "242",
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

  it("returns every donor uncapped, sorted by amount within the group", () => {
    // The display cap lives in the SYNC layer, after classification —
    // capping here would drop tail donors from rebuilt industry totals.
    const result = aggregateMaineOutsideGroupContributions({
      electionYear: 2024,
      outsideGroups: [outsideGroup()],
      minIndustryAmount: 0,
      contributionRows: Array.from({ length: 4 }, (_, index) =>
        contribution({
          "Receipt ID": `R-${index}`,
          "Receipt Amount": `${(index + 1) * 1000}.0000`,
          "Last Name": `DONOR ${index} LLC`,
        })
      ),
    });

    const donors = result.outsideGroupBreakdowns.filter((breakdown) => breakdown.categoryType === "donor");
    expect(donors.map((donor) => donor.categoryName)).toEqual([
      "DONOR 3 LLC",
      "DONOR 2 LLC",
      "DONOR 1 LLC",
      "DONOR 0 LLC",
    ]);
  });

  it("applies one PAC's donors to each support/opposition target and skips non-organization receipts", () => {
    const result = aggregateMaineOutsideGroupContributions({
      electionYear: 2024,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose" })],
      minIndustryAmount: 0,
      contributionRows: [
        contribution({ "Receipt Amount": "5000.0000" }),
        contribution({
          "Receipt ID": "R-2",
          "Receipt Amount": "7500.0000",
          "Last Name": "Public",
          "First Name": "Pat",
          "Receipt Source Type": "Individual",
          Occupation: "Attorney",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(2);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeId: "242",
          supportOppose: "support",
          categoryType: "donor",
          amount: 5000,
        }),
        expect.objectContaining({
          committeeId: "242",
          supportOppose: "oppose",
          categoryType: "donor",
          amount: 5000,
        }),
      ])
    );
  });

  it("handles empty outside groups and validates inputs", () => {
    expect(
      aggregateMaineOutsideGroupContributions({
        electionYear: 2024,
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
      aggregateMaineOutsideGroupContributions({
        electionYear: 1999,
        outsideGroups: [],
        contributionRows: [],
      })
    ).toThrow("Invalid Maine outside group contribution election year");
    expect(() =>
      aggregateMaineOutsideGroupContributions({
        electionYear: 2024,
        outsideGroups: [],
        contributionRows: [],
        minIndustryAmount: -1,
      })
    ).toThrow("Invalid Maine outside group contribution minIndustryAmount");
  });
});
