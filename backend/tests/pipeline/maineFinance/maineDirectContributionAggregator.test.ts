import { describe, expect, it } from "vitest";

import {
  aggregateMaineDirectContributions,
  isMaineDirectDonorSupportReceipt,
  isMaineTotalReceipt,
  maineElectionCycleStartYear,
} from "../../../src/pipeline/maineFinance/maineDirectContributionAggregator.js";
import type { MaineCfisContributionRow } from "../../../src/pipeline/maineFinance/maineCfisArtifactReader.js";

function contribution(overrides: Partial<MaineCfisContributionRow> = {}): MaineCfisContributionRow {
  return {
    OrgID: "1001",
    LegacyID: "618",
    "Committee Name": "Jane Doe for Maine",
    "Candidate Name": "Jane Doe",
    "Receipt Amount": "250.0000",
    "Receipt Date": "03/11/2024",
    Office: "Representative",
    District: "37",
    "Last Name": "Voter",
    "First Name": "Pat",
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
    "Receipt Source Type": "Individual",
    "Receipt Type": "Monetary (Itemized)",
    "Committee Type": "Candidate Committee",
    Amended: "N",
    Employer: "LARGAY LAW OFFICES, P.A.",
    Occupation: "Attorney/Legal",
    "Occupation Comment": "",
    "Employment Information Requested": "N",
    "Forgiven Loan": "N",
    ElectionType: "General",
    ...overrides,
  };
}

describe("maineDirectContributionAggregator", () => {
  it("aggregates candidate committee receipts into top occupations and contribution sizes", () => {
    const sourceUrl = "https://mainecampaignfinance.com/";
    const result = aggregateMaineDirectContributions({
      committeeId: "1001",
      electionYear: 2024,
      sourceUrl,
      contributionRows: [
        contribution({ "Receipt Amount": "100.0000", "Receipt ID": "R-1" }),
        contribution({ "Receipt Amount": "150.0000", "Receipt ID": "R-2" }),
        contribution({
          "Receipt Amount": "250.0000",
          "Receipt ID": "R-3",
          "Last Name": "Educator",
          Address1: "200 State St",
          Employer: "Augusta Schools",
          Occupation: "Teacher",
        }),
        contribution({ "Receipt Type": "Loan", "Receipt Amount": "5000.0000", "Receipt ID": "R-4" }),
        contribution({ OrgID: "9999", "Receipt Amount": "9999.0000" }),
      ],
    });

    expect(result).toEqual({
      summary: {
        totalReceipts: 5500,
        directContributionTotal: 500,
        sourceUrl,
      },
      directBreakdowns: [
        {
          categoryType: "occupation",
          categoryName: "Attorney/Legal",
          amount: 250,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "occupation",
          categoryName: "Teacher",
          amount: 250,
          contributorCount: 1,
          sourceUrl,
        },
        {
          categoryType: "contribution_size",
          categoryName: "$100-$249",
          amount: 250,
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
      ],
      matchedContributionRowCount: 4,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 1,
    });
  });

  it("filters to the two-year cycle and keeps total receipts broader than direct donor support", () => {
    expect(maineElectionCycleStartYear(2024)).toBe(2023);

    const result = aggregateMaineDirectContributions({
      committeeId: "1001",
      electionYear: 2024,
      contributionRows: [
        contribution({ "Receipt Date": "12/31/2022", "Receipt Amount": "100.0000" }),
        contribution({ "Receipt Date": "01/01/2023", "Receipt Amount": "200.0000" }),
        contribution({ "Receipt Date": "2024-10-01", "Receipt Amount": "300.0000" }),
        contribution({ "Receipt Date": "01/01/2025", "Receipt Amount": "400.0000" }),
        contribution({ "Receipt Type": "Forgiven Loan", "Receipt Amount": "500.0000" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(1000);
    expect(result.summary.directContributionTotal).toBe(500);
    expect(result.matchedContributionRowCount).toBe(5);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(3);
  });

  it("classifies Maine receipt rows used for total and direct support", () => {
    expect(isMaineTotalReceipt({ row: contribution(), electionYear: 2024 })).toBe(true);
    expect(isMaineDirectDonorSupportReceipt({ row: contribution(), electionYear: 2024 })).toBe(true);
    expect(isMaineTotalReceipt({ row: contribution({ "Receipt Type": "Loan" }), electionYear: 2024 })).toBe(true);
    expect(isMaineDirectDonorSupportReceipt({ row: contribution({ "Receipt Type": "Loan" }), electionYear: 2024 })).toBe(
      false
    );
  });

  it("validates required inputs", () => {
    expect(() =>
      aggregateMaineDirectContributions({
        committeeId: " ",
        electionYear: 2024,
        contributionRows: [],
      })
    ).toThrow("Maine committee id is required");
    expect(() =>
      aggregateMaineDirectContributions({
        committeeId: "1001",
        electionYear: 1999,
        contributionRows: [],
      })
    ).toThrow("Invalid Maine direct contribution aggregation election year");
    expect(() =>
      aggregateMaineDirectContributions({
        committeeId: "1001",
        electionYear: 2024,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("Invalid Maine direct contribution aggregation maxBreakdownsPerCategory");
  });
});
