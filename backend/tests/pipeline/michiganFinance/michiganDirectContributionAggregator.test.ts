import { describe, expect, it } from "vitest";

import {
  aggregateMichiganDirectContributions,
  isMichiganDirectDonorSupportReceipt,
  isMichiganTotalReceipt,
  michiganElectionCycleStartYear,
} from "../../../src/pipeline/michiganFinance/michiganDirectContributionAggregator.js";
import type { MichiganMitnLegacyContributionRow } from "../../../src/pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

function contribution(
  overrides: Partial<MichiganMitnLegacyContributionRow> = {}
): MichiganMitnLegacyContributionRow {
  return {
    doc_seq_no: "100",
    page_no: "1",
    contribution_id: "200",
    cont_detail_id: "300",
    doc_stmnt_year: "2022",
    doc_type_desc: "Post-General",
    com_legal_name: "WHITMER FOR GOVERNOR",
    common_name: "Whitmer for Governor",
    cfr_com_id: "514456",
    com_type: "CAN",
    can_first_name: "GRETCHEN",
    can_last_name: "WHITMER",
    contribtype: "Individual",
    f_name: "JANE",
    l_name_or_org: "DOE",
    address: "1 Main",
    city: "Lansing",
    state: "MI",
    zip: "48901",
    occupation: "Attorney",
    employer: "Law Firm",
    received_date: "10/01/2022",
    amount: "250.00",
    aggregate: "250.00",
    extra_desc: "",
    ...overrides,
  };
}

describe("michiganDirectContributionAggregator", () => {
  it("aggregates direct donor support by occupation and contribution size", () => {
    const sourceUrl = "https://www.michigan.gov/sos/example/2022_mi_cfr.7z";
    const result = aggregateMichiganDirectContributions({
      committeeId: "514456",
      electionYear: 2022,
      sourceUrl,
      contributionRows: [
        contribution({ amount: "100.00", occupation: "Attorney" }),
        contribution({
          cont_detail_id: "301",
          amount: "$250.00",
          occupation: "Attorney",
          l_name_or_org: "ROE",
        }),
        contribution({
          cont_detail_id: "302",
          amount: "5,000.00",
          occupation: "Teacher",
          l_name_or_org: "SMITH",
        }),
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

  it("counts distinct contributors instead of contribution rows", () => {
    const result = aggregateMichiganDirectContributions({
      committeeId: "514456",
      electionYear: 2022,
      contributionRows: [
        contribution({ cont_detail_id: "R1", amount: "100", occupation: "Attorney" }),
        contribution({ cont_detail_id: "R2", amount: "200", occupation: "Attorney" }),
        contribution({
          cont_detail_id: "R3",
          amount: "300",
          occupation: "Attorney",
          l_name_or_org: "ROE",
        }),
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

  it("matches committee IDs case-insensitively and does not emit employer breakdowns", () => {
    const result = aggregateMichiganDirectContributions({
      committeeId: " abc123 ",
      electionYear: 2022,
      contributionRows: [
        contribution({
          cfr_com_id: "ABC123",
          amount: "300",
          occupation: "Attorney",
          employer: "Law Firm",
        }),
        contribution({
          cfr_com_id: "OTHER",
          amount: "900",
          occupation: "Doctor",
          employer: "Hospital",
        }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(300);
    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 300 }),
      ])
    );
    expect(
      result.directBreakdowns.every((row) => row.categoryType === "occupation" || row.categoryType === "contribution_size")
    ).toBe(true);
  });

  it("sums cents without floating-point drift", () => {
    const result = aggregateMichiganDirectContributions({
      committeeId: "514456",
      electionYear: 2022,
      contributionRows: [
        contribution({ amount: "0.10", occupation: "Engineer" }),
        contribution({ amount: "0.20", occupation: "Engineer" }),
      ],
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

  it("filters to the two-year election cycle ending in the election year", () => {
    expect(michiganElectionCycleStartYear(2022)).toBe(2021);

    const result = aggregateMichiganDirectContributions({
      committeeId: "514456",
      electionYear: 2022,
      contributionRows: [
        contribution({ received_date: "12/31/2020", amount: "100" }),
        contribution({ received_date: "1/1/2021", amount: "200" }),
        contribution({ received_date: "2022-11-01", amount: "300" }),
        contribution({ received_date: "1/1/2023", amount: "400" }),
      ],
    });

    expect(result.summary.totalReceipts).toBe(500);
    expect(result.summary.directContributionTotal).toBe(500);
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(2);
  });

  it("keeps non-donor receipt types out of direct donor breakdowns", () => {
    const result = aggregateMichiganDirectContributions({
      committeeId: "514456",
      electionYear: 2022,
      contributionRows: [
        contribution({ amount: "500", contribtype: "Individual", occupation: "Attorney" }),
        contribution({ cont_detail_id: "loan", amount: "1000", contribtype: "Loan", occupation: "Business Owner" }),
        contribution({ cont_detail_id: "refund", amount: "250", contribtype: "Refund", occupation: "Accountant" }),
      ],
    });

    expect(result.summary).toEqual({ totalReceipts: 1750, directContributionTotal: 500, sourceUrl: null });
    expect(result.matchedContributionRowCount).toBe(3);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(2);
    expect(result.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 500 }),
      ])
    );
    expect(result.directBreakdowns.some((row) => row.categoryName === "Business Owner")).toBe(false);
  });

  it("classifies only positive same-cycle committee rows as total and direct receipts", () => {
    const valid = contribution({ amount: "250", contribtype: "Individual" });
    expect(isMichiganTotalReceipt({ row: valid, electionYear: 2022 })).toBe(true);
    expect(isMichiganDirectDonorSupportReceipt({ row: valid, electionYear: 2022 })).toBe(true);

    const excludedRows = [
      contribution({ cfr_com_id: "" }),
      contribution({ amount: "0" }),
      contribution({ amount: "-10" }),
      contribution({ amount: "not a number" }),
      contribution({ received_date: "2020-12-31" }),
      contribution({ contribtype: "Loan" }),
    ];
    for (const excludedRow of excludedRows) {
      if (excludedRow.contribtype === "Loan") {
        expect(isMichiganTotalReceipt({ row: excludedRow, electionYear: 2022 })).toBe(true);
        expect(isMichiganDirectDonorSupportReceipt({ row: excludedRow, electionYear: 2022 })).toBe(false);
      } else {
        expect(isMichiganTotalReceipt({ row: excludedRow, electionYear: 2022 })).toBe(false);
        expect(isMichiganDirectDonorSupportReceipt({ row: excludedRow, electionYear: 2022 })).toBe(false);
      }
    }
  });

  it("limits occupation breakdowns without dropping contribution-size buckets", () => {
    const result = aggregateMichiganDirectContributions({
      committeeId: "514456",
      electionYear: 2022,
      maxBreakdownsPerCategory: 1,
      contributionRows: [
        contribution({ occupation: "Engineer", amount: "100" }),
        contribution({ occupation: "Teacher", amount: "300" }),
        contribution({ occupation: "Doctor", amount: "600" }),
      ],
    });

    expect(result.directBreakdowns.filter((row) => row.categoryType === "occupation")).toEqual([
      expect.objectContaining({ categoryName: "Doctor", amount: 600 }),
    ]);
    expect(result.directBreakdowns.filter((row) => row.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("validates required aggregation inputs", () => {
    expect(() =>
      aggregateMichiganDirectContributions({ committeeId: " ", electionYear: 2022, contributionRows: [] })
    ).toThrow("Michigan committee id is required");
    expect(() =>
      aggregateMichiganDirectContributions({ committeeId: "514456", electionYear: 2019, contributionRows: [] })
    ).toThrow("Invalid Michigan MiTN legacy archive year");
    expect(() =>
      aggregateMichiganDirectContributions({
        committeeId: "514456",
        electionYear: 2022,
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("maxBreakdownsPerCategory");
  });
});
