import { describe, expect, it } from "vitest";

import { aggregateMichiganOutsideGroupContributions } from "../../../src/pipeline/michiganFinance/michiganOutsideGroupContributionAggregator.js";
import type { MichiganMitnLegacyContributionRow } from "../../../src/pipeline/michiganFinance/michiganMitnLegacyRowTypes.js";
import type { MichiganOutsideSpendingGroup } from "../../../src/pipeline/michiganFinance/michiganOutsideSpendingAggregator.js";

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
    com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
    common_name: "Get Michigan Working Again",
    cfr_com_id: "520012",
    com_type: "IND",
    can_first_name: "",
    can_last_name: "",
    contribtype: "Organization",
    f_name: "",
    l_name_or_org: "Energy Transfer LLC",
    address: "1 Main",
    city: "Lansing",
    state: "MI",
    zip: "48901",
    occupation: "",
    employer: "",
    received_date: "10/01/2022",
    amount: "25000.00",
    aggregate: "25000.00",
    extra_desc: "",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<MichiganOutsideSpendingGroup> = {}): MichiganOutsideSpendingGroup {
  return {
    committeeId: "520012",
    committeeName: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
    supportOppose: "oppose",
    amount: 863076.75,
    sourceUrl: "https://www.michigan.gov/sos/example/2022_mi_cfr.7z",
    ...overrides,
  };
}

describe("michiganOutsideGroupContributionAggregator", () => {
  it("backtraces outside spender organization donors into donor and industry breakdowns", () => {
    const sourceUrl = "https://www.michigan.gov/sos/example/2022_mi_cfr.7z";
    const result = aggregateMichiganOutsideGroupContributions({
      electionYear: 2022,
      sourceUrl,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({ cont_detail_id: "1", amount: "25000.00", l_name_or_org: "Energy Transfer LLC" }),
        contribution({ cont_detail_id: "2", amount: "10000.00", l_name_or_org: "Energy Transfer LLC" }),
        contribution({ cont_detail_id: "3", amount: "30000.00", l_name_or_org: "IBEW Voluntary PAC" }),
      ],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [
        {
          committeeId: "520012",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "520012",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "IBEW Voluntary PAC",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "520012",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 35000,
          contributorCount: 1,
          sourceUrl,
        },
        {
          committeeId: "520012",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 30000,
          contributorCount: 1,
          sourceUrl,
        },
      ],
      matchedContributionRowCount: 3,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 0,
    });
  });

  it("keeps support and opposition breakdowns separate for the same spender committee", () => {
    const result = aggregateMichiganOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [
        outsideGroup({ supportOppose: "oppose" }),
        outsideGroup({ supportOppose: "support", amount: 1000 }),
      ],
      contributionRows: [contribution({ amount: "25000", l_name_or_org: "Energy Transfer LLC" })],
    });

    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeId: "520012",
          supportOppose: "oppose",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 25000,
        }),
        expect.objectContaining({
          committeeId: "520012",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Energy Transfer LLC",
          amount: 25000,
        }),
        expect.objectContaining({
          committeeId: "520012",
          supportOppose: "oppose",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 25000,
        }),
        expect.objectContaining({
          committeeId: "520012",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "oil_gas_energy",
          amount: 25000,
        }),
      ])
    );
  });

  it("classifies only organization donors above the state threshold and never uses individual employer fields", () => {
    const result = aggregateMichiganOutsideGroupContributions({
      electionYear: 2022,
      minIndustryAmount: 25_000,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({ cont_detail_id: "1", amount: "24999.99", l_name_or_org: "Energy Transfer LLC" }),
        contribution({
          cont_detail_id: "2",
          amount: "50000",
          contribtype: "Individual",
          f_name: "Pat",
          l_name_or_org: "Person",
          employer: "Energy Transfer LLC",
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "donor",
        categoryName: "Energy Transfer LLC",
        amount: 24999.99,
      }),
    ]);
    expect(result.matchedContributionRowCount).toBe(2);
    expect(result.includedContributionRowCount).toBe(1);
    expect(result.skippedContributionRowCount).toBe(1);
  });

  it("returns every donor uncapped, sorted by amount", () => {
    // The display cap lives in the SYNC layer, after classification —
    // capping here would drop tail donors from rebuilt industry totals.
    const result = aggregateMichiganOutsideGroupContributions({
      electionYear: 2022,
      minIndustryAmount: 0,
      outsideGroups: [outsideGroup()],
      contributionRows: Array.from({ length: 4 }, (_, index) =>
        contribution({
          cont_detail_id: String(index),
          amount: `${(index + 1) * 1000}.00`,
          l_name_or_org: `DONOR ${index} LLC`,
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

  it("skips invalid outside donor receipts", () => {
    const result = aggregateMichiganOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({ amount: "0" }),
        contribution({ amount: "-100" }),
        contribution({ amount: "not a number" }),
        contribution({ received_date: "10/01/2020" }),
        contribution({ contribtype: "Loan" }),
        contribution({ l_name_or_org: "" }),
        contribution({ cfr_com_id: "999999", amount: "50000", l_name_or_org: "Energy Transfer LLC" }),
      ],
    });

    expect(result).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 6,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 6,
    });
  });

  it("returns an empty result when there are no safe outside groups", () => {
    expect(
      aggregateMichiganOutsideGroupContributions({
        electionYear: 2022,
        outsideGroups: [],
        contributionRows: [contribution()],
      })
    ).toEqual({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    });
  });

  it("allows future election years and validates the min industry amount", () => {
    expect(
      aggregateMichiganOutsideGroupContributions({
        electionYear: 2026,
        outsideGroups: [outsideGroup()],
        contributionRows: [],
      })
    ).toMatchObject({
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
    });

    expect(() =>
      aggregateMichiganOutsideGroupContributions({
        electionYear: 2019,
        outsideGroups: [outsideGroup()],
        contributionRows: [],
      })
    ).toThrow("Invalid Michigan MiTN legacy archive year");

    expect(() =>
      aggregateMichiganOutsideGroupContributions({
        electionYear: 2022,
        minIndustryAmount: -1,
        outsideGroups: [outsideGroup()],
        contributionRows: [],
      })
    ).toThrow("minIndustryAmount");
  });
});
