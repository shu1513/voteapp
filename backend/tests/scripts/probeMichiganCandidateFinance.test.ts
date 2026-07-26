import { describe, expect, it } from "vitest";

import {
  parseProbeMichiganCandidateFinanceArgs,
  runProbeMichiganCandidateFinance,
} from "../../src/scripts/probeMichiganCandidateFinance.js";
import type {
  MichiganMitnLegacyContributionRow,
  MichiganMitnLegacyExpenditureRow,
} from "../../src/pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

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
    amount: "100.00",
    aggregate: "100.00",
    extra_desc: "",
    ...overrides,
  };
}

function expenditure(overrides: Partial<MichiganMitnLegacyExpenditureRow> = {}): MichiganMitnLegacyExpenditureRow {
  return {
    doc_seq_no: "900",
    doc_stmnt_year: "2022",
    doc_type_desc: "Post-General",
    com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
    common_name: "Get Michigan Working Again",
    cfr_com_id: "520012",
    com_type: "IND",
    schedule_desc: "Independent Expenditure",
    supp_opp: "2",
    can_or_ballot: "GRETCHEN WHITMER",
    _column_29: "GOVERNOR",
    amount: "863076.75",
    ...overrides,
  };
}

describe("probeMichiganCandidateFinance script", () => {
  it("parses live probe arguments", () => {
    expect(
      parseProbeMichiganCandidateFinanceArgs([
        "--candidate-name=Gretchen Whitmer",
        "--year=2022",
        "--office=Governor",
        "--office-scope=statewide",
        "--raw-extracted-dir=/tmp/2022_mi_cfr",
        "--limit=3",
        "--min-industry-amount=25000",
      ])
    ).toEqual({
      candidateName: "Gretchen Whitmer",
      electionYear: 2022,
      officeScope: "statewide",
      officeName: "Governor",
      district: null,
      rawExtractedDir: "/tmp/2022_mi_cfr",
      sourceUrl:
        "https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data/2022_mi_cfr.7z",
      maxRows: undefined,
      limit: 3,
      minIndustryAmount: 25000,
    });
  });

  it("rejects missing and malformed required options", () => {
    expect(() => parseProbeMichiganCandidateFinanceArgs(["--year=2022", "--office=Governor"])).toThrow(
      "--candidate-name is required"
    );
    expect(() =>
      parseProbeMichiganCandidateFinanceArgs([
        "--candidate-name=Gretchen Whitmer",
        "--year=2022",
        "--office=Governor",
      ])
    ).toThrow("--raw-extracted-dir is required");
    expect(() =>
      parseProbeMichiganCandidateFinanceArgs([
        "--candidate-name=Gretchen Whitmer",
        "--year=22",
        "--office=Governor",
        "--raw-extracted-dir=/tmp/2022_mi_cfr",
      ])
    ).toThrow("Invalid --year value");
  });

  it("returns direct occupations and outside industry backtrace from fixture rows", async () => {
    const result = await runProbeMichiganCandidateFinance({
      options: {
        candidateName: "Gretchen Whitmer",
        electionYear: 2022,
        officeScope: "statewide",
        officeName: "Governor",
        rawExtractedDir: "/unused",
        sourceUrl: "https://example.test/2022_mi_cfr.7z",
        limit: 5,
        minIndustryAmount: 25000,
      },
      contributionRows: [
        contribution({ cont_detail_id: "1", amount: "100.00", occupation: "Attorney" }),
        contribution({ cont_detail_id: "2", amount: "250.00", occupation: "Teacher", l_name_or_org: "ROE" }),
        contribution({
          cont_detail_id: "3",
          cfr_com_id: "520012",
          com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
          common_name: "Get Michigan Working Again",
          com_type: "IND",
          can_first_name: "",
          can_last_name: "",
          contribtype: "Organization",
          f_name: "",
          l_name_or_org: "Energy Transfer LLC",
          occupation: "",
          employer: "",
          amount: "25000.00",
        }),
      ],
      expenditureRows: [expenditure()],
    });

    expect(result).toMatchObject({
      ok: true,
      resolution: {
        status: "matched",
        committeeId: "514456",
      },
      summary: {
        total_receipts: 350,
        direct_contribution_total: 350,
        outside_support_total: 0,
        outside_oppose_total: 863076.75,
      },
      direct_campaign: {
        top_occupations: [
          {
            name: "Teacher",
            amount: 250,
          },
          {
            name: "Attorney",
            amount: 100,
          },
        ],
      },
      outside_spending: {
        top_opposing_groups: [
          {
            name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
            support_oppose: "oppose",
            amount: 863076.75,
          },
        ],
        top_opposing_industries: [
          {
            name: "oil_gas_energy",
            amount: 25000,
          },
        ],
      },
    });
    expect(result.outside_spending.top_supporting_groups).toEqual([]);
  });
});
