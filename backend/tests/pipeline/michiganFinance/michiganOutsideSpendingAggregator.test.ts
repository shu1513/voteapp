import { describe, expect, it } from "vitest";

import {
  aggregateMichiganOutsideSpending,
  isMichiganIndependentExpenditureSchedule,
  supportOpposeFromMichiganSuppOpp,
} from "../../../src/pipeline/michiganFinance/michiganOutsideSpendingAggregator.js";
import type { MichiganMitnLegacyExpenditureRow } from "../../../src/pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

function expenditure(overrides: Partial<MichiganMitnLegacyExpenditureRow> = {}): MichiganMitnLegacyExpenditureRow {
  return {
    doc_seq_no: "100",
    doc_stmnt_year: "2022",
    doc_type_desc: "Post-General",
    com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
    common_name: "Get Michigan Working Again",
    cfr_com_id: "520012",
    com_type: "Independent Expenditure Committee",
    schedule_desc: "Independent Expenditure",
    supp_opp: "2",
    can_or_ballot: "GRETCHEN WHITMER",
    amount: "863076.75",
    ...overrides,
  };
}

describe("michiganOutsideSpendingAggregator", () => {
  it("maps Michigan support/oppose values conservatively", () => {
    expect(supportOpposeFromMichiganSuppOpp("1")).toBe("support");
    expect(supportOpposeFromMichiganSuppOpp("support")).toBe("support");
    expect(supportOpposeFromMichiganSuppOpp("for")).toBe("support");
    expect(supportOpposeFromMichiganSuppOpp("2")).toBe("oppose");
    expect(supportOpposeFromMichiganSuppOpp("oppose")).toBe("oppose");
    expect(supportOpposeFromMichiganSuppOpp("against")).toBe("oppose");
    expect(supportOpposeFromMichiganSuppOpp("assist")).toBeNull();
    expect(supportOpposeFromMichiganSuppOpp("")).toBeNull();
  });

  it("detects independent expenditure schedules only", () => {
    expect(isMichiganIndependentExpenditureSchedule("Independent Expenditure")).toBe(true);
    expect(isMichiganIndependentExpenditureSchedule("INDEPENDENT EXPENDITURES")).toBe(true);
    expect(isMichiganIndependentExpenditureSchedule("INDEPENDEN")).toBe(true);
    expect(isMichiganIndependentExpenditureSchedule("Direct Expenditure")).toBe(false);
    expect(isMichiganIndependentExpenditureSchedule("Contribution")).toBe(false);
  });

  it("aggregates exact candidate independent expenditures into support and oppose groups", () => {
    const sourceUrl = "https://www.michigan.gov/sos/example/2022_mi_cfr.7z";
    const result = aggregateMichiganOutsideSpending({
      candidateName: "Gretchen Whitmer",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2022,
      sourceUrl,
      expenditureRows: [
        expenditure(),
        expenditure({
          doc_seq_no: "101",
          cfr_com_id: "520012",
          amount: "100.25",
        }),
        expenditure({
          doc_seq_no: "102",
          cfr_com_id: "600001",
          com_legal_name: "WORKING FAMILIES FOR WHITMER",
          common_name: "Working Families for Whitmer",
          supp_opp: "1",
          amount: "200000.00",
        }),
        expenditure({
          doc_seq_no: "103",
          can_or_ballot: "OTHER PERSON",
          amount: "999999.00",
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 200000,
        opposeTotal: 863177,
        groups: [
          {
            committeeId: "520012",
            committeeName: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
            supportOppose: "oppose",
            amount: 863177,
            sourceUrl,
          },
          {
            committeeId: "600001",
            committeeName: "WORKING FAMILIES FOR WHITMER",
            supportOppose: "support",
            amount: 200000,
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

  it("skips rows missing explicit direction, independent-expenditure schedule, positive amount, or cycle year", () => {
    const result = aggregateMichiganOutsideSpending({
      candidateName: "Gretchen Whitmer",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2022,
      expenditureRows: [
        expenditure({ supp_opp: "" }),
        expenditure({ schedule_desc: "Direct Expenditure" }),
        expenditure({ amount: "0" }),
        expenditure({ amount: "-10" }),
        expenditure({ amount: "not a number" }),
        expenditure({ doc_stmnt_year: "2020" }),
        expenditure({ cfr_com_id: "" }),
        expenditure({ com_legal_name: "", common_name: "" }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 8,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 8,
    });
  });

  it("requires eligible office and legislative district before aggregation", () => {
    expect(
      aggregateMichiganOutsideSpending({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "State Treasurer",
        electionYear: 2022,
        expenditureRows: [expenditure({ can_or_ballot: "JANE DOE" })],
      })
    ).toEqual({
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });

    expect(
      aggregateMichiganOutsideSpending({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2022,
        expenditureRows: [expenditure({ can_or_ballot: "JANE DOE" })],
      })
    ).toEqual({
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });
  });

  it("matches comma-form candidate names without substring matching", () => {
    const result = aggregateMichiganOutsideSpending({
      candidateName: "Gretchen Whitmer",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2022,
      expenditureRows: [
        expenditure({ can_or_ballot: "WHITMER, GRETCHEN" }),
        expenditure({ doc_seq_no: "other", can_or_ballot: "WHITMERSON, GRETCHEN" }),
      ],
    });

    expect(result.summary?.opposeTotal).toBe(863076.75);
    expect(result.matchedExpenditureRowCount).toBe(1);
    expect(result.includedExpenditureRowCount).toBe(1);
  });

  it("limits groups without changing summary totals", () => {
    const result = aggregateMichiganOutsideSpending({
      candidateName: "Gretchen Whitmer",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2022,
      maxGroups: 1,
      expenditureRows: [
        expenditure({ cfr_com_id: "1", com_legal_name: "A", amount: "100" }),
        expenditure({ cfr_com_id: "2", com_legal_name: "B", supp_opp: "1", amount: "200" }),
      ],
    });

    expect(result.summary?.supportTotal).toBe(200);
    expect(result.summary?.opposeTotal).toBe(100);
    expect(result.summary?.groups).toEqual([
      expect.objectContaining({ committeeId: "2", committeeName: "B", amount: 200 }),
    ]);
  });

  it("validates supported legacy years and maxGroups", () => {
    expect(() =>
      aggregateMichiganOutsideSpending({
        candidateName: "Gretchen Whitmer",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        expenditureRows: [],
      })
    ).toThrow("Invalid Michigan MiTN legacy archive year");

    expect(() =>
      aggregateMichiganOutsideSpending({
        candidateName: "Gretchen Whitmer",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        maxGroups: 0,
        expenditureRows: [],
      })
    ).toThrow("maxGroups");
  });
});
