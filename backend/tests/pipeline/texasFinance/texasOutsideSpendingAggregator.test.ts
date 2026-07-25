import { describe, expect, it } from "vitest";

import {
  aggregateTexasOutsideSpending,
  supportOpposeFromTexasSpacPosition,
} from "../../../src/pipeline/texasFinance/texasOutsideSpendingAggregator.js";
import type {
  TexasTecCandidateRow,
  TexasTecExpenditureRow,
  TexasTecSpacRow,
} from "../../../src/pipeline/texasFinance/texasTecCsvDatabaseReader.js";

function candidate(overrides: Partial<TexasTecCandidateRow> = {}): TexasTecCandidateRow {
  return {
    recordType: "CAND",
    filerIdent: "7001",
    filerTypeCd: "SPAC",
    filerName: "Texans for Example",
    expendInfoId: "E1",
    expendDt: "20261015",
    expendAmount: "70000.00",
    expendDescr: "Direct campaign expenditure",
    candidatePersentTypeCd: "INDIVIDUAL",
    candidateNameOrganization: "",
    candidateNameLast: "ABBOTT",
    candidateNameFirst: "GREG",
    candidateSeekOfficeCd: "GOVERNOR",
    candidateSeekOfficeDistrict: "",
    candidateSeekOfficePlace: "",
    candidateSeekOfficeDescr: "Governor",
    candidateSeekOfficeCountyCd: "",
    candidateSeekOfficeCountyDescr: "",
    ...overrides,
  };
}

function expenditure(overrides: Partial<TexasTecExpenditureRow> = {}): TexasTecExpenditureRow {
  return {
    recordType: "EXPEND",
    formTypeCd: "SPAC",
    schedFormTypeCd: "F1",
    reportInfoIdent: "R1",
    receivedDt: "20261016",
    infoOnlyFlag: "",
    filerIdent: "7001",
    filerTypeCd: "SPAC",
    filerName: "Texans for Example",
    expendInfoId: "E1",
    expendDt: "20261015",
    expendAmount: "70000.00",
    expendDescr: "Direct campaign expenditure",
    expendCatCd: "ADV",
    expendCatDescr: "Advertising",
    politicalExpendCd: "DIRECT",
    payeePersentTypeCd: "ENTITY",
    payeeNameOrganization: "Vendor LLC",
    payeeNameLast: "",
    payeeNameFirst: "",
    ...overrides,
  };
}

function spac(overrides: Partial<TexasTecSpacRow> = {}): TexasTecSpacRow {
  return {
    recordType: "SPAC",
    spacFilerIdent: "7001",
    spacFilerTypeCd: "SPAC",
    spacFilerName: "Texans for Example",
    spacFilerNameShort: "",
    spacCommitteeStatusCd: "ACTIVE",
    spacPositionCd: "SUPPORT",
    candidateFilerIdent: "00012345",
    candidateFilerTypeCd: "COH",
    candidateFilerName: "ABBOTT, GREG",
    candidateFilerpersStatusCd: "CURRENT",
    candidateSeekOfficeCd: "GOVERNOR",
    candidateSeekOfficeDistrict: "",
    candidateSeekOfficePlace: "",
    candidateSeekOfficeDescr: "Governor",
    candidateSeekOfficeCountyCd: "",
    candidateSeekOfficeCountyDescr: "",
    ...overrides,
  };
}

describe("texasOutsideSpendingAggregator", () => {
  it("aggregates outside support and opposition only when SPAC relationship and expenditure rows agree", () => {
    const sourceUrl = "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip";
    const result = aggregateTexasOutsideSpending({
      candidateName: "Greg Abbott",
      candidateCommitteeId: "00012345",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      sourceUrl,
      spacRows: [
        spac(),
        spac({
          spacFilerIdent: "7002",
          spacFilerName: "Texas Accountability PAC",
          spacPositionCd: "OPPOSE",
        }),
      ],
      expenditureRows: [
        expenditure({ expendAmount: "70000.00" }),
        expenditure({ expendInfoId: "E2", expendAmount: "30,000.25" }),
        expenditure({
          filerIdent: "7002",
          filerName: "Texas Accountability PAC",
          expendInfoId: "E3",
          expendAmount: "5000",
        }),
        expenditure({
          filerIdent: "9000",
          filerName: "Unrelated PAC",
          expendInfoId: "E4",
          expendAmount: "999999",
        }),
      ],
      candidateRows: [
        candidate({ expendAmount: "70000.00" }),
        candidate({ expendInfoId: "E2", expendAmount: "30,000.25" }),
        candidate({
          filerIdent: "7002",
          filerName: "Texas Accountability PAC",
          expendInfoId: "E3",
          expendAmount: "5000",
        }),
        candidate({
          filerIdent: "9000",
          filerName: "Unrelated PAC",
          expendInfoId: "E4",
          expendAmount: "999999",
        }),
        candidate({
          filerIdent: "7001",
          expendInfoId: "E5",
          candidateNameLast: "PAXTON",
          candidateNameFirst: "KEN",
          expendAmount: "888888",
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 100000.25,
        opposeTotal: 5000,
        groups: [
          {
            committeeId: "7001",
            committeeName: "Texans for Example",
            supportOppose: "support",
            amount: 100000.25,
            sourceUrl,
          },
          {
            committeeId: "7002",
            committeeName: "Texas Accountability PAC",
            supportOppose: "oppose",
            amount: 5000,
            sourceUrl,
          },
        ],
        sourceUrl,
      },
      matchedCandidateExpenditureRowCount: 4,
      includedCandidateExpenditureRowCount: 3,
      skippedCandidateExpenditureRowCount: 1,
    });
  });

  it("matches nickname purpose rows but only counts spenders related to the linked committee", () => {
    // "Pat Smith" expands to PATRICK SMITH and PATRICIA SMITH on the VoteApp
    // side. The unrelated spender's PATRICIA row name-matches but cannot
    // contribute money (no SPAC position on the linked committee), so it is
    // skipped — and it must not trip the conflicting-first-name abort either,
    // which only consults rows from related spenders.
    const result = aggregateTexasOutsideSpending({
      candidateName: "Pat Smith",
      candidateCommitteeId: "00012345",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      spacRows: [spac({ candidateFilerName: "SMITH, PATRICK" })],
      expenditureRows: [
        expenditure({ expendAmount: "70000.00" }),
        expenditure({
          filerIdent: "9000",
          filerName: "Unrelated PAC",
          expendInfoId: "E6",
          expendAmount: "40000.00",
        }),
      ],
      candidateRows: [
        candidate({
          candidateNameLast: "SMITH",
          candidateNameFirst: "PATRICK",
          expendAmount: "70000.00",
        }),
        candidate({
          filerIdent: "9000",
          filerName: "Unrelated PAC",
          expendInfoId: "E6",
          candidateNameLast: "SMITH",
          candidateNameFirst: "PATRICIA",
          expendAmount: "40000.00",
        }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        supportTotal: 70000,
        opposeTotal: 0,
      },
      matchedCandidateExpenditureRowCount: 2,
      includedCandidateExpenditureRowCount: 1,
      skippedCandidateExpenditureRowCount: 1,
    });
    expect(result.summary?.groups).toHaveLength(1);
    expect(result.summary?.groups[0]).toMatchObject({ committeeId: "7001", supportOppose: "support" });
  });

  it("refuses to aggregate when matched rows span conflicting formal first names", () => {
    // A spender related to the linked committee filed purpose rows for both
    // PATRICK and PATRICIA: positive evidence the expanded key set caught two
    // people. The whole aggregation aborts rather than combining their money.
    const result = aggregateTexasOutsideSpending({
      candidateName: "Pat Smith",
      candidateCommitteeId: "00012345",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      spacRows: [spac({ candidateFilerName: "SMITH, PATRICK" })],
      expenditureRows: [
        expenditure({ expendAmount: "70000.00" }),
        expenditure({ expendInfoId: "E6", expendAmount: "40000.00" }),
      ],
      candidateRows: [
        candidate({
          candidateNameLast: "SMITH",
          candidateNameFirst: "PATRICK",
          expendAmount: "70000.00",
        }),
        candidate({
          expendInfoId: "E6",
          candidateNameLast: "SMITH",
          candidateNameFirst: "PATRICIA",
          expendAmount: "40000.00",
        }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedCandidateExpenditureRowCount: 2,
      includedCandidateExpenditureRowCount: 0,
      skippedCandidateExpenditureRowCount: 2,
    });
  });

  it("still aggregates when matched rows only differ by formal spelling of one name", () => {
    // STEPHEN and STEVEN are spellings of the same name, not two people.
    const result = aggregateTexasOutsideSpending({
      candidateName: "Steve Weir",
      candidateCommitteeId: "00012345",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      spacRows: [spac({ candidateFilerName: "WEIR, STEPHEN" })],
      expenditureRows: [
        expenditure({ expendAmount: "70000.00" }),
        expenditure({ expendInfoId: "E6", expendAmount: "40000.00" }),
      ],
      candidateRows: [
        candidate({
          candidateNameLast: "WEIR",
          candidateNameFirst: "STEPHEN",
          expendAmount: "70000.00",
        }),
        candidate({
          expendInfoId: "E6",
          candidateNameLast: "WEIR",
          candidateNameFirst: "STEVEN",
          expendAmount: "40000.00",
        }),
      ],
    });

    expect(result).toMatchObject({
      summary: { supportTotal: 110000, opposeTotal: 0 },
      matchedCandidateExpenditureRowCount: 2,
      includedCandidateExpenditureRowCount: 2,
      skippedCandidateExpenditureRowCount: 0,
    });
  });

  it("uses the joined expenditure row as the amount and date source", () => {
    const result = aggregateTexasOutsideSpending({
      candidateName: "Greg Abbott",
      candidateCommitteeId: "00012345",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      sourceUrl: "https://www.ethics.state.tx.us/search/cf/",
      spacRows: [spac()],
      expenditureRows: [expenditure({ expendAmount: "1200.00", expendDt: "20261015" })],
      candidateRows: [candidate({ expendAmount: "999999.00", expendDt: "20240101" })],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 1200,
        opposeTotal: 0,
        groups: [
          {
            committeeId: "7001",
            committeeName: "Texans for Example",
            supportOppose: "support",
            amount: 1200,
            sourceUrl: "https://www.ethics.state.tx.us/search/cf/",
          },
        ],
        sourceUrl: "https://www.ethics.state.tx.us/search/cf/",
      },
      matchedCandidateExpenditureRowCount: 1,
      includedCandidateExpenditureRowCount: 1,
      skippedCandidateExpenditureRowCount: 0,
    });
  });

  it("requires exact target office and legislative district matches", () => {
    const result = aggregateTexasOutsideSpending({
      candidateName: "Jane Doe",
      candidateCommitteeId: "00022222",
      officeScope: "state_lower",
      officeName: "State Representative",
      district: "52",
      electionYear: 2026,
      spacRows: [
        spac({
          spacFilerIdent: "8001",
          spacFilerName: "House Future PAC",
          candidateFilerIdent: "00022222",
          candidateFilerName: "DOE, JANE",
          candidateSeekOfficeCd: "STATEREP",
          candidateSeekOfficeDistrict: "52",
          candidateSeekOfficeDescr: "State Representative",
        }),
      ],
      expenditureRows: [
        expenditure({ filerIdent: "8001", filerName: "House Future PAC", expendInfoId: "H1", expendAmount: "1000" }),
        expenditure({ filerIdent: "8001", filerName: "House Future PAC", expendInfoId: "H2", expendAmount: "2000" }),
      ],
      candidateRows: [
        candidate({
          filerIdent: "8001",
          filerName: "House Future PAC",
          expendInfoId: "H1",
          expendAmount: "1000",
          candidateNameLast: "DOE",
          candidateNameFirst: "JANE",
          candidateSeekOfficeCd: "STATEREP",
          candidateSeekOfficeDistrict: "52",
          candidateSeekOfficeDescr: "State Representative",
        }),
        candidate({
          filerIdent: "8001",
          filerName: "House Future PAC",
          expendInfoId: "H2",
          expendAmount: "2000",
          candidateNameLast: "DOE",
          candidateNameFirst: "JANE",
          candidateSeekOfficeCd: "STATEREP",
          candidateSeekOfficeDistrict: "53",
          candidateSeekOfficeDescr: "State Representative",
        }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        supportTotal: 1000,
        opposeTotal: 0,
        groups: [expect.objectContaining({ committeeId: "8001", amount: 1000 })],
      },
      matchedCandidateExpenditureRowCount: 1,
      includedCandidateExpenditureRowCount: 1,
      skippedCandidateExpenditureRowCount: 0,
    });
  });

  it("skips assist, missing relationship, ambiguous relationship, missing joined expenditure, info-only, bad amount, and wrong cycle", () => {
    const result = aggregateTexasOutsideSpending({
      candidateName: "Greg Abbott",
      candidateCommitteeId: "00012345",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      spacRows: [
        spac({ spacFilerIdent: "7001", spacPositionCd: "ASSIST" }),
        spac({ spacFilerIdent: "7003", spacFilerName: "Conflicted PAC", spacPositionCd: "SUPPORT" }),
        spac({ spacFilerIdent: "7003", spacFilerName: "Conflicted PAC", spacPositionCd: "OPPOSE" }),
      ],
      expenditureRows: [
        expenditure({ expendInfoId: "A1", expendAmount: "1000" }),
        expenditure({ filerIdent: "7003", filerName: "Conflicted PAC", expendInfoId: "A2", expendAmount: "2000" }),
        expenditure({ expendInfoId: "A4", expendAmount: "3000", infoOnlyFlag: "Y" }),
        expenditure({ expendInfoId: "A5", expendAmount: "4000" }),
        expenditure({ expendInfoId: "A6", expendAmount: "5000" }),
      ],
      candidateRows: [
        candidate({ expendInfoId: "A1", expendAmount: "1000" }),
        candidate({ filerIdent: "7003", filerName: "Conflicted PAC", expendInfoId: "A2", expendAmount: "2000" }),
        candidate({ filerIdent: "7004", filerName: "No Relationship PAC", expendInfoId: "A3", expendAmount: "3000" }),
        candidate({ expendInfoId: "A4", expendAmount: "3000" }),
        candidate({ expendInfoId: "A5", expendAmount: "bad" }),
        candidate({ expendInfoId: "A6", expendAmount: "5000", expendDt: "20240101" }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedCandidateExpenditureRowCount: 6,
      includedCandidateExpenditureRowCount: 0,
      skippedCandidateExpenditureRowCount: 6,
    });
  });

  it("maps only support and oppose SPAC positions", () => {
    expect(supportOpposeFromTexasSpacPosition("Support")).toBe("support");
    expect(supportOpposeFromTexasSpacPosition("OPPOSE")).toBe("oppose");
    expect(supportOpposeFromTexasSpacPosition("Assist")).toBeNull();
    expect(supportOpposeFromTexasSpacPosition("Neutral")).toBeNull();
  });

  it("handles empty candidate names and validates unsupported inputs", () => {
    expect(
      aggregateTexasOutsideSpending({
        candidateName: "   ",
        candidateCommitteeId: "00012345",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRows: [candidate()],
        expenditureRows: [expenditure()],
        spacRows: [spac()],
      })
    ).toEqual({
      summary: null,
      matchedCandidateExpenditureRowCount: 0,
      includedCandidateExpenditureRowCount: 0,
      skippedCandidateExpenditureRowCount: 0,
    });

    expect(() =>
      aggregateTexasOutsideSpending({
        candidateName: "Greg Abbott",
        candidateCommitteeId: " ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRows: [],
        expenditureRows: [],
        spacRows: [],
      })
    ).toThrow("Texas candidate committee id is required");

    expect(() =>
      aggregateTexasOutsideSpending({
        candidateName: "Greg Abbott",
        candidateCommitteeId: "00012345",
        officeScope: "statewide",
        officeName: "Mayor",
        electionYear: 2026,
        candidateRows: [],
        expenditureRows: [],
        spacRows: [],
      })
    ).toThrow("Unsupported Texas outside spending office");

    expect(() =>
      aggregateTexasOutsideSpending({
        candidateName: "Jane Doe",
        candidateCommitteeId: "00022222",
        officeScope: "state_lower",
        officeName: "State Representative",
        electionYear: 2026,
        candidateRows: [],
        expenditureRows: [],
        spacRows: [],
      })
    ).toThrow("Texas outside spending district is required");

    expect(() =>
      aggregateTexasOutsideSpending({
        candidateName: "Greg Abbott",
        candidateCommitteeId: "00012345",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2013,
        candidateRows: [],
        expenditureRows: [],
        spacRows: [],
      })
    ).toThrow("Invalid Texas outside spending aggregation election year");

    expect(() =>
      aggregateTexasOutsideSpending({
        candidateName: "Greg Abbott",
        candidateCommitteeId: "00012345",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        maxGroups: 0,
        candidateRows: [],
        expenditureRows: [],
        spacRows: [],
      })
    ).toThrow("Invalid Texas outside spending aggregation maxGroups");
  });
});
