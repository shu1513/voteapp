import { describe, expect, it } from "vitest";

import type { ConnecticutEcrisIndependentExpenditureRow } from "../../../src/pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureParsers.js";
import {
  aggregateConnecticutOutsideSpending,
  normalizeConnecticutOutsideCommitteeId,
} from "../../../src/pipeline/connecticutFinance/connecticutOutsideSpendingAggregator.js";

const SOURCE_URL = "https://seec.ct.gov/eCrisReporting/SearchingIndependentExpenditure.aspx";

function row(overrides: Partial<ConnecticutEcrisIndependentExpenditureRow> = {}): ConnecticutEcrisIndependentExpenditureRow {
  return {
    rootExpenditureId: "0",
    committeeName: "Nutmeg Forward",
    formTag: "SEEC40",
    documentUrl: "https://seec.ct.gov/eCrisReporting/Data/Attachment/Unassigned/SEEC40_July_10_Filing_1.PDF",
    reportType: "July 10 Filing",
    documentType: "Original",
    payee: "Shoreline Digital LLC",
    receivedDate: "2026-06-30",
    fileYear: 2026,
    periodStartDate: "2026-04-01",
    periodEndDate: "2026-06-30",
    amountCents: 100_000,
    formSection: "G. Expenses Paid by Committee",
    supportingCandidates: ["Jane Q Doe"],
    supportingOffices: ["State Representative"],
    opposingCandidates: [],
    opposingOffices: [],
    dataSource: "eFile",
    ...overrides,
  };
}

function aggregate(rows: ConnecticutEcrisIndependentExpenditureRow[], overrides: Record<string, unknown> = {}) {
  return aggregateConnecticutOutsideSpending({
    candidateName: "Jane Doe",
    officeName: "State Lower Chamber Legislator",
    electionYear: 2026,
    expenditureRows: rows,
    sourceUrl: SOURCE_URL,
    ...overrides,
  });
}

describe("connecticutOutsideSpendingAggregator", () => {
  it("sums paid single-candidate lines by committee and stance", () => {
    const result = aggregate([
      row({ amountCents: 100_000 }),
      row({ amountCents: 25_050, payee: "Harbor Media", receivedDate: "2026-06-01" }),
      row({ committeeName: "Hands Off Our Schools", amountCents: 2_470_000, supportingCandidates: [], supportingOffices: [], opposingCandidates: ["Jane Doe"], opposingOffices: ["State Representative"] }),
      row({ committeeName: "River PAC", amountCents: 5_000 }),
      row({ supportingCandidates: ["Someone Else"] }),
    ]);

    expect(result).toEqual({
      summary: {
        supportTotal: 1300.5,
        opposeTotal: 24_700,
        sourceUrl: SOURCE_URL,
        groups: [
          { committeeId: "HANDS OFF OUR SCHOOLS", committeeName: "Hands Off Our Schools", supportOppose: "oppose", amount: 24_700, sourceUrl: SOURCE_URL },
          { committeeId: "NUTMEG FORWARD", committeeName: "Nutmeg Forward", supportOppose: "support", amount: 1250.5, sourceUrl: SOURCE_URL },
          { committeeId: "RIVER PAC", committeeName: "River PAC", supportOppose: "support", amount: 50, sourceUrl: SOURCE_URL },
        ],
      },
      sourceRowCount: 5,
      targetedRowCount: 4,
      includedRowCount: 4,
      skippedMultiCandidateRowCount: 0,
      skippedOfficeMismatchRowCount: 0,
      skippedConflictingStanceRowCount: 0,
      skippedYearMismatchRowCount: 0,
      skippedUnpaidRowCount: 0,
      skippedNonPositiveRowCount: 0,
    });
  });

  it("returns an authoritative zero summary when nothing targets the candidate", () => {
    const result = aggregate([row({ supportingCandidates: ["Someone Else"] })]);

    expect(result.summary).toEqual({ supportTotal: 0, opposeTotal: 0, groups: [], sourceUrl: SOURCE_URL });
    expect(result.targetedRowCount).toBe(0);
  });

  it("skips lines that name several candidates because the amount is not per candidate", () => {
    const result = aggregate([
      row({ supportingCandidates: ["Jane Doe", "Sam Poe"], supportingOffices: ["State Representative", "State Senator"] }),
      row({ supportingCandidates: [], supportingOffices: [], opposingCandidates: ["Ann Coe", "Jane Doe"], opposingOffices: ["State Representative"] }),
    ]);

    expect(result.summary.supportTotal).toBe(0);
    expect(result.summary.opposeTotal).toBe(0);
    expect(result).toMatchObject({ targetedRowCount: 2, includedRowCount: 0, skippedMultiCandidateRowCount: 2 });
  });

  it("requires the line's single office to be the candidate's office", () => {
    const result = aggregate([
      row({ supportingOffices: ["State Senator"] }),
      row({ supportingOffices: ["State Representative", "State Senator"] }),
      row({ supportingOffices: [] }),
      row({ supportingOffices: ["First Selectman"] }),
    ]);

    expect(result).toMatchObject({ targetedRowCount: 4, includedRowCount: 0, skippedOfficeMismatchRowCount: 4 });
  });

  it("maps every eligible eCRIS office label to the app office name", () => {
    const governor = aggregateConnecticutOutsideSpending({
      candidateName: "Erin Stewart",
      officeName: "Governor",
      electionYear: 2026,
      expenditureRows: [
        row({ supportingCandidates: [], supportingOffices: [], opposingCandidates: ["Erin E Stewart"], opposingOffices: ["Governor"], amountCents: 2_867_352 }),
      ],
    });
    expect(governor.summary).toMatchObject({ supportTotal: 0, opposeTotal: 28_673.52 });

    const senator = aggregateConnecticutOutsideSpending({
      candidateName: "Maryam Khan",
      officeName: "State Senator",
      electionYear: 2026,
      expenditureRows: [row({ supportingCandidates: ["Maryam Khan"], supportingOffices: ["State Senator"], amountCents: 4_500_000, opposingCandidates: ["Doug McCrory", "Ayana Taylor"], opposingOffices: ["State Senator"] })],
    });
    expect(senator.summary).toMatchObject({ supportTotal: 45_000, opposeTotal: 0 });
  });

  it("skips a line that names the candidate on both sides", () => {
    const result = aggregate([
      row({ opposingCandidates: ["Jane Doe"], opposingOffices: ["State Representative"] }),
    ]);

    expect(result).toMatchObject({ targetedRowCount: 1, includedRowCount: 0, skippedConflictingStanceRowCount: 1 });
  });

  it("counts only paid sections of the election year with a positive amount", () => {
    const result = aggregate([
      row({ formSection: "I. Expenses Incurred by Committee but Not Paid" }),
      row({ formSection: "J. Itemization of Reimbursements to Committee Workers and Consultants" }),
      row({ formSection: "" }),
      row({ formSection: "P. Expenses Paid by Committee", amountCents: 700 }),
      row({ fileYear: 2025 }),
      row({ amountCents: null }),
      row({ amountCents: 0 }),
      row({ amountCents: -500 }),
    ]);

    expect(result.summary.supportTotal).toBe(7);
    expect(result).toMatchObject({
      targetedRowCount: 8,
      includedRowCount: 1,
      skippedYearMismatchRowCount: 1,
      skippedUnpaidRowCount: 3,
      skippedNonPositiveRowCount: 3,
    });
  });

  it("matches names the way the committee resolver does: nicknames one-sided, middle initials as evidence", () => {
    const nickname = aggregateConnecticutOutsideSpending({
      candidateName: "Timothy Ackert",
      officeName: "State Lower Chamber Legislator",
      electionYear: 2026,
      expenditureRows: [row({ supportingCandidates: ["Tim Ackert"] })],
    });
    expect(nickname.includedRowCount).toBe(1);

    const middleConflict = aggregateConnecticutOutsideSpending({
      candidateName: "Jane A. Doe",
      officeName: "State Lower Chamber Legislator",
      electionYear: 2026,
      expenditureRows: [row({ supportingCandidates: ["Jane B Doe"] })],
    });
    expect(middleConflict).toMatchObject({ targetedRowCount: 0, includedRowCount: 0 });

    const committeeStyleName = aggregateConnecticutOutsideSpending({
      candidateName: "Susan Bysiewicz",
      officeName: "Lieutenant Governor",
      electionYear: 2026,
      expenditureRows: [row({ supportingCandidates: ["Bysiewicz for CT"], supportingOffices: ["Lieutenant Governor"] })],
    });
    expect(committeeStyleName.targetedRowCount).toBe(0);
  });

  it("caps groups per stance after sorting by amount", () => {
    const result = aggregate(
      [
        row({ committeeName: "A PAC", amountCents: 100 }),
        row({ committeeName: "B PAC", amountCents: 300 }),
        row({ committeeName: "C PAC", amountCents: 200 }),
        row({ committeeName: "D PAC", amountCents: 900, supportingCandidates: [], supportingOffices: [], opposingCandidates: ["Jane Doe"], opposingOffices: ["State Representative"] }),
      ],
      { maxGroupsPerStance: 2 }
    );

    expect(result.summary.groups.map((group) => [group.committeeName, group.supportOppose])).toEqual([
      ["D PAC", "oppose"],
      ["B PAC", "support"],
      ["C PAC", "support"],
    ]);
    expect(result.summary.supportTotal).toBe(6);
  });

  it("normalizes committee names into stable ids", () => {
    expect(normalizeConnecticutOutsideCommitteeId("Impact CT, Inc.")).toBe("IMPACT CT INC");
    expect(normalizeConnecticutOutsideCommitteeId("  Hands  Off Our Schools ")).toBe("HANDS OFF OUR SCHOOLS");
    expect(normalizeConnecticutOutsideCommitteeId("Café & Vote PAC")).toBe("CAFE AND VOTE PAC");
  });

  it("validates inputs", () => {
    expect(() => aggregate([], { candidateName: " " })).toThrow("candidate name is required");
    expect(() => aggregate([], { officeName: "" })).toThrow("office name is required");
    expect(() => aggregate([], { electionYear: 2007 })).toThrow("Invalid Connecticut outside spending election year");
    expect(() => aggregate([], { maxGroupsPerStance: 0 })).toThrow("maxGroupsPerStance");
    expect(() => aggregate([row({ committeeName: "  " })])).toThrow("has no committee name");
  });
});
