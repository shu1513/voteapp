import { describe, expect, it } from "vitest";

import { aggregateMissouriOutsideSpending } from "../../../src/pipeline/missouriFinance/missouriOutsideSpendingAggregator.js";
import type { MissouriMecOutsideSpendingRow } from "../../../src/pipeline/missouriFinance/missouriMecParsers.js";

function row(overrides: Partial<MissouriMecOutsideSpendingRow> = {}): MissouriMecOutsideSpendingRow {
  return {
    candidateNameAndAddress: "Jane A Doe 10 Private St Jefferson City MO 65101",
    officeSought: "District 1 Missouri House of Representatives",
    supportOppose: "Support",
    expenditureDate: "2026-10-20",
    amountCents: 100_00,
    reportingCommittee: "Example PAC",
    report: "8 Day Before General Election-11/3/2026",
    ...overrides,
  };
}

const identities = [
  { reportingCommittee: "Example PAC", mecid: "C123456" },
  { reportingCommittee: "Second PAC", mecid: "C654321" },
];

describe("aggregateMissouriOutsideSpending", () => {
  it("matches exact candidate/office evidence and builds MECID-keyed stance groups", () => {
    const result = aggregateMissouriOutsideSpending({
      rows: [
        row(),
        row({ supportOppose: "Oppose", amountCents: 50_25, reportingCommittee: "Second PAC" }),
        row({ candidateNameAndAddress: "Other Person 20 Private St", amountCents: 999_00 }),
      ],
      identities, candidateName: "Jane Doe", officeName: "State Lower Chamber Legislator", district: "1",
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03", sourceUrl: "https://example.test/outside",
    });
    expect(result).toMatchObject({ supportTotal: 100, opposeTotal: 50.25, attributedRowCount: 2, attributedAmount: 150.25 });
    expect(result.outsideGroups).toEqual([
      { committeeId: "C123456", committeeName: "Example PAC", supportOppose: "support", amount: 100, sourceUrl: "https://example.test/outside" },
      { committeeId: "C654321", committeeName: "Second PAC", supportOppose: "oppose", amount: 50.25, sourceUrl: "https://example.test/outside" },
    ]);
  });

  it("quarantines cross-report composite collisions instead of guessing timely supersession", () => {
    const result = aggregateMissouriOutsideSpending({
      rows: [
        row({ amountCents: 10_46, report: "AMENDED 24 Hour Expenditure Report-11/3/2026 General" }),
        row({ amountCents: 10_46, report: "October Quarterly Report" }),
        // Same amount/date in one report can be two legitimate expenditures; keep both.
        row({ amountCents: 20_00, expenditureDate: "2026-10-21" }),
        row({ amountCents: 20_00, expenditureDate: "2026-10-21" }),
      ],
      identities, candidateName: "Jane Doe", officeName: "State Lower Chamber Legislator", district: "1",
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03",
    });
    expect(result).toMatchObject({
      supportTotal: 40, attributedRowCount: 2,
      ambiguousLineageRowCount: 2, ambiguousLineageAmount: 20.92,
      ambiguousTimelyLineageRowCount: 2,
    });
  });

  it("reports each fail-closed gate in rows and excluded dollars", () => {
    const result = aggregateMissouriOutsideSpending({
      rows: [
        row({ officeSought: "State Senate", amountCents: 10_00 }),
        row({ expenditureDate: "2026-07-01", amountCents: 20_00 }),
        row({ reportingCommittee: "Unknown PAC", amountCents: 30_00 }),
        row({ amountCents: 0 }),
      ],
      identities, candidateName: "Jane Doe", officeName: "State Lower Chamber Legislator", district: "1",
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03",
    });
    expect(result).toMatchObject({
      candidateNameRowCount: 4,
      candidateOfficeMismatchRowCount: 1, candidateOfficeMismatchAmount: 10,
      outOfCycleRowCount: 1, outOfCycleAmount: 20,
      unresolvedSpenderRowCount: 1, unresolvedSpenderAmount: 30,
      malformedAmountRowCount: 1, malformedAmount: 0,
      attributedRowCount: 0,
    });
  });

  it("rejects district conflicts and invalid cycle inputs", () => {
    expect(aggregateMissouriOutsideSpending({
      rows: [row({ officeSought: "District 2 Missouri House of Representatives" })], identities,
      candidateName: "Jane Doe", officeName: "State Lower Chamber Legislator", district: "1",
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03",
    }).candidateOfficeMismatchRowCount).toBe(1);
    expect(() => aggregateMissouriOutsideSpending({
      rows: [], identities, candidateName: "Jane Doe", officeName: "State Lower Chamber Legislator",
      cycleStart: "2026-11-03", cycleEnd: "2026-08-05",
    })).toThrow("cycle start is after cycle end");
  });

  it("accepts observed Missouri legislative aliases but rejects federal and conflicting districts", () => {
    const lower = aggregateMissouriOutsideSpending({
      rows: [
        row({ officeSought: "House 1" }),
        row({ officeSought: "HD-1" }),
        row({ officeSought: "MO 1 Representative" }),
        row({ officeSought: "U.S. Representative, 1st" }),
        row({ officeSought: "House 2" }),
      ],
      identities, candidateName: "Jane Doe", officeName: "State Lower Chamber Legislator", district: "1",
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03",
    });
    expect(lower).toMatchObject({ attributedRowCount: 3, candidateOfficeMismatchRowCount: 2 });

    const upper = aggregateMissouriOutsideSpending({
      rows: [
        row({ officeSought: "SD1" }),
        row({ officeSought: "Senate 1" }),
        row({ officeSought: "State Senator 1 District" }),
        row({ officeSought: "SD2" }),
      ],
      identities, candidateName: "Jane Doe", officeName: "State Senator", district: "1",
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03",
    });
    expect(upper).toMatchObject({ attributedRowCount: 3, candidateOfficeMismatchRowCount: 1 });
  });
});
