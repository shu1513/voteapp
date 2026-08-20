import { describe, expect, it } from "vitest";

import { aggregateMissouriDirectFinance } from "../../../src/pipeline/missouriFinance/missouriDirectContributionAggregator.js";
import type { MissouriMecContributionRow, MissouriMecExpenditureRow, MissouriMecReportInventoryRow } from "../../../src/pipeline/missouriFinance/missouriMecParsers.js";

const report = "8 Day Before General Election-11/3/2026";
const inventory: MissouriMecReportInventoryRow[] = [{ reportId: "1", report, dateFiled: "2026-10-26", isAmended: false, lineageKey: report.toUpperCase() }];

function contribution(overrides: Partial<MissouriMecContributionRow> = {}): MissouriMecContributionRow {
  return {
    mecid: "C263985", committeeName: "Example Committee", report, contributorCommittee: null,
    contributorCompany: null, contributorLastName: "Doe", contributorFirstName: "Jane", employer: null,
    occupation: "Engineer", contributionDate: "2026-09-01", amountCents: 10000, contributionKind: "Monetary", ...overrides,
  };
}

function expenditure(overrides: Partial<MissouriMecExpenditureRow> = {}): MissouriMecExpenditureRow {
  return {
    mecid: "C263985", committeeName: "Example Committee", report, payeeLastName: null, payeeFirstName: null,
    payeeCompany: "Printer", purpose: "Signs", expenditureDate: "2026-09-01", amountCents: 30000,
    expenditureType: "Paid", ...overrides,
  };
}

describe("aggregateMissouriDirectFinance", () => {
  it("uses general-cycle dates, cash contribution types, paid expenditures, occupation and size buckets", () => {
    const result = aggregateMissouriDirectFinance({
      inventory,
      cycleStart: "2026-08-05",
      cycleEnd: "2026-11-03",
      contributionRows: [
        contribution(),
        contribution({ contributorLastName: "Roe", occupation: "Retired", amountCents: 60000 }),
        contribution({ contributorLastName: "Refund", amountCents: -2000 }),
        contribution({ contributorLastName: "Gift", amountCents: 5000, contributionKind: "In-Kind" }),
        contribution({ contributorLastName: "Primary", contributionDate: "2026-08-04", amountCents: 99900 }),
      ],
      expenditureRows: [
        expenditure(),
        expenditure({ amountCents: 5000, expenditureType: "Incurred" }),
        expenditure({ amountCents: 7000, expenditureDate: "2026-08-04" }),
      ],
      sourceUrl: "https://example.test",
    });
    expect(result.directContributionTotal).toBe(680);
    expect(result.totalDisbursements).toBe(300);
    expect(result.inKindAmount).toBe(50);
    expect(result.incurredExpenditureAmount).toBe(50);
    expect(result.outsideCycleContributionRowCount).toBe(1);
    expect(result.outsideCycleExpenditureRowCount).toBe(1);
    expect(result.directBreakdowns).toEqual(expect.arrayContaining([
      expect.objectContaining({ categoryType: "occupation", categoryName: "Retired", amount: 600 }),
      expect.objectContaining({ categoryType: "contribution_size", categoryName: "$100-$249", amount: 100 }),
      expect.objectContaining({ categoryType: "contribution_size", categoryName: "$500-$999", amount: 600 }),
    ]));
  });

  it("does not let an out-of-cycle amendment ambiguity block a general-cycle total", () => {
    const april = "April Quarterly Report";
    const result = aggregateMissouriDirectFinance({
      inventory: [
        { reportId: "1", report: april, dateFiled: "2026-04-13", isAmended: false, lineageKey: april.toUpperCase() },
        { reportId: "2", report: `AMENDED ${april}`, dateFiled: "2026-04-15", isAmended: true, lineageKey: april.toUpperCase() },
      ],
      cycleStart: "2026-08-05",
      cycleEnd: "2026-11-03",
      contributionRows: [
        contribution({ report: april, contributionDate: "2026-03-01", amountCents: 30000 }),
        contribution({ report: `AMENDED ${april}`, contributionDate: "2026-03-01", amountCents: 12500 }),
      ],
      expenditureRows: [],
    });
    expect(result.outsideCycleContributionRowCount).toBe(2);
    expect(result.contributionReportDiagnostics).toEqual([]);
    expect(result.directContributionTotal).toBe(0);
  });
});
