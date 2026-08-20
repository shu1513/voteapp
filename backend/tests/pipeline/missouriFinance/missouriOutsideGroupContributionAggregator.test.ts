import { describe, expect, it } from "vitest";

import { aggregateMissouriOutsideGroupContributions } from "../../../src/pipeline/missouriFinance/missouriOutsideGroupContributionAggregator.js";
import type { MissouriMecContributionRow } from "../../../src/pipeline/missouriFinance/missouriMecParsers.js";

function contribution(overrides: Partial<MissouriMecContributionRow> = {}): MissouriMecContributionRow {
  return {
    mecid: "C123456", committeeName: "Example PAC", report: "October Quarterly Report",
    contributorCommittee: null, contributorCompany: "Acme Corp", contributorLastName: null,
    contributorFirstName: null, employer: null, occupation: null, contributionDate: "2026-09-01",
    amountCents: 100_00, contributionKind: "Monetary", ...overrides,
  };
}

const inventory = [{
  reportId: "1", report: "October Quarterly Report", dateFiled: "2026-10-15", isAmended: false,
  lineageKey: "OCTOBER QUARTERLY REPORT",
}];
const groups = [
  { committeeId: "C123456", committeeName: "Example PAC", supportOppose: "support" as const, amount: 500, sourceUrl: null },
  { committeeId: "C123456", committeeName: "Example PAC", supportOppose: "oppose" as const, amount: 50, sourceUrl: null },
];

describe("aggregateMissouriOutsideGroupContributions", () => {
  it("aggregates structured organization donors onto each spender stance", () => {
    const result = aggregateMissouriOutsideGroupContributions({
      outsideGroups: groups,
      artifactsBySpender: new Map([["C123456", {
        inventory,
        contributionRows: [
          contribution(), contribution({ contributorCompany: null, contributorCommittee: "Builders PAC", amountCents: 200_00 }),
          contribution({ amountCents: 50_00, contributionKind: "In-Kind" }),
        ],
        sourceUrl: "https://example.test/contributions",
      }]]),
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03",
    });
    expect(result).toMatchObject({ includedContributionRowCount: 3, individualContributionRowCount: 0 });
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({ supportOppose: "oppose", categoryName: "Builders PAC", amount: 200 }),
      expect.objectContaining({ supportOppose: "oppose", categoryName: "Acme Corp", amount: 150 }),
      expect.objectContaining({ supportOppose: "support", categoryName: "Builders PAC", amount: 200 }),
      expect.objectContaining({ supportOppose: "support", categoryName: "Acme Corp", amount: 150 }),
    ]);
  });

  it("excludes individuals, off-cycle, non-positive, and conflicting organization fields", () => {
    const result = aggregateMissouriOutsideGroupContributions({
      outsideGroups: groups.slice(0, 1),
      artifactsBySpender: new Map([["C123456", {
        inventory,
        contributionRows: [
          contribution({ contributorCompany: null, contributorLastName: "Doe", contributorFirstName: "Jane" }),
          contribution({ contributionDate: "2026-07-01" }),
          contribution({ amountCents: -10_00 }),
          contribution({ contributorCommittee: "One PAC", contributorCompany: "Other LLC", amountCents: 20_00 }),
          contribution({ contributionKind: "Mystery", amountCents: 30_00 }),
        ],
        sourceUrl: "https://example.test",
      }]]),
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03",
    });
    expect(result).toMatchObject({
      individualContributionRowCount: 1, outsideCycleContributionRowCount: 1,
      nonPositiveContributionRowCount: 1, ambiguousOrganizationRowCount: 1, ambiguousOrganizationAmount: 20,
      unrecognizedContributionKindRowCount: 1, unrecognizedContributionKindAmount: 30,
    });
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });

  it("returns excluded-dollar diagnostics for ambiguous amendment lineages", () => {
    const amendedInventory = [inventory[0]!, {
      reportId: "2", report: "AMENDED October Quarterly Report", dateFiled: "2026-10-16", isAmended: true,
      lineageKey: "OCTOBER QUARTERLY REPORT",
    }];
    const result = aggregateMissouriOutsideGroupContributions({
      outsideGroups: groups.slice(0, 1),
      artifactsBySpender: new Map([["C123456", {
        inventory: amendedInventory,
        contributionRows: [contribution(), contribution({ report: "AMENDED October Quarterly Report", amountCents: 50_00 })],
        sourceUrl: "https://example.test",
      }]]),
      cycleStart: "2026-08-05", cycleEnd: "2026-11-03",
    });
    expect(result.reportDiagnostics).toEqual([expect.objectContaining({
      mecid: "C123456", reason: "ambiguous_amendment", excludedRowCount: 2, excludedAmountCents: 15000,
    })]);
  });
});
