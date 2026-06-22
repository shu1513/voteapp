import { describe, expect, it } from "vitest";

import { buildOklahomaGuardianIeOutsideFinanceSnapshot } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianIeOutsideSpendingAggregator.js";
import type { OklahomaGuardianIeOutsideSpendingReport } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianIeOutsideSpendingDiscovery.js";

function report(
  input: Partial<OklahomaGuardianIeOutsideSpendingReport> & {
    spenderName: string;
    supportOppose: "support" | "oppose";
    amount: number;
  }
): OklahomaGuardianIeOutsideSpendingReport {
  return {
    rowIndex: 0,
    sourceRow: {
      filerName: input.spenderName,
      reportDescription: "IE EC SQ Report",
      periodBegin: null,
      periodEnd: null,
      filedDate: null,
      viewReportTarget: "target",
    },
    candidateName: "KEVIN STITT",
    officeName: "GOVERNOR",
    reportingPeriodBegin: null,
    reportingPeriodEnd: null,
    reportDescription: "IE EC SQ Report",
    amended: false,
    sourceUrl: "https://guardian.ok.gov/PublicSite/PublicReports/IndependentExpenditure.aspx",
    pdfByteLength: 100,
    ...input,
  };
}

describe("Oklahoma Guardian IE outside-spending aggregator", () => {
  it("builds summary totals and grouped outside spenders", () => {
    const snapshot = buildOklahomaGuardianIeOutsideFinanceSnapshot([
      report({ spenderName: "The Oklahoma Project", supportOppose: "oppose", amount: 100.1 }),
      report({ spenderName: " THE OKLAHOMA PROJECT ", supportOppose: "oppose", amount: 200.2 }),
      report({ spenderName: "Oklahoma Families First", supportOppose: "support", amount: 50 }),
    ]);

    expect(snapshot.summary).toEqual({
      outsideSupportTotal: 50,
      outsideOpposeTotal: 300.3,
      sourceUrl: "https://guardian.ok.gov/PublicSite/PublicReports/IndependentExpenditure.aspx",
    });
    expect(snapshot.outsideGroups).toEqual([
      {
        committeeId: "THE OKLAHOMA PROJECT",
        committeeName: "The Oklahoma Project",
        supportOppose: "oppose",
        amount: 300.3,
        sourceUrl: "https://guardian.ok.gov/PublicSite/PublicReports/IndependentExpenditure.aspx",
      },
      {
        committeeId: "OKLAHOMA FAMILIES FIRST",
        committeeName: "Oklahoma Families First",
        supportOppose: "support",
        amount: 50,
        sourceUrl: "https://guardian.ok.gov/PublicSite/PublicReports/IndependentExpenditure.aspx",
      },
    ]);
  });

  it("keeps support and opposition rows separate for the same spender", () => {
    const snapshot = buildOklahomaGuardianIeOutsideFinanceSnapshot([
      report({ spenderName: "Example PAC", supportOppose: "support", amount: 10 }),
      report({ spenderName: "Example PAC", supportOppose: "oppose", amount: 20 }),
    ]);

    expect(snapshot.summary.outsideSupportTotal).toBe(10);
    expect(snapshot.summary.outsideOpposeTotal).toBe(20);
    expect(snapshot.outsideGroups).toEqual([
      expect.objectContaining({ committeeId: "EXAMPLE PAC", supportOppose: "oppose", amount: 20 }),
      expect.objectContaining({ committeeId: "EXAMPLE PAC", supportOppose: "support", amount: 10 }),
    ]);
  });

  it("returns explicit zero totals for an empty usable report set", () => {
    expect(buildOklahomaGuardianIeOutsideFinanceSnapshot([])).toEqual({
      summary: {
        outsideSupportTotal: 0,
        outsideOpposeTotal: 0,
        sourceUrl: null,
      },
      outsideGroups: [],
    });
  });

  it("rejects invalid amounts", () => {
    expect(() =>
      buildOklahomaGuardianIeOutsideFinanceSnapshot([
        report({ spenderName: "Example PAC", supportOppose: "support", amount: -1 }),
      ])
    ).toThrow("Invalid Oklahoma Guardian IE outside spending amount");
  });
});
