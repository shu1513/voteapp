import { describe, expect, it } from "vitest";

import {
  SOUTH_CAROLINA_DIRECT_COVERAGE_NOTE,
  aggregateSouthCarolinaDirectFinance,
  selectSouthCarolinaAcceptedRuns,
  southCarolinaContributionYearsForRuns,
} from "../../../src/pipeline/southCarolinaFinance/southCarolinaDirectContributionAggregator.js";
import type {
  SouthCarolinaCandidateReportRow,
  SouthCarolinaContributionSearchRow,
  SouthCarolinaReportDetails,
} from "../../../src/pipeline/southCarolinaFinance/southCarolinaEthicsClient.js";

function reportRow(overrides: Partial<SouthCarolinaCandidateReportRow>): SouthCarolinaCandidateReportRow {
  return {
    reportId: 1,
    reportName: "Quarter 2, 2026 Report",
    reportType: "Quarterly",
    electionDate: "6/9/2026",
    contributions: 0,
    expenses: 0,
    balance: 0,
    dateSubmitted: "2026-07-10T12:00:00",
    campaignId: 100,
    candidateFilerId: 54395,
    filingStartDate: "2026-05-21T04:00:00",
    filingEndDate: "2026-06-30T00:00:00",
    isPrimary: true,
    isGeneral: false,
    isPreElection: false,
    isFinal: false,
    ...overrides,
  };
}

function details(input: {
  cash?: number;
  inKind?: number;
  personal?: number;
  loans?: number;
  credits?: number;
  expTotal?: number;
  endingBalance?: number;
  extraIncome?: { type: string; electionCycleTotal: number }[];
}): SouthCarolinaReportDetails {
  const cash = input.cash ?? 0;
  const inKind = input.inKind ?? 0;
  const personal = input.personal ?? 0;
  const loans = input.loans ?? 0;
  const credits = input.credits ?? 0;
  const incomeTotal = cash + inKind + personal + loans + credits;
  const expTotal = input.expTotal ?? 0;
  const line = (type: string, electionCycleTotal: number) => ({ type, filingPeriod: 0, electionCycleTotal });
  return {
    filerName: "Evette, Pamela S",
    electionDate: "2026-06-09T04:00:00",
    electionType: "Primary",
    reportType: "Quarter 2, 2026 Report",
    filingPeriod: "5/21/2026 - 6/30/2026",
    isAmendment: false,
    reportSequenceNumber: 1,
    contributionsTotal: 0,
    expendituresTotal: 0,
    income: [
      line("Cash Contributions", cash),
      line("In-kind Contributions", inKind),
      line("Personal Contributions", personal),
      line("Loans", loans),
      line("Account Credits", credits),
      ...(input.extraIncome ?? []).map((extra) => line(extra.type, extra.electionCycleTotal)),
      line("Total", incomeTotal),
    ],
    expenditures: [line("Expenditures", expTotal), line("Total", expTotal)],
    totals: [{ totalType: "Campaign Funds", startingBalance: 0, endingBalance: input.endingBalance ?? 0 }],
    versions: [{ id: 1, name: "Original Report" }],
  };
}

function contributionRow(
  overrides: Partial<SouthCarolinaContributionSearchRow>
): SouthCarolinaContributionSearchRow {
  return {
    contributionId: 1,
    candidateId: 54395,
    officeRunId: 100,
    candidateName: "Pamela Evette",
    officeName: "4",
    electionDate: "2026-06-09T05:00:00",
    date: "2026-05-01T04:00:00",
    amount: 0,
    contributorName: "Test Person",
    contributorOccupation: "Attorney",
    group: "No",
    description: null,
    ...overrides,
  };
}

describe("selectSouthCarolinaAcceptedRuns", () => {
  it("groups the election year's reports into runs by campaignId (McMaster cycle-reset shape)", () => {
    const reports = [
      reportRow({ reportId: 1, campaignId: 100, electionDate: "6/14/2022", contributions: 5528030.35, filingEndDate: "2022-06-30T00:00:00" }),
      reportRow({ reportId: 2, campaignId: 200, electionDate: "11/8/2022", contributions: 2103841.11, filingEndDate: "2022-10-15T00:00:00" }),
      reportRow({ reportId: 3, campaignId: 300, electionDate: "6/12/2018", filingEndDate: "2018-06-30T00:00:00" }),
    ];
    const runs = selectSouthCarolinaAcceptedRuns(reports, 2022);
    expect(runs.map((run) => run.campaignId)).toEqual([100, 200]);
    expect(runs.map((run) => run.finalReport.reportId)).toEqual([1, 2]);
  });

  it("picks the run's final report by filing-period end, never submission timestamp (Evette amendment trap)", () => {
    const preElection = reportRow({
      reportId: 430061,
      filingEndDate: "2026-05-20T00:00:00",
      // Amended AFTER Q2 was filed.
      dateSubmitted: "2026-07-14T10:00:00",
    });
    const quarterTwo = reportRow({
      reportId: 426225,
      filingEndDate: "2026-06-30T00:00:00",
      dateSubmitted: "2026-07-10T14:45:49",
    });
    const runs = selectSouthCarolinaAcceptedRuns([preElection, quarterTwo], 2026);
    expect(runs).toHaveLength(1);
    expect(runs[0]!.finalReport.reportId).toBe(426225);
    // Both reports sit in the same election-date phase, so the run has one.
    expect(runs[0]!.phaseFinals.map((report) => report.reportId)).toEqual([426225]);
  });

  it("splits one campaignId into election-date phases (Dianne Mitchell 2026 shape)", () => {
    // Live shape, filer 57316 run 79388: the primary phase's reports and the
    // general-phase Quarter 2 report share ONE campaignId, and the cumulative
    // restarts at the boundary.
    const reports = [
      reportRow({ reportId: 429745, campaignId: 79388, electionDate: "6/9/2026", filingEndDate: "2026-03-31T00:00:00" }),
      reportRow({ reportId: 427606, campaignId: 79388, electionDate: "6/9/2026", filingEndDate: "2026-05-20T00:00:00" }),
      reportRow({ reportId: 426434, campaignId: 79388, electionDate: "11/3/2026", filingEndDate: "2026-06-30T00:00:00" }),
    ];
    const runs = selectSouthCarolinaAcceptedRuns(reports, 2026, ["6/9/2026", "11/3/2026"]);
    expect(runs).toHaveLength(1);
    // One final per phase, oldest first — the primary's last report survives
    // instead of being shadowed by the general's.
    expect(runs[0]!.phaseFinals.map((report) => report.reportId)).toEqual([427606, 426434]);
    // Cash on hand still comes from the chronologically newest report.
    expect(runs[0]!.finalReport.reportId).toBe(426434);
  });
});

describe("southCarolinaContributionYearsForRuns", () => {
  it("returns every calendar year touched by accepted-run filing periods", () => {
    const reports = [
      reportRow({ reportId: 1, campaignId: 100, filingStartDate: "2025-01-01T04:00:00", filingEndDate: "2025-03-31T00:00:00", electionDate: "11/3/2026" }),
      reportRow({ reportId: 2, campaignId: 100, filingStartDate: "2026-04-01T04:00:00", filingEndDate: "2026-06-30T00:00:00", electionDate: "11/3/2026" }),
      reportRow({ reportId: 3, campaignId: 900, filingStartDate: "2021-01-01T04:00:00", filingEndDate: "2021-03-31T00:00:00", electionDate: "11/2/2021" }),
    ];
    expect(southCarolinaContributionYearsForRuns(reports, 2026)).toEqual([2025, 2026]);
  });
});

describe("aggregateSouthCarolinaDirectFinance", () => {
  it("returns no_filed_reports when no reports fall in the election year", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ electionDate: "11/8/2022" })],
      detailsByReportId: new Map(),
      contributionRows: [],
    });
    expect(result).toEqual({ status: "no_filed_reports" });
  });

  it("sums both election-date phases of one run (Dianne Mitchell live numbers)", () => {
    // Filer 57316, run 79388: the primary phase ends at the Pre-Election
    // report and the general phase restarts at Quarter 2. Reading only the
    // newest report would report $9,450 raised against $24,839.99 of itemized
    // rows — the fail-closed error this shape used to produce.
    const primaryPhaseFinal = reportRow({
      reportId: 427606,
      campaignId: 79388,
      electionDate: "6/9/2026",
      contributions: 108639.99,
      expenses: 95232.76,
      balance: 13407.23,
      filingEndDate: "2026-05-20T00:00:00",
    });
    const generalPhaseFinal = reportRow({
      reportId: 426434,
      campaignId: 79388,
      electionDate: "11/3/2026",
      contributions: 14450,
      expenses: 24922.22,
      balance: 2935.01,
      filingEndDate: "2026-06-30T00:00:00",
    });
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 57316,
      electionYear: 2026,
      reports: [primaryPhaseFinal, generalPhaseFinal],
      detailsByReportId: new Map([
        [427606, details({ cash: 15310.81, inKind: 79.18, loans: 93250, expTotal: 95232.76, endingBalance: 13407.23 })],
        [426434, details({ cash: 9450, loans: 5000, expTotal: 24922.22, endingBalance: 2935.01 })],
      ]),
      contributionRows: [
        contributionRow({ contributionId: 1, candidateId: 57316, officeRunId: 79388, amount: 15389.99, contributorName: "Primary Donors" }),
        contributionRow({ contributionId: 2, candidateId: 57316, officeRunId: 79388, amount: 9450, contributorName: "General Donors" }),
      ],
      acceptedElectionDates: ["6/9/2026", "11/3/2026"],
    });

    expect(result).toMatchObject({
      status: "aggregated",
      runCount: 1,
      // Both phases' cumulatives, loans excluded from the direct total.
      totalReceipts: 123089.99,
      directContributionTotal: 24839.99,
      totalDisbursements: 120154.98,
      // Newest phase's balance, not the sum.
      cashOnHand: 2935.01,
      // Itemized rows reach the full direct total across both phases.
      directCoverageNote: null,
      includedContributionRowCount: 2,
    });
  });

  it("sums totals across runs, excludes loans and credits from the direct total, and takes cash on hand from the latest report", () => {
    const primaryFinal = reportRow({
      reportId: 10,
      campaignId: 100,
      electionDate: "6/9/2026",
      contributions: 1500,
      expenses: 900,
      balance: 600,
      filingEndDate: "2026-06-30T00:00:00",
    });
    const generalFinal = reportRow({
      reportId: 20,
      campaignId: 200,
      electionDate: "11/3/2026",
      contributions: 1310,
      expenses: 300,
      balance: 1610,
      filingEndDate: "2026-10-15T00:00:00",
    });
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [primaryFinal, generalFinal],
      detailsByReportId: new Map([
        [10, details({ cash: 1000, inKind: 100, personal: 0, loans: 400, credits: 0, expTotal: 900, endingBalance: 600 })],
        [20, details({ cash: 1200, inKind: 0, personal: 100, loans: 0, credits: 10, expTotal: 300, endingBalance: 1610 })],
      ]),
      contributionRows: [
        contributionRow({ contributionId: 1, officeRunId: 100, amount: 1000, contributorName: "Alice Donor", contributorOccupation: "Attorney" }),
        contributionRow({ contributionId: 2, officeRunId: 100, amount: 100, contributorName: "GOOD PAC", group: "Yes", contributorOccupation: null }),
        contributionRow({ contributionId: 3, officeRunId: 200, amount: 1200, contributorName: "Alice Donor", contributorOccupation: "Attorney" }),
        contributionRow({ contributionId: 4, officeRunId: 200, amount: 100, contributorName: "Pamela Evette", contributorOccupation: "Business Owner" }),
        // Older-cycle row in a requested calendar year: ignored.
        contributionRow({ contributionId: 5, officeRunId: 900, amount: 55 }),
      ],
    });
    expect(result).toMatchObject({
      status: "aggregated",
      totalReceipts: 2810,
      directContributionTotal: 2400,
      totalDisbursements: 1200,
      cashOnHand: 1610,
      directCoverageNote: null,
      runCount: 2,
      includedContributionRowCount: 4,
      otherRunContributionRowCount: 1,
    });
    if (result.status !== "aggregated") throw new Error("unreachable");
    const occupations = result.directBreakdowns.filter((row) => row.categoryType === "occupation");
    expect(occupations).toEqual([
      { categoryType: "occupation", categoryName: "Attorney", amount: 2200, contributorCount: 1, sourceUrl: null },
      { categoryType: "occupation", categoryName: "Business Owner", amount: 100, contributorCount: 1, sourceUrl: null },
    ]);
    const buckets = result.directBreakdowns.filter((row) => row.categoryType === "contribution_size");
    expect(buckets).toEqual([
      { categoryType: "contribution_size", categoryName: "$1,000-$4,999", amount: 2200, contributorCount: 1, sourceUrl: null },
      { categoryType: "contribution_size", categoryName: "$100-$249", amount: 200, contributorCount: 2, sourceUrl: null },
    ]);
  });

  it("publishes partial breakdowns with the coverage note when itemized rows lawfully undershoot", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 500, expenses: 0, balance: 500 })],
      detailsByReportId: new Map([[10, details({ cash: 500, endingBalance: 500 })]]),
      contributionRows: [contributionRow({ contributionId: 1, amount: 300 })],
    });
    expect(result).toMatchObject({
      status: "aggregated",
      directContributionTotal: 500,
      directCoverageNote: SOUTH_CAROLINA_DIRECT_COVERAGE_NOTE,
    });
  });

  it("represents a filed-zero run as zeros with no coverage note", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10 })],
      detailsByReportId: new Map([[10, details({})]]),
      contributionRows: [],
    });
    expect(result).toMatchObject({
      status: "aggregated",
      totalReceipts: 0,
      directContributionTotal: 0,
      totalDisbursements: 0,
      cashOnHand: 0,
      directBreakdowns: [],
      directCoverageNote: null,
    });
  });

  it("fails closed when the itemized sum exceeds the summary contributions", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 100, balance: 100 })],
      detailsByReportId: new Map([[10, details({ cash: 100, endingBalance: 100 })]]),
      contributionRows: [contributionRow({ contributionId: 1, amount: 150 })],
    });
    expect(result).toMatchObject({ status: "failed" });
  });

  it("accepts an itemized sum that includes the personal-contribution line", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 600, balance: 600 })],
      detailsByReportId: new Map([[10, details({ cash: 500, personal: 100, endingBalance: 600 })]]),
      contributionRows: [
        contributionRow({ contributionId: 1, amount: 500 }),
        contributionRow({ contributionId: 2, amount: 100, contributorName: "Pamela Evette" }),
      ],
    });
    expect(result).toMatchObject({ status: "aggregated", directCoverageNote: null });
  });

  it("fails closed on duplicate contribution ids", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 200, balance: 200 })],
      detailsByReportId: new Map([[10, details({ cash: 200, endingBalance: 200 })]]),
      contributionRows: [
        contributionRow({ contributionId: 7, amount: 100 }),
        contributionRow({ contributionId: 7, amount: 100 }),
      ],
    });
    expect(result).toMatchObject({ status: "failed" });
    if (result.status === "failed") {
      expect(result.diagnostics[0]).toMatch(/duplicate contributionId 7/);
    }
  });

  it("fails closed on a negative contribution amount", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 100, balance: 100 })],
      detailsByReportId: new Map([[10, details({ cash: 100, endingBalance: 100 })]]),
      contributionRows: [contributionRow({ contributionId: 1, amount: -25 })],
    });
    expect(result).toMatchObject({ status: "failed" });
  });

  it("fails closed on an unrecognized income type with a nonzero cycle total, tolerates zero", () => {
    const failing = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 150, balance: 150 })],
      detailsByReportId: new Map([
        [10, details({ cash: 100, extraIncome: [{ type: "Mystery Funds", electionCycleTotal: 50 }] })],
      ]),
      contributionRows: [],
    });
    expect(failing).toMatchObject({ status: "failed" });

    const tolerated = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 100, balance: 100 })],
      detailsByReportId: new Map([
        [10, details({ cash: 100, endingBalance: 100, extraIncome: [{ type: "Mystery Funds", electionCycleTotal: 0 }] })],
      ]),
      contributionRows: [contributionRow({ contributionId: 1, amount: 100 })],
    });
    expect(tolerated).toMatchObject({ status: "aggregated" });
  });

  it("excludes an unrelated same-year run when accepted election dates are provided", () => {
    const novemberRun = reportRow({
      reportId: 10,
      campaignId: 100,
      electionDate: "11/3/2026",
      contributions: 500,
      balance: 500,
      filingEndDate: "2026-06-30T00:00:00",
    });
    const specialRun = reportRow({
      reportId: 20,
      campaignId: 900,
      electionDate: "3/10/2026",
      contributions: 9000,
      balance: 9000,
      filingEndDate: "2026-03-31T00:00:00",
    });
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [novemberRun, specialRun],
      detailsByReportId: new Map([[10, details({ cash: 500, endingBalance: 500 })]]),
      contributionRows: [contributionRow({ contributionId: 1, officeRunId: 100, amount: 500 })],
      acceptedElectionDates: ["6/9/2026", "6/23/2026", "11/3/2026"],
    });
    expect(result).toMatchObject({ status: "aggregated", totalReceipts: 500, runCount: 1 });
    expect(
      southCarolinaContributionYearsForRuns([novemberRun, specialRun], 2026, ["6/9/2026", "6/23/2026", "11/3/2026"])
    ).toEqual([2026]);
  });

  it("marks coverage partial when positive personal contributions are not fully itemized", () => {
    const build = (itemized: number) =>
      aggregateSouthCarolinaDirectFinance({
        candidateFilerId: 54395,
        electionYear: 2026,
        reports: [reportRow({ reportId: 10, contributions: 600, balance: 600 })],
        detailsByReportId: new Map([[10, details({ cash: 500, personal: 100, endingBalance: 600 })]]),
        contributionRows: [contributionRow({ contributionId: 1, amount: itemized })],
      });
    // Personal money absent from rows: totals right, breakdowns short -> note.
    expect(build(500)).toMatchObject({ status: "aggregated", directCoverageNote: SOUTH_CAROLINA_DIRECT_COVERAGE_NOTE });
    // Personal money partially itemized: still aggregated, still noted.
    expect(build(550)).toMatchObject({ status: "aggregated", directCoverageNote: SOUTH_CAROLINA_DIRECT_COVERAGE_NOTE });
    // Above the full direct total: unexplained, fail closed.
    expect(build(650)).toMatchObject({ status: "failed" });
  });

  it("fails closed when the Campaign Funds ending balance disagrees with the report-index row", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 100, balance: 100 })],
      detailsByReportId: new Map([[10, details({ cash: 100, endingBalance: 55 })]]),
      contributionRows: [contributionRow({ contributionId: 1, amount: 100 })],
    });
    expect(result).toMatchObject({ status: "failed" });
    if (result.status === "failed") {
      expect(result.diagnostics[0]).toMatch(/Campaign Funds ending balance/);
    }
  });

  it("fails closed when detail totals disagree with the report-index row", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 999, balance: 0 })],
      detailsByReportId: new Map([[10, details({ cash: 100 })]]),
      contributionRows: [],
    });
    expect(result).toMatchObject({ status: "failed" });
    if (result.status === "failed") {
      expect(result.diagnostics[0]).toMatch(/disagree/);
    }
  });

  it("throws when a run's report details were not provided", () => {
    expect(() =>
      aggregateSouthCarolinaDirectFinance({
        candidateFilerId: 54395,
        electionYear: 2026,
        reports: [reportRow({ reportId: 10 })],
        detailsByReportId: new Map(),
        contributionRows: [],
      })
    ).toThrow(/Missing South Carolina report details/);
  });

  it("maps placeholder occupations to Unknown and keeps status occupations verbatim", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 300, balance: 300 })],
      detailsByReportId: new Map([[10, details({ cash: 300, endingBalance: 300 })]]),
      contributionRows: [
        contributionRow({ contributionId: 1, amount: 100, contributorName: "A Person", contributorOccupation: "Retired" }),
        contributionRow({ contributionId: 2, amount: 100, contributorName: "B Person", contributorOccupation: "N/A" }),
        contributionRow({ contributionId: 3, amount: 100, contributorName: "C Person", contributorOccupation: null }),
      ],
    });
    if (result.status !== "aggregated") throw new Error(`unexpected ${result.status}`);
    const occupations = result.directBreakdowns
      .filter((row) => row.categoryType === "occupation")
      .map((row) => `${row.categoryName}:${row.amount}`);
    expect(occupations.sort()).toEqual(["Retired:100", "Unknown:200"]);
  });
});
