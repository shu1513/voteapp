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
    totals: [{ totalType: "Campaign Funds", startingBalance: 0, endingBalance: 0 }],
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
        [10, details({ cash: 1000, inKind: 100, personal: 0, loans: 400, credits: 0, expTotal: 900 })],
        [20, details({ cash: 1200, inKind: 0, personal: 100, loans: 0, credits: 10, expTotal: 300 })],
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
      detailsByReportId: new Map([[10, details({ cash: 500 })]]),
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
      detailsByReportId: new Map([[10, details({ cash: 100 })]]),
      contributionRows: [contributionRow({ contributionId: 1, amount: 150 })],
    });
    expect(result).toMatchObject({ status: "failed" });
  });

  it("accepts an itemized sum that includes the personal-contribution line", () => {
    const result = aggregateSouthCarolinaDirectFinance({
      candidateFilerId: 54395,
      electionYear: 2026,
      reports: [reportRow({ reportId: 10, contributions: 600, balance: 600 })],
      detailsByReportId: new Map([[10, details({ cash: 500, personal: 100 })]]),
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
      detailsByReportId: new Map([[10, details({ cash: 200 })]]),
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
      detailsByReportId: new Map([[10, details({ cash: 100 })]]),
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
        [10, details({ cash: 100, extraIncome: [{ type: "Mystery Funds", electionCycleTotal: 0 }] })],
      ]),
      contributionRows: [contributionRow({ contributionId: 1, amount: 100 })],
    });
    expect(tolerated).toMatchObject({ status: "aggregated" });
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
      detailsByReportId: new Map([[10, details({ cash: 300 })]]),
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
