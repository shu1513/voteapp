import { describe, expect, it, vi } from "vitest";

import {
  southCarolinaAcceptedElectionDates,
  syncSouthCarolinaCandidateFinance,
} from "../../../src/pipeline/southCarolinaFinance/southCarolinaCandidateFinanceSync.js";
import type {
  SouthCarolinaCandidateReportRow,
  SouthCarolinaContributionSearchRow,
  SouthCarolinaReportDetails,
} from "../../../src/pipeline/southCarolinaFinance/southCarolinaEthicsClient.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function reportRow(overrides: Partial<SouthCarolinaCandidateReportRow>): SouthCarolinaCandidateReportRow {
  return {
    reportId: 430061,
    reportName: "Pre-Election Report 2026",
    reportType: "Pre-Election Quarterly",
    electionDate: "6/9/2026",
    contributions: 100,
    expenses: 40,
    balance: 60,
    dateSubmitted: "2026-07-14T10:00:00",
    campaignId: 77609,
    candidateFilerId: 54395,
    filingStartDate: "2026-01-01T04:00:00",
    filingEndDate: "2026-05-20T00:00:00",
    isPrimary: true,
    isGeneral: false,
    isPreElection: true,
    isFinal: false,
    ...overrides,
  };
}

function details(input: {
  cash?: number;
  personal?: number;
  loans?: number;
  expTotal: number;
  endingBalance: number;
}): SouthCarolinaReportDetails {
  const cash = input.cash ?? 0;
  const personal = input.personal ?? 0;
  const loans = input.loans ?? 0;
  const incomeTotal = cash + personal + loans;
  return {
    filerName: "Evette, Pamela S",
    electionDate: "6/9/2026",
    electionType: "Primary",
    reportType: "Pre-Election Quarterly",
    filingPeriod: "1/1/2026 - 5/20/2026",
    isAmendment: false,
    reportSequenceNumber: 1,
    contributionsTotal: incomeTotal,
    expendituresTotal: input.expTotal,
    income: [
      { type: "Cash Contributions", filingPeriod: cash, electionCycleTotal: cash },
      { type: "Personal Contributions", filingPeriod: personal, electionCycleTotal: personal },
      { type: "Loans", filingPeriod: loans, electionCycleTotal: loans },
      { type: "Total", filingPeriod: incomeTotal, electionCycleTotal: incomeTotal },
    ],
    expenditures: [{ type: "Total", filingPeriod: input.expTotal, electionCycleTotal: input.expTotal }],
    totals: [{ totalType: "Campaign Funds", startingBalance: 0, endingBalance: input.endingBalance }],
    versions: [{ id: 430061, name: "Original" }],
  };
}

function contributionRow(
  overrides: Partial<SouthCarolinaContributionSearchRow>
): SouthCarolinaContributionSearchRow {
  return {
    contributionId: 1,
    candidateId: 54395,
    officeRunId: 77609,
    candidateName: "Evette, Pamela S",
    officeName: "4",
    electionDate: "6/9/2026",
    date: "2026-03-01T00:00:00",
    amount: 100,
    contributorName: "Jane Donor",
    contributorOccupation: "Attorney",
    group: "No",
    description: null,
    ...overrides,
  };
}

function writingDb() {
  const client = {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.sc_candidate_finance_links")) {
        return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
    release: vi.fn(),
  };
  const db = { query: vi.fn(), connect: vi.fn().mockResolvedValue(client) };
  return { db, client };
}

function baseSyncInput(db: { query: unknown; connect: unknown }) {
  return {
    db: db as never,
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Pamela Evette",
    electionYear: 2026,
    electionDate: "2026-11-03",
    officeScope: "statewide",
    officeName: "Governor",
    district: null,
    filer: {
      candidateFilerId: 54395,
      filerName: "Evette, Pamela S",
      linkSource: "ethics_filer_search" as const,
      sourceUrl: null,
    },
    now: new Date("2026-08-27T00:00:00.000Z"),
  };
}

describe("southCarolinaAcceptedElectionDates", () => {
  it("derives the full statutory trio for the 2026 general, in both date forms", () => {
    expect(southCarolinaAcceptedElectionDates(2026, "2026-11-03")).toEqual([
      "6/9/2026",
      "06/09/2026",
      "6/23/2026",
      "06/23/2026",
      "11/3/2026",
      "11/03/2026",
    ]);
  });

  it("gives a non-statutory (special) election only its own date", () => {
    expect(southCarolinaAcceptedElectionDates(2026, "2026-03-17")).toEqual(["3/17/2026", "03/17/2026"]);
  });

  it("rejects a date outside the election year", () => {
    expect(() => southCarolinaAcceptedElectionDates(2026, "2025-11-04")).toThrow(
      "does not match election year"
    );
  });
});

describe("syncSouthCarolinaCandidateFinance", () => {
  it("fetches, aggregates, and writes a filed snapshot", async () => {
    const { db, client } = writingDb();
    const reports = [
      reportRow({}),
      // Prior-cycle run: excluded by the year filter.
      reportRow({ reportId: 300000, campaignId: 50000, electionDate: "11/8/2022" }),
    ];
    const fetchContributions = vi.fn().mockResolvedValue([contributionRow({})]);

    const result = await syncSouthCarolinaCandidateFinance({
      ...baseSyncInput(db),
      fetchCandidateReports: vi.fn().mockResolvedValue(reports),
      fetchReportDetails: vi.fn().mockResolvedValue(details({ cash: 100, expTotal: 40, endingBalance: 60 })),
      fetchContributions,
    });

    expect(result).toMatchObject({
      status: "synced",
      dryRun: false,
      runCount: 1,
      contributionYears: [2026],
      totalReceipts: 100,
      directContributionTotal: 100,
      totalDisbursements: 40,
      cashOnHand: 60,
      directCoverageNote: null,
      includedContributionRowCount: 1,
      summaryWritten: true,
      directBreakdownsWritten: 2,
    });
    // Contribution search text is the filer's surname (contains-match recall).
    expect(fetchContributions).toHaveBeenCalledWith(
      { candidate: "Evette", contributionYear: 2026 },
      undefined
    );
    const linkInsert = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.sc_candidate_finance_links")
    );
    expect(linkInsert?.[1]?.[3]).toBe("PAMELA EVETTE");
    expect(linkInsert?.[1]?.[6]).toBe("54395");
    const summaryInsert = client.query.mock.calls.find((call) =>
      String(call[0]).includes("sc_candidate_finance_summaries")
    );
    expect(summaryInsert?.[1]?.slice(2, 9)).toEqual([
      100,
      100,
      40,
      60,
      null,
      null,
      "https://ethicsfiling.sc.gov/public",
    ]);
  });

  it("skips the write entirely when the filer has no cycle filings (never a zero)", async () => {
    const { db, client } = writingDb();
    const result = await syncSouthCarolinaCandidateFinance({
      ...baseSyncInput(db),
      fetchCandidateReports: vi
        .fn()
        .mockResolvedValue([reportRow({ campaignId: 50000, electionDate: "11/8/2022" })]),
      fetchReportDetails: vi.fn(),
      fetchContributions: vi.fn(),
    });

    expect(result).toMatchObject({ status: "no_filed_reports", summaryWritten: false });
    expect(db.connect).not.toHaveBeenCalled();
    expect(client.query).not.toHaveBeenCalled();
  });

  it("throws on aggregation failure without writing (fail-closed)", async () => {
    const { db, client } = writingDb();
    await expect(
      syncSouthCarolinaCandidateFinance({
        ...baseSyncInput(db),
        fetchCandidateReports: vi.fn().mockResolvedValue([reportRow({ contributions: 999 })]),
        fetchReportDetails: vi.fn().mockResolvedValue(details({ cash: 100, expTotal: 40, endingBalance: 60 })),
        fetchContributions: vi.fn().mockResolvedValue([]),
      })
    ).rejects.toThrow(/aggregation failed for filer 54395/);
    expect(client.query).not.toHaveBeenCalled();
  });

  it("does not write in dry-run mode but reports totals", async () => {
    const { db, client } = writingDb();
    const result = await syncSouthCarolinaCandidateFinance({
      ...baseSyncInput(db),
      dryRun: true,
      fetchCandidateReports: vi.fn().mockResolvedValue([reportRow({})]),
      fetchReportDetails: vi.fn().mockResolvedValue(details({ cash: 100, expTotal: 40, endingBalance: 60 })),
      fetchContributions: vi.fn().mockResolvedValue([contributionRow({})]),
    });

    expect(result).toMatchObject({ dryRun: true, status: "synced", totalReceipts: 100, summaryWritten: false });
    expect(db.connect).not.toHaveBeenCalled();
    expect(client.query).not.toHaveBeenCalled();
  });

  it("rejects ineligible offices and invalid filer ids before any fetch", async () => {
    const { db } = writingDb();
    const fetchCandidateReports = vi.fn();
    await expect(
      syncSouthCarolinaCandidateFinance({
        ...baseSyncInput(db),
        officeScope: "county",
        officeName: "Sheriff",
        fetchCandidateReports,
      })
    ).rejects.toThrow("not South Carolina-finance eligible");
    await expect(
      syncSouthCarolinaCandidateFinance({
        ...baseSyncInput(db),
        filer: { candidateFilerId: 0, filerName: "X, Y", linkSource: "manual" },
        fetchCandidateReports,
      })
    ).rejects.toThrow("invalid candidate filer id: 0");
    expect(fetchCandidateReports).not.toHaveBeenCalled();
  });
});
