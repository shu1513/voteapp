import { describe, expect, it, vi } from "vitest";

import {
  listDueOklahomaCandidateFinanceSyncRows,
  syncDueOklahomaCandidateFinance,
  type OklahomaContributionDataForYear,
} from "../../../src/pipeline/oklahomaFinance/oklahomaCandidateFinanceBatchSync.js";
import type { OklahomaGuardianContributionRow } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianContributionReader.js";
import type { OklahomaGuardianIeOutsideSpendingDiscoveryResult } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianIeOutsideSpendingDiscovery.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function contribution(overrides: Partial<OklahomaGuardianContributionRow> = {}): OklahomaGuardianContributionRow {
  return {
    "Receipt ID": "R1",
    "Org ID": "11954",
    "Receipt Type": "Contribution",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "100.00",
    Description: "",
    "Receipt Source Type": "Individual",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Oklahoma City",
    State: "OK",
    Zip: "73102",
    "Filed Date": "02/01/2026",
    "Committee Type": "Candidate Committee",
    "Committee Name": "Dishman for Senate",
    "Candidate Name": "C. Brent Dishman",
    Amended: "",
    Employer: "Acme Inc",
    Occupation: "Attorney",
    ...overrides,
  };
}

function contributionDataForYear(input: {
  year: number;
  sourceUrl?: string;
  rowsByCommitteeId: Map<string, OklahomaGuardianContributionRow[]>;
  rowsByCommitteeName?: Map<string, OklahomaGuardianContributionRow[]>;
}): OklahomaContributionDataForYear {
  return {
    year: input.year,
    zipPath: `/tmp/${input.year}_ContributionLoanExtract.csv.zip`,
    sourceUrl:
      input.sourceUrl ??
      `https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/${input.year}_ContributionLoanExtract.csv.zip`,
    rowsByCommitteeId: input.rowsByCommitteeId,
    rowsByCommitteeName: input.rowsByCommitteeName,
  };
}

describe("oklahomaCandidateFinanceBatchSync", () => {
  it("lists due Oklahoma finance sync rows from explicit active links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Brent Dishman",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "47",
        committee_id: "11954",
        committee_name: "Dishman for Senate",
        source_url: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "9001",
        committee_name: "Doe for Governor",
        source_url: null,
        last_synced_at: "2026-01-01 00:00:00+00",
        total_due_rows: "2",
      },
    ]);

    const result = await listDueOklahomaCandidateFinanceSyncRows(db, {
      now: new Date("2026-06-01T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });

    expect(result).toEqual({
      totalDueRows: 2,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Brent Dishman",
          electionYear: 2026,
          officeScope: "state_upper",
          officeName: "State Senator",
          district: "47",
          committeeId: "11954",
          committeeName: "Dishman for Senate",
          sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "9001",
          committeeName: "Doe for Governor",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.ok_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'OK'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("election.election_date >= ($1::date - make_interval(days => $4::int))");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      30,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("uses a one-day post-election grace window by default for due selection", async () => {
    const db = createMockDb();

    await syncDueOklahomaCandidateFinance({
      db,
      syncOklahomaCandidateFinanceFn: vi.fn(),
      now: new Date("2026-06-01T00:00:00.000Z"),
      autoLinkMissingLinks: false,
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "election.election_date >= ($1::date - make_interval(days => $4::int))"
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("syncs selected due links with cached yearly contribution rows and continues after failures", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Brent Dishman",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "47",
        committee_id: "11954",
        committee_name: "Dishman for Senate",
        source_url: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "9001",
        committee_name: "Doe for Governor",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "2",
      },
    ]);
    const syncOklahomaCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: false,
        resolution: { status: "matched", committeeId: "11954" },
        linkWritten: true,
        summaryWritten: true,
        directBreakdownsWritten: 3,
        outsideIncluded: true,
        outsideGroupsWritten: 1,
        totalReceipts: 100,
        directContributionTotal: 90,
        outsideSupportTotal: 50,
        outsideOpposeTotal: 0,
        outsideReportsExamined: 1,
        outsideUsableReports: 1,
        outsideSkippedReports: 0,
        matchedContributionRowCount: 1,
        includedContributionRowCount: 1,
        skippedContributionRowCount: 0,
      })
      .mockRejectedValueOnce(new Error("Guardian row parse failed"));
    const row = contribution({ "Org ID": "11954", "Receipt Amount": "100.00" });

    const result = await syncDueOklahomaCandidateFinance({
      db,
      syncOklahomaCandidateFinanceFn,
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      autoLinkMissingLinks: false,
      contributionDataByYear: new Map([
        [
          2026,
          contributionDataForYear({
            year: 2026,
            rowsByCommitteeId: new Map([["11954", [row]]]),
          }),
        ],
      ]),
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
    });
    expect(result.results[0]).toMatchObject({
      ok: true,
      committeeId: "11954",
      result: {
        totalReceipts: 100,
        directContributionTotal: 90,
      },
    });
    expect(result.results[1]).toMatchObject({
      ok: false,
      committeeId: "9001",
      error: "Guardian row parse failed",
    });
    expect(syncOklahomaCandidateFinanceFn).toHaveBeenCalledTimes(2);
    expect(syncOklahomaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Brent Dishman",
        electionYear: 2026,
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        contributionRows: [row],
        contributionSourceUrl:
          "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
        includeOutsideSpending: true,
        outsideMaxReports: 10,
        discoverOutsideSpendingReportsFn: expect.any(Function),
      })
    );
    expect(syncOklahomaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: "33333333-3333-4333-8333-333333333333",
        contributionRows: [],
        includeOutsideSpending: true,
        outsideMaxReports: 10,
        discoverOutsideSpendingReportsFn: expect.any(Function),
      })
    );
  });

  it("can disable outside-spending discovery for due syncs", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Brent Dishman",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "47",
        committee_id: "11954",
        committee_name: "Dishman for Senate",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);
    const syncOklahomaCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      resolution: { status: "matched", committeeId: "11954" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideIncluded: false,
      outsideGroupsWritten: 0,
      totalReceipts: 0,
      directContributionTotal: 0,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      outsideReportsExamined: 0,
      outsideUsableReports: 0,
      outsideSkippedReports: 0,
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    });

    await syncDueOklahomaCandidateFinance({
      db,
      syncOklahomaCandidateFinanceFn,
      now: new Date("2026-06-01T00:00:00.000Z"),
      autoLinkMissingLinks: false,
      includeOutsideSpending: false,
      contributionDataByYear: new Map([
        [
          2026,
          contributionDataForYear({
            year: 2026,
            rowsByCommitteeId: new Map(),
          }),
        ],
      ]),
    });

    expect(syncOklahomaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        includeOutsideSpending: false,
        outsideMaxReports: 10,
      })
    );
  });

  it("preloads outside spender contribution rows for the real due sync path", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Kevin Stitt",
        election_year: 2022,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "11954",
        committee_name: "Stitt for Governor",
        source_url: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);
    const candidateRow = contribution({
      "Org ID": "11954",
      "Receipt Date": "01/10/2022",
      "Committee Type": "Candidate Committee",
      "Committee Name": "Stitt for Governor",
      "Candidate Name": "Kevin Stitt",
      "Receipt Amount": "100.00",
    });
    const outsideDonorRow = contribution({
      "Receipt ID": "O1",
      "Org ID": "90001",
      "Receipt Date": "02/10/2022",
      "Receipt Amount": "50000.00",
      "Receipt Source Type": "Business",
      "Last Name": "Energy Transfer",
      "First Name": "",
      "Committee Type": "Independent Expenditure Committee",
      "Committee Name": "THE OKLAHOMA PROJECT",
      "Candidate Name": "",
      Employer: "",
      Occupation: "",
    });
    const discovery: OklahomaGuardianIeOutsideSpendingDiscoveryResult = {
      search: {
        candidateName: "Kevin Stitt",
        dateFrom: "01/01/2021",
        dateThrough: "12/31/2022",
        expenditureType: "independent_expenditure",
        rows: [],
        sourceUrl: "https://guardian.ok.gov/PublicSite/SearchPages/IEReports.aspx",
      },
      reportsExamined: 1,
      usableReports: [
        {
          rowIndex: 0,
          sourceRow: {
            filerName: "THE OKLAHOMA PROJECT",
            reportDescription: "Independent expenditure",
            periodBegin: "01/01/2022",
            periodEnd: "12/31/2022",
            filedDate: "03/01/2022",
            viewReportPostbackTarget: "ctl00$MainContent$GridView1$ctl02$lnkView",
          },
          spenderName: "THE OKLAHOMA PROJECT",
          candidateName: "Kevin Stitt",
          officeName: "Governor",
          supportOppose: "support",
          amount: 1234.56,
          reportingPeriodBegin: "01/01/2022",
          reportingPeriodEnd: "12/31/2022",
          reportDescription: "Independent expenditure",
          amended: false,
          sourceUrl: "https://guardian.ok.gov/PublicSite/report.pdf",
          pdfByteLength: 12345,
        },
      ],
      skippedReports: [],
    };
    const discoverOutsideSpendingReportsFn = vi.fn().mockResolvedValue(discovery);

    const result = await syncDueOklahomaCandidateFinance({
      db,
      now: new Date("2022-06-01T00:00:00.000Z"),
      dryRun: true,
      autoLinkMissingLinks: false,
      contributionDataByYear: new Map([
        [
          2022,
          contributionDataForYear({
            year: 2022,
            rowsByCommitteeId: new Map([
              ["11954", [candidateRow]],
              ["90001", [outsideDonorRow]],
            ]),
          }),
        ],
      ]),
      discoverOutsideSpendingReportsFn,
    });

    expect(discoverOutsideSpendingReportsFn).toHaveBeenCalledTimes(1);
    expect(result.results[0]?.result).toMatchObject({
      dryRun: true,
      outsideIncluded: true,
      outsideSupportTotal: 1234.56,
      outsideOpposeTotal: 0,
      outsideReportsExamined: 1,
      outsideUsableReports: 1,
      outsideSkippedReports: 0,
      outsideMatchedContributionRowCount: 1,
      outsideIncludedContributionRowCount: 1,
      outsideSkippedContributionRowCount: 0,
      outsideGroupBreakdownsWritten: 0,
    });
  });

  it("rejects invalid batch options before querying", async () => {
    const db = createMockDb();

    await expect(
      syncDueOklahomaCandidateFinance({
        db,
        maxCandidates: 0,
      })
    ).rejects.toThrow("Invalid Oklahoma finance batch sync maxCandidates");
    expect(db.query).not.toHaveBeenCalled();
  });
});
