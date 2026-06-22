import { describe, expect, it, vi } from "vitest";

import {
  listDueNewMexicoCandidateFinanceSyncRows,
  syncDueNewMexicoCandidateFinance,
  type NewMexicoContributionDataForYear,
  type NewMexicoExpenditureDataForYear,
} from "../../../src/pipeline/newMexicoFinance/newMexicoCandidateFinanceBatchSync.js";
import type {
  NewMexicoCfisContributionRow,
  NewMexicoCfisExpenditureRow,
} from "../../../src/pipeline/newMexicoFinance/newMexicoCfisArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function contribution(overrides: Partial<NewMexicoCfisContributionRow> = {}): NewMexicoCfisContributionRow {
  return {
    OrgID: "1001",
    "Transaction Amount": "100.00",
    "Transaction Date": "01/10/2026",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Prefix: "",
    Suffix: "",
    "Contributor Address Line 1": "",
    "Contributor Address Line 2": "",
    "Contributor City": "Santa Fe",
    "Contributor State": "NM",
    "Contributor Zip Code": "87501",
    Description: "",
    "Check Number": "",
    "Transaction ID": "T1",
    "Filed Date": "02/01/2026",
    Election: "2026 General",
    "Report Name": "First Report",
    "Start of Period": "01/01/2026",
    "End of Period": "01/31/2026",
    "Contributor Code": "Individual",
    "Contribution Type": "Contributions - Monetary",
    "Report Entity Type": "Candidate",
    "Committee Name": "Haaland for New Mexico",
    "Candidate Last Name": "Haaland",
    "Candidate First Name": "Deb",
    "Candidate Middle Name": "",
    "Candidate Prefix": "",
    "Candidate Suffix": "",
    Amended: "",
    "Contributor Employer": "Acme",
    "Contributor Occupation": "Attorney",
    "Occupation Comment": "",
    "Employment Information Requested": "",
    ...overrides,
  };
}

function expenditure(overrides: Partial<NewMexicoCfisExpenditureRow> = {}): NewMexicoCfisExpenditureRow {
  return {
    OrgID: "PAC1",
    "Expenditure Amount": "500.00",
    "Expenditure Date": "03/01/2026",
    "Payee Last Name": "Vendor",
    "Payee First Name": "",
    "Payee Middle Name": "",
    "Payee Prefix": "",
    "Payee Suffix": "",
    "Payee Address 1": "",
    "Payee Address 2": "",
    "Payee City": "Santa Fe",
    "Payee State": "NM",
    "Payee Zip Code": "87501",
    Description: "Mail",
    "Expenditure ID": "E1",
    "Filed Date": "03/02/2026",
    Election: "2026 General",
    "Report Name": "First Report",
    "Start of Period": "01/01/2026",
    "End of Period": "03/31/2026",
    Purpose: "Advertising",
    "Expenditure Type": "Independent Expenditure",
    Reason: "Deb Haaland",
    Stance: "Support",
    "Report Entity Type": "PAC - Independent Expenditure",
    "Committee Name": "NM PAC",
    "Candidate Last Name": "",
    "Candidate First Name": "",
    "Candidate Middle Name": "",
    "Candidate Prefix": "",
    "Candidate Suffix": "",
    Amended: "",
    ...overrides,
  };
}

function contributionDataForYear(input: {
  year: number;
  sourceUrl?: string;
  rowsByCommitteeId: Map<string, NewMexicoCfisContributionRow[]>;
}): NewMexicoContributionDataForYear {
  return {
    year: input.year,
    filePath: `/tmp/CON_${input.year}.csv`,
    sourceUrl:
      input.sourceUrl ??
      `https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=${input.year}&transactionType=CON&reportFormat=csv&fileName=CON_${input.year}.csv`,
    rowsByCommitteeId: input.rowsByCommitteeId,
  };
}

function expenditureDataForYear(input: {
  year: number;
  sourceUrl?: string;
  rows: NewMexicoCfisExpenditureRow[];
}): NewMexicoExpenditureDataForYear {
  return {
    year: input.year,
    filePath: `/tmp/EXP_${input.year}.csv`,
    sourceUrl:
      input.sourceUrl ??
      `https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=${input.year}&transactionType=EXP&reportFormat=csv&fileName=EXP_${input.year}.csv`,
    rows: input.rows,
  };
}

describe("newMexicoCandidateFinanceBatchSync", () => {
  it("lists due New Mexico finance sync rows from explicit active links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Deb Haaland",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "1001",
        committee_name: "Haaland for New Mexico",
        source_url: "https://login.cfis.sos.state.nm.us/",
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);

    const result = await listDueNewMexicoCandidateFinanceSyncRows(db, {
      now: new Date("2026-06-01T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 30,
      electionLookaheadDays: 730,
    });

    expect(result).toEqual({
      totalDueRows: 1,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Deb Haaland",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "1001",
          committeeName: "Haaland for New Mexico",
          sourceUrl: "https://login.cfis.sos.state.nm.us/",
          lastSyncedAt: null,
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.nm_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'NM'");
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

    await syncDueNewMexicoCandidateFinance({
      db,
      syncNewMexicoCandidateFinanceFn: vi.fn(),
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

  it("syncs selected due links with cached yearly contribution and expenditure rows", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Deb Haaland",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "1001",
        committee_name: "Haaland for New Mexico",
        source_url: "https://login.cfis.sos.state.nm.us/",
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);
    const contributionRow = contribution({ OrgID: "1001", "Transaction Amount": "100.00" });
    const expenditureRow = expenditure();
    const syncNewMexicoCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      resolution: { status: "matched", committeeId: "1001" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: 500,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      skippedExpenditureRowCount: 0,
    });

    const result = await syncDueNewMexicoCandidateFinance({
      db,
      syncNewMexicoCandidateFinanceFn,
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
            rowsByCommitteeId: new Map([["1001", [contributionRow]]]),
          }),
        ],
      ]),
      expenditureDataByYear: new Map([[2026, expenditureDataForYear({ year: 2026, rows: [expenditureRow] })]]),
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
    });
    expect(result.results[0]).toMatchObject({
      ok: true,
      committeeId: "1001",
      result: {
        totalReceipts: 100,
        directContributionTotal: 100,
        outsideSupportTotal: 500,
      },
    });
    expect(syncNewMexicoCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Deb Haaland",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
        sourceUrl: "https://login.cfis.sos.state.nm.us/",
        contributionRows: [contributionRow],
        expenditureRows: [expenditureRow],
        trustedCommittee: {
          committeeId: "1001",
          committeeName: "Haaland for New Mexico",
          sourceUrl: "https://login.cfis.sos.state.nm.us/",
        },
      })
    );
  });

  it("records contribution artifact load failures per year", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Deb Haaland",
        election_year: 2025,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "1001",
        committee_name: "Haaland for New Mexico",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);

    const result = await syncDueNewMexicoCandidateFinance({
      db,
      syncNewMexicoCandidateFinanceFn: vi.fn(),
      now: new Date("2026-06-01T00:00:00.000Z"),
      rawDataCacheDir: "/tmp/voteapp-missing-new-mexico-cfis-cache",
      autoLinkMissingLinks: false,
    });

    expect(result).toMatchObject({
      selectedCandidateCount: 1,
      syncedCandidateCount: 0,
      failedCandidateCount: 1,
    });
    expect(result.results[0]).toMatchObject({
      ok: false,
      candidateId: CANDIDATE_ID,
      electionYear: 2025,
      committeeId: "1001",
    });
    expect(result.results[0]?.error).toContain("New Mexico CFIS contribution artifact not found for 2025");
  });

  it("auto-links missing candidates before listing due rows", async () => {
    const contributionRow = contribution({ OrgID: "1001", "Transaction Amount": "100.00" });
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              candidate_name: "Deb Haaland",
              election_year: 2026,
              office_scope: "statewide",
              office_name: "Governor",
              district: null,
            },
          ],
          rowCount: 1,
        })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              candidate_name: "Deb Haaland",
              election_year: 2026,
              office_scope: "statewide",
              office_name: "Governor",
              district: null,
              committee_id: "1001",
              committee_name: "Haaland for New Mexico",
              source_url: "https://login.cfis.sos.state.nm.us/",
              last_synced_at: null,
              total_due_rows: "1",
            },
          ],
          rowCount: 1,
        }),
    };
    const syncNewMexicoCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      resolution: { status: "matched", committeeId: "1001" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });

    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const result = await syncDueNewMexicoCandidateFinance({
        db,
        syncNewMexicoCandidateFinanceFn,
        now: new Date("2026-06-01T00:00:00.000Z"),
        contributionDataByYear: new Map([
          [
            2026,
            contributionDataForYear({
              year: 2026,
              sourceUrl: "https://login.cfis.sos.state.nm.us/",
              rowsByCommitteeId: new Map([["1001", [contributionRow]]]),
            }),
          ],
        ]),
      });

      expect(result).toMatchObject({
        dueCandidateCount: 1,
        selectedCandidateCount: 1,
        syncedCandidateCount: 1,
        failedCandidateCount: 0,
      });
    } finally {
      warnSpy.mockRestore();
    }

    expect(db.query).toHaveBeenCalledTimes(3);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.candidate_elections AS candidate_election");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.nm_candidate_finance_links");
    expect(String(db.query.mock.calls[2]?.[0])).toContain("FROM public.nm_candidate_finance_links AS link");
    expect(syncNewMexicoCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        contributionRows: [contributionRow],
        trustedCommittee: {
          committeeId: "1001",
          committeeName: "Haaland for New Mexico",
          sourceUrl: "https://login.cfis.sos.state.nm.us/",
        },
      })
    );
  });
});
