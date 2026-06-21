import { describe, expect, it, vi } from "vitest";

import {
  listDueNebraskaCandidateFinanceSyncRows,
  syncDueNebraskaCandidateFinance,
  type NebraskaContributionDataForYear,
} from "../../../src/pipeline/nebraskaFinance/nebraskaCandidateFinanceBatchSync.js";
import type { NebraskaNadcContributionRow } from "../../../src/pipeline/nebraskaFinance/nebraskaNadcArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function contribution(overrides: Partial<NebraskaNadcContributionRow> = {}): NebraskaNadcContributionRow {
  return {
    "Receipt ID": "R1",
    "Org ID": "7569",
    "Filer Type": "Candidate Committee",
    "Filer Name": "VOTE VEST",
    "Candidate Name": "Rick Vest",
    "Receipt Transaction/Contribution Type": "Monetary Contribution",
    "Other Funds Type": "",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "100.00",
    Description: "",
    "Contributor or Transaction Source Type": "Individual",
    "Contributor or Source Name (Individual Last Name)": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Lincoln",
    State: "NE",
    Zip: "68508",
    "Filed Date": "02/01/2026",
    Amended: "False",
    Employer: "Acme Inc",
    Occupation: "Attorney",
    ...overrides,
  };
}

function contributionDataForYear(input: {
  year: number;
  sourceUrl?: string;
  rowsByCommitteeId: Map<string, NebraskaNadcContributionRow[]>;
}): NebraskaContributionDataForYear {
  return {
    year: input.year,
    zipPath: `/tmp/${input.year}_ContributionLoanExtract.csv.zip`,
    sourceUrl:
      input.sourceUrl ??
      `https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/${input.year}_ContributionLoanExtract.csv.zip`,
    rowsByCommitteeId: input.rowsByCommitteeId,
  };
}

describe("nebraskaCandidateFinanceBatchSync", () => {
  it("lists due Nebraska finance sync rows from explicit active links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Rick Vest",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "30",
        committee_id: "7569",
        committee_name: "VOTE VEST",
        source_url: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
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
        committee_name: "DOE FOR GOVERNOR",
        source_url: null,
        last_synced_at: "2026-01-01 00:00:00+00",
        total_due_rows: "2",
      },
    ]);

    const result = await listDueNebraskaCandidateFinanceSyncRows(db, {
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
          candidateName: "Rick Vest",
          electionYear: 2026,
          officeScope: "state_upper",
          officeName: "State Senator",
          district: "30",
          committeeId: "7569",
          committeeName: "VOTE VEST",
          sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
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
          committeeName: "DOE FOR GOVERNOR",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.ne_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'NE'");
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

    await syncDueNebraskaCandidateFinance({
      db,
      syncNebraskaCandidateFinanceFn: vi.fn(),
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
        candidate_name: "Rick Vest",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "30",
        committee_id: "7569",
        committee_name: "VOTE VEST",
        source_url: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
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
        committee_name: "DOE FOR GOVERNOR",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "2",
      },
    ]);
    const syncNebraskaCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: false,
        resolution: { status: "matched", committeeId: "7569" },
        linkWritten: true,
        summaryWritten: true,
        directBreakdownsWritten: 3,
        totalReceipts: 100,
        directContributionTotal: 90,
        matchedContributionRowCount: 1,
        includedContributionRowCount: 1,
        skippedContributionRowCount: 0,
      })
      .mockRejectedValueOnce(new Error("NADC row parse failed"));
    const row = contribution({ "Org ID": "7569", "Receipt Amount": "100.00" });

    const result = await syncDueNebraskaCandidateFinance({
      db,
      syncNebraskaCandidateFinanceFn,
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
            rowsByCommitteeId: new Map([["7569", [row]]]),
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
      committeeId: "7569",
      result: {
        totalReceipts: 100,
        directContributionTotal: 90,
      },
    });
    expect(result.results[1]).toMatchObject({
      ok: false,
      committeeId: "9001",
      error: "NADC row parse failed",
    });
    expect(syncNebraskaCandidateFinanceFn).toHaveBeenCalledTimes(2);
    expect(syncNebraskaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Rick Vest",
        electionYear: 2026,
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "30",
        sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
        contributionRows: [row],
        contributionSourceUrl:
          "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
      })
    );
    expect(syncNebraskaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: "33333333-3333-4333-8333-333333333333",
        contributionRows: [],
      })
    );
  });

  it("records artifact load failures per year without blocking other due years", async () => {
    const successfulCandidateId = "55555555-5555-4555-8555-555555555555";
    const successfulElectionId = "66666666-6666-4666-8666-666666666666";
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Rick Vest",
        election_year: 2025,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "30",
        committee_id: "7569",
        committee_name: "VOTE VEST",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: successfulCandidateId,
        election_id: successfulElectionId,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "9001",
        committee_name: "DOE FOR GOVERNOR",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "2",
      },
    ]);
    const row = contribution({ "Org ID": "9001", "Receipt Amount": "100.00" });
    const syncNebraskaCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: successfulCandidateId,
      electionId: successfulElectionId,
      electionYear: 2026,
      dryRun: false,
      resolution: { status: "matched", committeeId: "9001" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      totalReceipts: 100,
      directContributionTotal: 90,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
    });

    const result = await syncDueNebraskaCandidateFinance({
      db,
      syncNebraskaCandidateFinanceFn,
      now: new Date("2026-06-01T00:00:00.000Z"),
      rawDataCacheDir: "/tmp/voteapp-missing-nebraska-nadc-cache",
      autoLinkMissingLinks: false,
      contributionDataByYear: new Map([
        [
          2026,
          contributionDataForYear({
            year: 2026,
            rowsByCommitteeId: new Map([["9001", [row]]]),
          }),
        ],
      ]),
    });

    expect(result).toMatchObject({
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
    });
    expect(result.results[0]).toMatchObject({
      ok: false,
      candidateId: CANDIDATE_ID,
      electionYear: 2025,
      committeeId: "7569",
    });
    expect(result.results[0]?.error).toContain("Nebraska NADC contribution ZIP not found for 2025");
    expect(result.results[1]).toMatchObject({
      ok: true,
      candidateId: successfulCandidateId,
      electionYear: 2026,
      committeeId: "9001",
    });
    expect(syncNebraskaCandidateFinanceFn).toHaveBeenCalledTimes(1);
  });

  it("auto-links missing candidates before listing due rows", async () => {
    const row = contribution({ "Org ID": "7569", "Receipt Amount": "100.00" });
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              candidate_name: "Rick Vest",
              election_year: 2026,
              office_scope: "state_upper",
              office_name: "State Senator",
              district: "30",
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
              candidate_name: "Rick Vest",
              election_year: 2026,
              office_scope: "state_upper",
              office_name: "State Senator",
              district: "30",
              committee_id: "7569",
              committee_name: "VOTE VEST",
              source_url: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
              last_synced_at: null,
              total_due_rows: "1",
            },
          ],
          rowCount: 1,
        }),
    };
    const syncNebraskaCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      resolution: { status: "matched", committeeId: "7569" },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 3,
      totalReceipts: 100,
      directContributionTotal: 90,
      matchedContributionRowCount: 1,
      includedContributionRowCount: 1,
      skippedContributionRowCount: 0,
    });

    const result = await syncDueNebraskaCandidateFinance({
      db,
      syncNebraskaCandidateFinanceFn,
      now: new Date("2026-06-01T00:00:00.000Z"),
      contributionDataByYear: new Map([
        [
          2026,
          contributionDataForYear({
            year: 2026,
            sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
            rowsByCommitteeId: new Map([["7569", [row]]]),
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
    expect(result.results[0]).toMatchObject({
      result: {
        totalReceipts: 100,
        directContributionTotal: 90,
      },
    });
    expect(db.query).toHaveBeenCalledTimes(3);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.candidate_elections AS candidate_election");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.ne_candidate_finance_links");
    expect(String(db.query.mock.calls[2]?.[0])).toContain("FROM public.ne_candidate_finance_links AS link");
    expect(syncNebraskaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        contributionRows: [row],
      })
    );
  });
});
