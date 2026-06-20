import { describe, expect, it, vi } from "vitest";

import {
  listDueConnecticutCandidateFinanceSyncRows,
  syncDueConnecticutCandidateFinance,
  type ConnecticutEcrisReceiptDataForYear,
} from "../../../src/pipeline/connecticutFinance/connecticutCandidateFinanceBatchSync.js";
import type { ConnecticutEcrisArtifactRow } from "../../../src/pipeline/connecticutFinance/connecticutEcrisArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

function receipt(overrides: Partial<ConnecticutEcrisArtifactRow> = {}): ConnecticutEcrisArtifactRow {
  return {
    Committee: "ACKERT FOR THE 8TH",
    "Contributor Name": "Carolyn Gerrity",
    District: "8",
    "Office Sought": "State Representative",
    Employer: "RTX-Pratt Whitney",
    "Receipt Type": "Itemized Contributions from Individuals",
    "Committee Type": "Candidate Committee",
    "Transaction Date": "03/31/2026",
    "File To State": "04/01/2026",
    Amount: "50.00",
    "Receipt State": "Original",
    Occupation: "Business Manager",
    ElectionYear: "2026",
    "Committee ID": "14376",
    "Candidate First Name": "Timothy",
    "Candidate Middle Intial": "J",
    "Candidate Last Name": "Ackert",
    ...overrides,
  };
}

function receiptDataForYear(input: {
  year: number;
  sourceUrl?: string;
  rowsByCommitteeId: Map<string, ConnecticutEcrisArtifactRow[]>;
}): ConnecticutEcrisReceiptDataForYear {
  return {
    year: input.year,
    filePath: `/tmp/${input.year}_candidate_receipts.csv`,
    sourceUrl:
      input.sourceUrl ??
      `https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles/Receipts${input.year}ElectionYearCandidateExploratoryCommittees.csv`,
    rowsByCommitteeId: input.rowsByCommitteeId,
  };
}

describe("connecticutCandidateFinanceBatchSync", () => {
  it("lists due Connecticut finance sync rows from explicit active links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Timothy Ackert",
        election_year: 2026,
        office_name: "State Lower Chamber Legislator",
        district: "8",
        committee_id: "14376",
        committee_name: "ACKERT FOR THE 8TH",
        source_url: "https://seec.ct.gov/portal/ecris/CurPreYears",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_name: "Governor",
        district: null,
        committee_id: "20001",
        committee_name: "DOE FOR GOVERNOR",
        source_url: null,
        last_synced_at: "2026-01-01 00:00:00+00",
        total_due_rows: "2",
      },
    ]);

    const result = await listDueConnecticutCandidateFinanceSyncRows(db, {
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
          candidateName: "Timothy Ackert",
          electionYear: 2026,
          officeName: "State Lower Chamber Legislator",
          district: "8",
          committeeId: "14376",
          committeeName: "ACKERT FOR THE 8TH",
          sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeName: "Governor",
          district: null,
          committeeId: "20001",
          committeeName: "DOE FOR GOVERNOR",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.ct_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'CT'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("election.election_date >= ($1::date - make_interval(days => $4::int))");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      30,
      730,
    ]);
  });

  it("uses a one-day post-election grace window by default for due selection", async () => {
    const db = createMockDb();

    await syncDueConnecticutCandidateFinance({
      db,
      syncConnecticutCandidateFinanceFn: vi.fn(),
      now: new Date("2026-06-01T00:00:00.000Z"),
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
    ]);
  });

  it("syncs selected due links with cached yearly receipt rows and continues after failures", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Timothy Ackert",
        election_year: 2026,
        office_name: "State Lower Chamber Legislator",
        district: "8",
        committee_id: "14376",
        committee_name: "ACKERT FOR THE 8TH",
        source_url: "https://seec.ct.gov/portal/ecris/CurPreYears",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_name: "Governor",
        district: null,
        committee_id: "20001",
        committee_name: "DOE FOR GOVERNOR",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "2",
      },
    ]);
    const syncConnecticutCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: false,
        resolution: { status: "matched", committeeId: "14376" },
        linkWritten: true,
        summaryWritten: true,
        directBreakdownsWritten: 3,
        totalReceipts: 100,
        matchedReceiptRowCount: 1,
        includedReceiptRowCount: 1,
        skippedReceiptRowCount: 0,
      })
      .mockRejectedValueOnce(new Error("eCRIS row parse failed"));
    const row = receipt({ "Committee ID": "14376", Amount: "100.00" });

    const result = await syncDueConnecticutCandidateFinance({
      db,
      syncConnecticutCandidateFinanceFn,
      now: new Date("2026-06-01T00:00:00.000Z"),
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      receiptDataByYear: new Map([
        [
          2026,
          receiptDataForYear({
            year: 2026,
            rowsByCommitteeId: new Map([["14376", [row]]]),
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
    expect(result.results[0]).toMatchObject({ ok: true, committeeId: "14376" });
    expect(result.results[1]).toMatchObject({
      ok: false,
      committeeId: "20001",
      error: "eCRIS row parse failed",
    });
    expect(syncConnecticutCandidateFinanceFn).toHaveBeenCalledTimes(2);
    expect(syncConnecticutCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Timothy Ackert",
        electionYear: 2026,
        officeName: "State Lower Chamber Legislator",
        district: "8",
        sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
        receiptRows: [row],
        receiptSourceUrl:
          "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles/Receipts2026ElectionYearCandidateExploratoryCommittees.csv",
      })
    );
    expect(syncConnecticutCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: "33333333-3333-4333-8333-333333333333",
        electionId: "44444444-4444-4444-8444-444444444444",
        officeName: "Governor",
        receiptRows: [],
      })
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      3,
      2,
      30,
      730,
    ]);
  });

  it("validates batch options", async () => {
    const db = createMockDb();

    await expect(
      syncDueConnecticutCandidateFinance({
        db,
        maxCandidates: 0,
      })
    ).rejects.toThrow("Invalid Connecticut finance batch sync maxCandidates");

    expect(db.query).not.toHaveBeenCalled();
  });
});
