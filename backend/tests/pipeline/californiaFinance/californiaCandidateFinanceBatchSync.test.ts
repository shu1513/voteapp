import { describe, expect, it, vi } from "vitest";

import {
  listDueCaliforniaCandidateFinanceSyncRows,
  syncDueCaliforniaCandidateFinance,
} from "../../../src/pipeline/californiaFinance/californiaCandidateFinanceBatchSync.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

describe("californiaCandidateFinanceBatchSync", () => {
  it("lists due California finance sync rows from explicit active links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Gavin Newsom",
        election_year: 2026,
        office_name: "Governor",
        controlled_committee_id: "1456045",
        controlled_committee_name: "Newsom for California Governor 2026",
        source_url: "https://powersearch.sos.ca.gov/",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Rob Bonta",
        election_year: 2026,
        office_name: "Attorney General",
        controlled_committee_id: "9876543",
        controlled_committee_name: "Bonta for Attorney General 2026",
        source_url: null,
        last_synced_at: "2026-01-01 00:00:00+00",
        total_due_rows: "2",
      },
    ]);

    const result = await listDueCaliforniaCandidateFinanceSyncRows(db, {
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
          candidateName: "Gavin Newsom",
          electionYear: 2026,
          officeName: "Governor",
          controlledCommitteeId: "1456045",
          controlledCommitteeName: "Newsom for California Governor 2026",
          sourceUrl: "https://powersearch.sos.ca.gov/",
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Rob Bonta",
          electionYear: 2026,
          officeName: "Attorney General",
          controlledCommitteeId: "9876543",
          controlledCommitteeName: "Bonta for Attorney General 2026",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.ca_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'CA'");
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

    await syncDueCaliforniaCandidateFinance({
      db,
      syncCaliforniaCandidateFinanceFn: vi.fn(),
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

  it("syncs selected due links and continues after individual failures", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Gavin Newsom",
        election_year: 2026,
        office_name: "Governor",
        controlled_committee_id: "1456045",
        controlled_committee_name: "Newsom for California Governor 2026",
        source_url: "https://powersearch.sos.ca.gov/",
        last_synced_at: null,
        total_due_rows: "2",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Rob Bonta",
        election_year: 2026,
        office_name: "Attorney General",
        controlled_committee_id: "9876543",
        controlled_committee_name: "Bonta for Attorney General 2026",
        source_url: null,
        last_synced_at: null,
        total_due_rows: "2",
      },
    ]);
    const syncCaliforniaCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        electionYear: 2026,
        dryRun: false,
        outsideIncluded: true,
        linkWritten: true,
        summaryWritten: true,
        directBreakdownsWritten: 0,
        outsideGroupsWritten: 1,
        outsideGroupBreakdownsWritten: 0,
        outsideSupportTotal: 1000,
        outsideOpposeTotal: 0,
      })
      .mockRejectedValueOnce(new Error("Power Search unavailable"));

    const result = await syncDueCaliforniaCandidateFinance({
      db,
      syncCaliforniaCandidateFinanceFn,
      now: new Date("2026-06-01T00:00:00.000Z"),
      includeOutside: true,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      powerSearchOptions: { timeoutMs: 1000 },
    });

    expect(result).toMatchObject({
      dryRun: false,
      includeOutside: true,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
    });
    expect(result.results[0]).toMatchObject({ ok: true, controlledCommitteeId: "1456045" });
    expect(result.results[1]).toMatchObject({
      ok: false,
      controlledCommitteeId: "9876543",
      error: "Power Search unavailable",
    });
    expect(syncCaliforniaCandidateFinanceFn).toHaveBeenCalledTimes(2);
    expect(syncCaliforniaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Gavin Newsom",
        electionYear: 2026,
        officeName: "Governor",
        controlledCommitteeId: "1456045",
        controlledCommitteeName: "Newsom for California Governor 2026",
        sourceUrl: "https://powersearch.sos.ca.gov/",
        includeOutside: true,
        powerSearchOptions: { timeoutMs: 1000 },
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

  it("rejects invalid batch options before querying", async () => {
    const db = createMockDb();

    await expect(
      syncDueCaliforniaCandidateFinance({
        db,
        maxCandidates: 0,
      })
    ).rejects.toThrow("Invalid California finance batch sync maxCandidates");
    expect(db.query).not.toHaveBeenCalled();
  });
});
