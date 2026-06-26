import { describe, expect, it, vi } from "vitest";

import {
  listDueNewJerseyCandidateFinanceSyncRows,
  syncDueNewJerseyCandidateFinance,
} from "../../../src/pipeline/newJerseyFinance/newJerseyCandidateFinanceBatchSync.js";

const CANDIDATE_ID = "11111111-1111-1111-1111-111111111111";
const ELECTION_ID = "22222222-2222-2222-2222-222222222222";

function dueQueryRow() {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Mikie Sherrill",
    election_year: 2025,
    office_scope: "statewide",
    office_name: "Governor",
    district: "Statewide",
    source_url: null,
    last_synced_at: null,
    total_due_rows: "1",
  };
}

describe("newJerseyCandidateFinanceBatchSync", () => {
  it("lists due New Jersey candidate finance rows using eligible offices", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [dueQueryRow()], rowCount: 1 }),
    };

    const result = await listDueNewJerseyCandidateFinanceSyncRows(db, {
      now: new Date("2026-06-25T12:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 5,
      electionLookbackDays: 1,
      electionLookaheadDays: 730,
    });

    expect(result).toEqual({
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Mikie Sherrill",
          electionYear: 2025,
          officeScope: "statewide",
          officeName: "Governor",
          district: "Statewide",
          sourceUrl: null,
          lastSyncedAt: null,
        },
      ],
      totalDueRows: 1,
    });
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-25T12:00:00.000Z",
      7,
      5,
      1,
      730,
      [
        "statewide::governor",
        "statewide::lieutenant governor",
        "state_upper::state senator",
        "state_lower::state lower chamber legislator",
      ],
    ]);
  });

  it("syncs due rows through the ELEC-backed one-candidate sync", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({ rows: [dueQueryRow()], rowCount: 1 }),
      connect: vi.fn(),
    };
    const syncFn = vi.fn().mockResolvedValue({
      status: "matched",
      resolution: {
        status: "matched",
        entityS: 473742,
        entityName: "SHERRILL, MIKIE",
      },
      contributionRowsResult: { recordsTotal: 0, recordsFiltered: 0, rows: [], sourceUrl: "https://example.test" },
      syncResult: { directContributionTotal: 350 },
    });

    const result = await syncDueNewJerseyCandidateFinance({
      db,
      now: new Date("2026-06-25T12:00:00.000Z"),
      dryRun: true,
      maxCandidates: 5,
      syncNewJerseyCandidateFinanceFromElecFn: syncFn,
    });

    expect(syncFn).toHaveBeenCalledWith({
      db,
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Mikie Sherrill",
      electionYear: 2025,
      officeScope: "statewide",
      officeName: "Governor",
      district: "Statewide",
      sourceUrl: null,
      dryRun: true,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });
    expect(result).toMatchObject({
      dryRun: true,
      staleAfterDays: 7,
      maxCandidates: 5,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      skippedCandidateCount: 0,
      failedCandidateCount: 0,
    });
    expect(result.results[0]).toMatchObject({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2025,
      ok: true,
      status: "matched",
      candidateEntityS: 473742,
    });
  });
});
