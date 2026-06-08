import { describe, expect, it, vi } from "vitest";

import type { ParsedElectionResultPayloadRow } from "../../src/contracts/electionResultPayloadContract.js";
import {
  markElectionResultPassAttemptAndChecked,
  markElectionResultPassChecked,
} from "../../src/pipeline/electionResults/electionResultCheckedTimestamps.js";

const ELECTION_ID = "00000000-0000-0000-0000-000000000001";

function resultRow(overrides: Partial<ParsedElectionResultPayloadRow> = {}): ParsedElectionResultPayloadRow {
  return {
    election_id: ELECTION_ID,
    result_status: "unofficial",
    outcome: "won",
    winners: [
      {
        candidate_election_id: "10000000-0000-0000-0000-000000000001",
        candidate_id: "20000000-0000-0000-0000-000000000001",
        candidate_name: "Jane Candidate",
      },
    ],
    match_status: "matched",
    source_url: "https://elections.example.gov/results",
    source_type: "official",
    notes: "",
    ...overrides,
  };
}

describe("markElectionResultPassChecked", () => {
  it("updates the election-night checked timestamp for unique election ids", async () => {
    const query = vi.fn(async () => ({ rowCount: 2 }));

    const count = await markElectionResultPassChecked(
      { query },
      {
        electionIds: [
          "00000000-0000-0000-0000-000000000001",
          "00000000-0000-0000-0000-000000000001",
          "00000000-0000-0000-0000-000000000002",
        ],
        passType: "election_night",
        checkedAt: new Date("2026-06-03T03:20:00.000Z"),
      }
    );

    expect(count).toBe(2);
    expect(query).toHaveBeenCalledWith(expect.stringContaining("election_night_results_checked_at"), [
      ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"],
      "2026-06-03T03:20:00.000Z",
    ]);
  });

  it("updates the certified checked timestamp for certified pass", async () => {
    const query = vi.fn(async () => ({ rowCount: 1 }));

    await markElectionResultPassChecked(
      { query },
      {
        electionIds: ["00000000-0000-0000-0000-000000000001"],
        passType: "certified",
        checkedAt: new Date("2026-07-10T17:00:00.000Z"),
      }
    );

    expect(query).toHaveBeenCalledWith(expect.stringContaining("certified_results_checked_at"), [
      ["00000000-0000-0000-0000-000000000001"],
      "2026-07-10T17:00:00.000Z",
    ]);
  });

  it("increments election-night attempt without marking checked for unclear early results", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ id: ELECTION_ID, election_night_results_attempt_count: 1 }],
      })
      .mockResolvedValueOnce({ rowCount: 0, rows: [] });

    const result = await markElectionResultPassAttemptAndChecked(
      { query },
      {
        rows: [resultRow({ result_status: "not_final_yet", outcome: "unknown", winners: [], match_status: "unmatched" })],
        passType: "election_night",
        checkedAt: new Date("2026-06-03T03:20:00.000Z"),
      }
    );

    expect(result).toEqual({
      attemptedElectionCount: 1,
      checkedElectionCount: 0,
      checkedElectionIds: [],
      uncheckedElectionIds: [ELECTION_ID],
    });
    expect(query).toHaveBeenCalledTimes(1);
    expect(String(query.mock.calls[0][0])).toContain("election_night_results_attempt_count");
  });

  it("marks election-night checked after the third unclear attempt", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ id: ELECTION_ID, election_night_results_attempt_count: 3 }],
      })
      .mockResolvedValueOnce({ rowCount: 1, rows: [] });

    const result = await markElectionResultPassAttemptAndChecked(
      { query },
      {
        rows: [resultRow({ result_status: "not_found", outcome: "unknown", winners: [], match_status: "not_found" })],
        passType: "election_night",
        checkedAt: new Date("2026-06-05T03:20:00.000Z"),
      }
    );

    expect(result).toEqual({
      attemptedElectionCount: 1,
      checkedElectionCount: 1,
      checkedElectionIds: [ELECTION_ID],
      uncheckedElectionIds: [],
    });
    expect(query.mock.calls[1][1]).toEqual([[ELECTION_ID], "2026-06-05T03:20:00.000Z"]);
  });

  it("does not mark certified office results checked while winners are unmatched", async () => {
    const query = vi.fn(async () => ({
      rowCount: 1,
      rows: [{ id: ELECTION_ID, certified_results_attempt_count: 1 }],
    }));

    const result = await markElectionResultPassAttemptAndChecked(
      { query },
      {
        rows: [
          resultRow({
            result_status: "certified",
            outcome: "won",
            winners: [{ candidate_name: "Pat Connolly" }],
            match_status: "unmatched",
          }),
        ],
        passType: "certified",
        checkedAt: new Date("2026-07-10T17:00:00.000Z"),
      }
    );

    expect(result).toEqual({
      attemptedElectionCount: 1,
      checkedElectionCount: 0,
      checkedElectionIds: [],
      uncheckedElectionIds: [ELECTION_ID],
    });
    expect(query).toHaveBeenCalledTimes(1);
    expect(String(query.mock.calls[0][0])).toContain("certified_results_attempt_count");
  });

  it("marks certified checked on the third unresolved certified attempt", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ id: ELECTION_ID, certified_results_attempt_count: 3 }],
      })
      .mockResolvedValueOnce({ rowCount: 1, rows: [] });

    const result = await markElectionResultPassAttemptAndChecked(
      { query },
      {
        rows: [
          resultRow({
            result_status: "not_final_yet",
            outcome: "unknown",
            winners: [],
            match_status: "not_found",
          }),
        ],
        passType: "certified",
        checkedAt: new Date("2026-07-24T17:00:00.000Z"),
      }
    );

    expect(result).toEqual({
      attemptedElectionCount: 1,
      checkedElectionCount: 1,
      checkedElectionIds: [ELECTION_ID],
      uncheckedElectionIds: [],
    });
    expect(query.mock.calls[1][1]).toEqual([[ELECTION_ID], "2026-07-24T17:00:00.000Z"]);
  });
});
