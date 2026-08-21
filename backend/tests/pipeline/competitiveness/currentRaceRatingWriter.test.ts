import { describe, expect, it, vi } from "vitest";

import {
  upsertCurrentRaceRatings,
  type CurrentRaceRatingRecord,
} from "../../../src/pipeline/competitiveness/currentRaceRatingWriter.js";

const ELECTION_ID = "11111111-1111-4111-8111-111111111111";

function record(overrides: Partial<CurrentRaceRatingRecord> = {}): CurrentRaceRatingRecord {
  return {
    election_id: ELECTION_ID,
    method: "outlet_consensus",
    evidence_status: "rated",
    competitiveness_label: "competitive",
    confidence: "high",
    as_of: "2026-08-06",
    decisive_round: null,
    evidence: { observations: [] },
    source_url: "https://insideelections.com/ratings/senate",
    ...overrides,
  };
}

type QueryStub = { rows?: unknown[]; rowCount?: number };

function queryStub(results: QueryStub[]) {
  const query = vi.fn();
  for (const result of results) {
    query.mockResolvedValueOnce({ rows: [], rowCount: 0, ...result });
  }
  return query;
}

describe("currentRaceRatingWriter", () => {
  it("does nothing for an empty record list", async () => {
    const query = vi.fn();
    await expect(upsertCurrentRaceRatings({ query } as never, [])).resolves.toEqual({
      requested: 0,
      rowsWritten: 0,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("upserts a rating in a single guarded statement", async () => {
    const query = queryStub([{ rowCount: 1 }]);
    const researchedAt = new Date("2026-08-20T12:00:00.000Z");

    await expect(
      upsertCurrentRaceRatings({ query } as never, [record()], { researchedAt })
    ).resolves.toEqual({ requested: 1, rowsWritten: 1 });

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("INSERT INTO public.current_race_ratings");
    expect(query.mock.calls[0]?.[0]).toContain("ON CONFLICT (election_id)");
    // The refusal happens inside the statement, so it holds under concurrency.
    expect(query.mock.calls[0]?.[0]).toContain(
      "OR (EXCLUDED.evidence_status = 'rated' AND EXCLUDED.as_of >= current_race_ratings.as_of)"
    );
    expect(query.mock.calls[0]?.[1]).toEqual([
      ELECTION_ID,
      "current_race_rating.v1",
      "competitive",
      "outlet_consensus",
      "high",
      "rated",
      "2026-08-06",
      null,
      JSON.stringify({ observations: [] }),
      "https://insideelections.com/ratings/senate",
      "2026-08-20T12:00:00.000Z",
      false,
    ]);
  });

  it("refuses an upsert the guard filtered out and reports the stored as_of", async () => {
    const query = queryStub([
      { rowCount: 0 },
      { rows: [{ evidence_status: "rated", as_of: "2026-08-10" }] },
    ]);

    await expect(
      upsertCurrentRaceRatings({ query } as never, [record({ as_of: "2026-08-06" })])
    ).rejects.toThrow(/older than stored as_of 2026-08-10.*--force/);
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[0]).toContain("SELECT evidence_status, as_of::text");
  });

  it("reports a none_found refusal over a stored rating", async () => {
    const query = queryStub([
      { rowCount: 0 },
      { rows: [{ evidence_status: "rated", as_of: "2026-08-10" }] },
    ]);

    await expect(
      upsertCurrentRaceRatings({ query } as never, [
        record({
          evidence_status: "none_found",
          competitiveness_label: null,
          confidence: null,
          as_of: null,
        }),
      ])
    ).rejects.toThrow(/payload is none_found.*--force/);
  });

  it("passes force through to the guard so any write succeeds", async () => {
    const query = queryStub([{ rowCount: 1 }]);

    await expect(
      upsertCurrentRaceRatings({ query } as never, [record({ as_of: "2020-01-01" })], {
        force: true,
      })
    ).resolves.toEqual({ requested: 1, rowsWritten: 1 });
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[1]?.at(-1)).toBe(true);
  });

  it("stops at the first refused record", async () => {
    const second = record({ election_id: "22222222-2222-4222-8222-222222222222" });
    const query = queryStub([{ rowCount: 0 }, { rows: [] }]);

    await expect(
      upsertCurrentRaceRatings({ query } as never, [record(), second])
    ).rejects.toThrow(/upsert refused/);
    expect(query).toHaveBeenCalledTimes(2);
  });
});
