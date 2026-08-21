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

  it("inserts a new rating after finding no existing row", async () => {
    const query = queryStub([{ rows: [] }, { rowCount: 1 }]);
    const researchedAt = new Date("2026-08-20T12:00:00.000Z");

    await expect(
      upsertCurrentRaceRatings({ query } as never, [record()], { researchedAt })
    ).resolves.toEqual({ requested: 1, rowsWritten: 1 });

    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[0]?.[0]).toContain("SELECT evidence_status, as_of::text");
    expect(query.mock.calls[1]?.[0]).toContain("INSERT INTO public.current_race_ratings");
    expect(query.mock.calls[1]?.[0]).toContain("ON CONFLICT (election_id)");
    expect(query.mock.calls[1]?.[1]).toEqual([
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
    ]);
  });

  it("refuses an upsert whose as_of is older than the stored row", async () => {
    const query = queryStub([{ rows: [{ evidence_status: "rated", as_of: "2026-08-10" }] }]);

    await expect(
      upsertCurrentRaceRatings({ query } as never, [record({ as_of: "2026-08-06" })])
    ).rejects.toThrow(/older than stored as_of 2026-08-10/);
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("allows an upsert with an equal or newer as_of", async () => {
    for (const asOf of ["2026-08-10", "2026-08-12"]) {
      const query = queryStub([
        { rows: [{ evidence_status: "rated", as_of: "2026-08-10" }] },
        { rowCount: 1 },
      ]);
      await expect(
        upsertCurrentRaceRatings({ query } as never, [record({ as_of: asOf })])
      ).resolves.toEqual({ requested: 1, rowsWritten: 1 });
    }
  });

  it("refuses a none_found write over a stored rating without force", async () => {
    const query = queryStub([{ rows: [{ evidence_status: "rated", as_of: "2026-08-10" }] }]);

    await expect(
      upsertCurrentRaceRatings({ query } as never, [
        record({
          evidence_status: "none_found",
          competitiveness_label: null,
          confidence: null,
          as_of: null,
        }),
      ])
    ).rejects.toThrow(/payload is none_found/);
  });

  it("allows a rated write over a stored none_found row", async () => {
    const query = queryStub([
      { rows: [{ evidence_status: "none_found", as_of: null }] },
      { rowCount: 1 },
    ]);
    await expect(upsertCurrentRaceRatings({ query } as never, [record()])).resolves.toEqual({
      requested: 1,
      rowsWritten: 1,
    });
  });

  it("skips the existing-row check with force", async () => {
    const query = queryStub([{ rowCount: 1 }]);

    await expect(
      upsertCurrentRaceRatings({ query } as never, [record({ as_of: "2020-01-01" })], {
        force: true,
      })
    ).resolves.toEqual({ requested: 1, rowsWritten: 1 });
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("INSERT INTO public.current_race_ratings");
  });
});
