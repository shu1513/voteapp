import { describe, expect, it, vi } from "vitest";

import {
  claimCandidateRecordsSearch,
  markCandidateRecordsSearchCompleted,
  releaseCandidateRecordsSearchClaim,
} from "../../src/pipeline/candidates/candidateRecordsSearchClaim.js";

describe("claimCandidateRecordsSearch", () => {
  it("returns claimed=true when UPDATE returns a row", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          id: "cand-1",
          last_records_searched_at: "2026-03-01T00:00:00.000Z",
          last_records_researched_through: "2026-02-28",
        },
      ],
    });

    const result = await claimCandidateRecordsSearch(
      { query },
      {
        candidateId: "cand-1",
        asOf: new Date("2026-05-31T00:00:00.000Z"),
        cooldownDays: 30,
        leaseHours: 2,
      }
    );

    expect(result).toEqual({
      claimed: true,
      candidateId: "cand-1",
      lastRecordsSearchedAt: "2026-03-01T00:00:00.000Z",
      lastRecordsResearchedThrough: "2026-02-28",
    });
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("UPDATE public.candidates");
    expect(query.mock.calls[0]?.[0]).toContain("records_search_claimed_at");
  });

  it("returns claimed=false when no row is returned", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });

    const result = await claimCandidateRecordsSearch({ query }, { candidateId: "cand-2" });

    expect(result).toEqual({
      claimed: false,
      candidateId: "cand-2",
      lastRecordsSearchedAt: null,
      lastRecordsResearchedThrough: null,
    });
  });
});

describe("markCandidateRecordsSearchCompleted", () => {
  it("updates searched timestamp, researched_through, and clears claim", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 1 });

    await markCandidateRecordsSearchCompleted(
      { query },
      "cand-3",
      new Date("2026-05-30T12:00:00.000Z")
    );

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("last_records_searched_at = now()");
    expect(query.mock.calls[0]?.[0]).toContain("last_records_researched_through = $2::date");
    expect(query.mock.calls[0]?.[0]).toContain("records_search_claimed_at = NULL");
    expect(query.mock.calls[0]?.[1]).toEqual(["cand-3", "2026-05-30"]);
  });
});

describe("releaseCandidateRecordsSearchClaim", () => {
  it("clears claim timestamp", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 1 });

    await releaseCandidateRecordsSearchClaim({ query }, "cand-4");

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("records_search_claimed_at = NULL");
    expect(query.mock.calls[0]?.[1]).toEqual(["cand-4"]);
  });
});
