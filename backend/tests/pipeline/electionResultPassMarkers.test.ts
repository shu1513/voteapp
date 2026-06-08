import { describe, expect, it, vi } from "vitest";

import {
  buildElectionResultPassEmittedMarkerKey,
  clearElectionResultPassEmitted,
  isElectionResultPassEmitted,
  markElectionResultPassEmitted,
} from "../../src/pipeline/electionResults/electionResultPassMarkers.js";

describe("election result pass emitted markers", () => {
  it("builds one marker key per election/pass", () => {
    expect(
      buildElectionResultPassEmittedMarkerKey({
        electionId: "e-1",
        passType: "certified",
      })
    ).toBe("staging:election_result_emitted:certified:e-1");
  });

  it("checks and writes marker keys with a ttl", async () => {
    const redis = {
      exists: vi.fn(async () => 1),
      set: vi.fn(async () => "OK"),
    };

    await expect(
      isElectionResultPassEmitted(redis, {
        electionId: "e-1",
        passType: "election_night",
      })
    ).resolves.toBe(true);

    await markElectionResultPassEmitted(redis, {
      electionId: "e-1",
      passType: "election_night",
      emittedAt: "2026-06-03T00:00:00.000Z",
      ttlSeconds: 120,
    });

    expect(redis.set).toHaveBeenCalledWith(
      "staging:election_result_emitted:election_night:e-1",
      "2026-06-03T00:00:00.000Z",
      { EX: 120 }
    );
  });

  it("clears marker keys", async () => {
    const redis = {
      del: vi.fn(async () => 1),
    };

    await clearElectionResultPassEmitted(redis, {
      electionId: "e-1",
      passType: "certified",
    });

    expect(redis.del).toHaveBeenCalledWith("staging:election_result_emitted:certified:e-1");
  });
});
