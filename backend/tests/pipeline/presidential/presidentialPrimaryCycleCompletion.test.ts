import { describe, expect, it, vi } from "vitest";

import { completeExpiredPresidentialPrimaryCycles } from "../../../src/pipeline/presidential/presidentialPrimaryCycleCompletion.js";

describe("completeExpiredPresidentialPrimaryCycles", () => {
  it("marks only primary cycles past the research stop date as completed", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          { id: "cycle-2028-democratic", election_year: 2028 },
          { id: "cycle-2032-democratic", election_year: 2032 },
        ],
      })
      .mockResolvedValueOnce({
        rowCount: 1,
        rows: [{ id: "cycle-2028-democratic" }],
      });

    await expect(
      completeExpiredPresidentialPrimaryCycles(
        { query },
        { now: new Date("2028-12-07T00:00:00.000Z") }
      )
    ).resolves.toEqual({
      dryRun: false,
      now: "2028-12-07T00:00:00.000Z",
      scannedCycleCount: 2,
      expiredCycleCount: 1,
      completedCycleCount: 1,
      completedCycleIds: ["cycle-2028-democratic"],
    });

    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[0]).toContain("SET status = 'completed'");
    expect(query.mock.calls[1]?.[1]).toEqual([["cycle-2028-democratic"]]);
  });

  it("reports expired cycles without updating them in dry-run mode", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [{ id: "cycle-2028-republican", election_year: 2028 }],
    });

    await expect(
      completeExpiredPresidentialPrimaryCycles(
        { query },
        { dryRun: true, now: new Date("2028-12-07T00:00:00.000Z") }
      )
    ).resolves.toEqual({
      dryRun: true,
      now: "2028-12-07T00:00:00.000Z",
      scannedCycleCount: 1,
      expiredCycleCount: 1,
      completedCycleCount: 0,
      completedCycleIds: [],
    });

    expect(query).toHaveBeenCalledTimes(1);
  });

  it("does nothing when no scanned cycles are expired", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [{ id: "cycle-2028-republican", election_year: 2028 }],
    });

    await expect(
      completeExpiredPresidentialPrimaryCycles(
        { query },
        { now: new Date("2028-12-06T23:59:59.999Z") }
      )
    ).resolves.toMatchObject({
      expiredCycleCount: 0,
      completedCycleCount: 0,
      completedCycleIds: [],
    });

    expect(query).toHaveBeenCalledTimes(1);
  });
});
