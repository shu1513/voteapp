import { describe, expect, it, vi } from "vitest";

import {
  markPresidentialRosterResearchError,
  markPresidentialRosterResearchSuccess,
} from "../../../src/pipeline/presidential/presidentialRosterResearchWriter.js";

const cycleId = "11111111-1111-4111-8111-111111111111";

describe("presidentialRosterResearchWriter", () => {
  it("returns next tracking time after a successful update", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });

    await expect(
      markPresidentialRosterResearchSuccess(
        { query } as never,
        {
          cycleId,
          electionYear: 2028,
          researchedAt: new Date("2027-03-07T12:00:00.000Z"),
        }
      )
    ).resolves.toEqual({
      rowsUpdated: 1,
      nextResearchAt: "2027-03-14T12:00:00.000Z",
    });
  });

  it("returns next retry time after a failed update", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });

    await expect(
      markPresidentialRosterResearchError(
        { query } as never,
        {
          cycleId,
          electionYear: 2028,
          researchedAt: new Date("2027-07-07T12:00:00.000Z"),
          error: new Error("boom"),
        }
      )
    ).resolves.toEqual({
      rowsUpdated: 1,
      nextResearchAt: "2027-07-10T12:00:00.000Z",
    });
  });

  it("throws when success tracking updates no cycle row", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 0 });

    await expect(
      markPresidentialRosterResearchSuccess(
        { query } as never,
        {
          cycleId,
          electionYear: 2028,
          researchedAt: new Date("2027-03-07T12:00:00.000Z"),
        }
      )
    ).rejects.toThrow("presidential roster research success tracking expected to update exactly one");
  });

  it("throws when error tracking updates no cycle row", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 0 });

    await expect(
      markPresidentialRosterResearchError(
        { query } as never,
        {
          cycleId,
          electionYear: 2028,
          researchedAt: new Date("2027-07-07T12:00:00.000Z"),
          error: new Error("boom"),
        }
      )
    ).rejects.toThrow("presidential roster research error tracking expected to update exactly one");
  });
});
