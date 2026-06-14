import { describe, expect, it, vi } from "vitest";

import {
  markPresidentialNomineeResearchError,
  markPresidentialNomineeResearchSuccess,
} from "../../../src/pipeline/presidential/presidentialNomineeResearchWriter.js";

const cycleId = "11111111-1111-4111-8111-111111111111";

describe("presidentialNomineeResearchWriter", () => {
  it("returns next tracking time after a successful update", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });

    await expect(
      markPresidentialNomineeResearchSuccess(
        { query } as never,
        {
          cycleId,
          electionYear: 2028,
          researchedAt: new Date("2028-02-07T12:00:00.000Z"),
        }
      )
    ).resolves.toEqual({
      rowsUpdated: 1,
      nextResearchAt: "2028-02-09T12:00:00.000Z",
    });
  });

  it("does not schedule a next nominee check when research is stopped", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });

    await expect(
      markPresidentialNomineeResearchSuccess(
        { query } as never,
        {
          cycleId,
          electionYear: 2028,
          researchedAt: new Date("2028-03-15T12:00:00.000Z"),
          stopResearch: true,
        }
      )
    ).resolves.toEqual({
      rowsUpdated: 1,
      nextResearchAt: null,
    });
  });

  it("throws when success tracking updates no cycle row", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 0 });

    await expect(
      markPresidentialNomineeResearchSuccess(
        { query } as never,
        {
          cycleId,
          electionYear: 2028,
          researchedAt: new Date("2028-02-07T12:00:00.000Z"),
        }
      )
    ).rejects.toThrow("presidential nominee research success tracking expected to update exactly one");
  });

  it("throws when error tracking updates no cycle row", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 0 });

    await expect(
      markPresidentialNomineeResearchError(
        { query } as never,
        {
          cycleId,
          electionYear: 2028,
          researchedAt: new Date("2028-02-07T12:00:00.000Z"),
          error: new Error("boom"),
        }
      )
    ).rejects.toThrow("presidential nominee research error tracking expected to update exactly one");
  });
});
