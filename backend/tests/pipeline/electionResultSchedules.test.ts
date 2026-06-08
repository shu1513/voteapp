import { describe, expect, it, vi } from "vitest";

import {
  computeElectionResultScheduledAtUtc,
  getCertifiedOffsetDaysForState,
  getElectionResultScheduleForState,
} from "../../src/pipeline/electionResults/electionResultSchedules.js";

describe("election result schedules", () => {
  it("contains California election-night schedule at 8:10 p.m. Pacific", () => {
    const schedule = getElectionResultScheduleForState("CA");

    expect(schedule?.electionNight).toMatchObject({
      localTime: "20:10",
      timeZone: "America/Los_Angeles",
    });
  });

  it("contains New York election-night schedule at 9:10 p.m. Eastern", () => {
    const schedule = getElectionResultScheduleForState("NY");

    expect(schedule?.electionNight).toMatchObject({
      localTime: "21:10",
      timeZone: "America/New_York",
    });
  });

  it("uses configured E+30 certified schedules when available", () => {
    expect(getCertifiedOffsetDaysForState("WA")).toBe(30);
    expect(getElectionResultScheduleForState("WA")?.certified.strategy).toBe("offset_days");
  });

  it("uses fallback certified schedules for vague states", () => {
    const schedule = getElectionResultScheduleForState("PA");

    expect(schedule?.certified.strategy).toBe("fallback_offset_days");
    expect(schedule?.certified.offsetDays).toBe(45);
  });

  it("delegates local-time conversion to Postgres with the expected schedule parameters", async () => {
    const scheduledAt = new Date("2026-06-03T03:10:00.000Z");
    const query = vi.fn(async () => ({ rows: [{ scheduled_at: scheduledAt }] }));

    const result = await computeElectionResultScheduledAtUtc(
      { query },
      {
        state: "CA",
        electionDate: "2026-06-02",
        passType: "election_night",
      }
    );

    expect(result).toEqual(scheduledAt);
    expect(query).toHaveBeenCalledWith(expect.stringContaining("AT TIME ZONE"), [
      "2026-06-02",
      0,
      "20:10",
      "America/Los_Angeles",
    ]);
  });
});
