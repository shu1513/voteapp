import { describe, expect, it, vi } from "vitest";

import {
  buildPresidentialCycleSeeds,
  getPresidentialGeneralElectionDate,
  getUpcomingPresidentialElectionYears,
  isPresidentialElectionYear,
  upsertPresidentialCycles,
} from "../../../src/pipeline/presidential/presidentialCycles.js";

describe("presidentialCycles", () => {
  it("identifies presidential election years", () => {
    expect(isPresidentialElectionYear(2024)).toBe(true);
    expect(isPresidentialElectionYear(2028)).toBe(true);
    expect(isPresidentialElectionYear(2026)).toBe(false);
    expect(isPresidentialElectionYear(2027)).toBe(false);
  });

  it("computes presidential general Election Day", () => {
    expect(getPresidentialGeneralElectionDate(2028)).toBe("2028-11-07");
    expect(getPresidentialGeneralElectionDate(2032)).toBe("2032-11-02");
    expect(getPresidentialGeneralElectionDate(2036)).toBe("2036-11-04");
    expect(getPresidentialGeneralElectionDate(2040)).toBe("2040-11-06");
    expect(getPresidentialGeneralElectionDate(2044)).toBe("2044-11-08");
  });

  it("rejects non-presidential years for presidential general dates", () => {
    expect(() => getPresidentialGeneralElectionDate(2026)).toThrow(
      "Year is not a presidential election year"
    );
  });

  it("returns the next presidential cycle years from a date", () => {
    expect(getUpcomingPresidentialElectionYears(new Date("2026-06-11T12:00:00.000Z"), 5)).toEqual([
      2028,
      2032,
      2036,
      2040,
      2044,
    ]);
  });

  it("keeps the current presidential year until Election Day has passed", () => {
    expect(getUpcomingPresidentialElectionYears(new Date("2028-11-07T23:59:59.000Z"), 2)).toEqual([
      2028,
      2032,
    ]);
    expect(getUpcomingPresidentialElectionYears(new Date("2028-11-08T00:00:00.000Z"), 2)).toEqual([
      2032,
      2036,
    ]);
  });

  it("rejects invalid reference dates and counts", () => {
    expect(() => getUpcomingPresidentialElectionYears(new Date("not-a-date"), 5)).toThrow(
      "Invalid presidential cycle reference date"
    );
    expect(() => getUpcomingPresidentialElectionYears(new Date("2026-06-11T12:00:00.000Z"), 0)).toThrow(
      "Invalid presidential cycle count"
    );
    expect(() => getUpcomingPresidentialElectionYears(new Date("2026-06-11T12:00:00.000Z"), 20)).toThrow(
      "Only 19 supported presidential cycles remain through 2100"
    );
  });

  it("builds general and party primary cycle seeds", () => {
    const seeds = buildPresidentialCycleSeeds(new Date("2026-06-11T12:00:00.000Z"), 2);

    expect(seeds).toEqual([
      {
        electionYear: 2028,
        stage: "general",
        party: null,
        electionDate: "2028-11-07",
        status: "active",
        sources: [],
      },
      {
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
        electionDate: null,
        status: "active",
        sources: [],
      },
      {
        electionYear: 2028,
        stage: "primary",
        party: "Republican",
        electionDate: null,
        status: "active",
        sources: [],
      },
      {
        electionYear: 2032,
        stage: "general",
        party: null,
        electionDate: "2032-11-02",
        status: "active",
        sources: [],
      },
      {
        electionYear: 2032,
        stage: "primary",
        party: "Democratic",
        electionDate: null,
        status: "active",
        sources: [],
      },
      {
        electionYear: 2032,
        stage: "primary",
        party: "Republican",
        electionDate: null,
        status: "active",
        sources: [],
      },
    ]);
  });

  it("builds five cycles by default with one general and two primary rows per year", () => {
    const seeds = buildPresidentialCycleSeeds(new Date("2026-06-11T12:00:00.000Z"));

    expect(seeds).toHaveLength(15);
    expect(Array.from(new Set(seeds.map((seed) => seed.electionYear)))).toEqual([
      2028,
      2032,
      2036,
      2040,
      2044,
    ]);

    for (const electionYear of [2028, 2032, 2036, 2040, 2044]) {
      const yearSeeds = seeds.filter((seed) => seed.electionYear === electionYear);
      expect(yearSeeds).toHaveLength(3);
      expect(yearSeeds.filter((seed) => seed.stage === "general")).toHaveLength(1);
      expect(yearSeeds.filter((seed) => seed.stage === "primary").map((seed) => seed.party)).toEqual([
        "Democratic",
        "Republican",
      ]);
    }
  });

  it("trims custom primary parties and rejects blank party names", () => {
    const seeds = buildPresidentialCycleSeeds(new Date("2026-06-11T12:00:00.000Z"), 1, [
      " Democratic ",
      " Green ",
    ]);

    expect(seeds.map((seed) => seed.party)).toEqual([null, "Democratic", "Green"]);
    expect(() =>
      buildPresidentialCycleSeeds(new Date("2026-06-11T12:00:00.000Z"), 1, ["Democratic", " "])
    ).toThrow("Presidential primary party cannot be blank");
    expect(() => buildPresidentialCycleSeeds(new Date("2026-06-11T12:00:00.000Z"), 1, [])).toThrow(
      "At least one presidential primary party is required"
    );
  });

  it("upserts general and primary cycle seeds with stage-specific conflict targets", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rowCount: 1, rows: [{ id: "general-cycle" }] })
      .mockResolvedValueOnce({ rowCount: 0, rows: [] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [{ id: "republican-primary" }] });

    const seeds = buildPresidentialCycleSeeds(new Date("2026-06-11T12:00:00.000Z"), 1);
    const result = await upsertPresidentialCycles({ query }, seeds);

    expect(result).toEqual({ requested: 3, changed: 2, unchanged: 1 });
    expect(query).toHaveBeenCalledTimes(3);

    expect(query.mock.calls[0][0]).toContain("ON CONFLICT (election_year) WHERE stage = 'general'");
    expect(query.mock.calls[0][0]).toContain("election_date = EXCLUDED.election_date");
    expect(query.mock.calls[0][0]).not.toContain("status = EXCLUDED.status");
    expect(query.mock.calls[0][1]).toEqual([2028, "2028-11-07", "active", "[]"]);

    expect(query.mock.calls[1][0]).toContain("ON CONFLICT (election_year, party) WHERE stage = 'primary'");
    expect(query.mock.calls[1][0]).toContain("DO NOTHING");
    expect(query.mock.calls[1][1]).toEqual([2028, "Democratic", "active", "[]"]);

    expect(query.mock.calls[2][0]).toContain("ON CONFLICT (election_year, party) WHERE stage = 'primary'");
    expect(query.mock.calls[2][1]).toEqual([2028, "Republican", "active", "[]"]);
  });

  it("returns unchanged for an empty upsert seed list without querying", async () => {
    const query = vi.fn();

    await expect(upsertPresidentialCycles({ query }, [])).resolves.toEqual({
      requested: 0,
      changed: 0,
      unchanged: 0,
    });
    expect(query).not.toHaveBeenCalled();
  });

  it("reports all seeds unchanged when rerun against existing matching rows", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 0, rows: [] });
    const seeds = buildPresidentialCycleSeeds(new Date("2026-06-11T12:00:00.000Z"), 1);

    await expect(upsertPresidentialCycles({ query }, seeds)).resolves.toEqual({
      requested: 3,
      changed: 0,
      unchanged: 3,
    });
    expect(query).toHaveBeenCalledTimes(3);
  });

  it("rejects invalid cycle seeds before issuing SQL", async () => {
    const query = vi.fn();

    await expect(
      upsertPresidentialCycles({ query }, [
        {
          electionYear: 2028,
          stage: "primary",
          party: "Democratic",
          electionDate: "2028-11-07",
          status: "active",
          sources: [],
        },
      ])
    ).rejects.toThrow("Primary presidential cycle seed electionDate must be null");

    expect(query).not.toHaveBeenCalled();

    await expect(
      upsertPresidentialCycles({ query }, [
        {
          electionYear: 2028,
          stage: "primary",
          party: " Democratic ",
          electionDate: null,
          status: "active",
          sources: [],
        },
      ])
    ).rejects.toThrow("Primary presidential cycle seed party must be trimmed");

    expect(query).not.toHaveBeenCalled();
  });

  it("rejects invalid general cycle seeds before issuing SQL", async () => {
    const query = vi.fn();

    await expect(
      upsertPresidentialCycles({ query }, [
        {
          electionYear: 2028,
          stage: "general",
          party: "Democratic",
          electionDate: "2028-11-07",
          status: "active",
          sources: [],
        },
      ])
    ).rejects.toThrow("General presidential cycle seed party must be null");

    expect(query).not.toHaveBeenCalled();

    await expect(
      upsertPresidentialCycles({ query }, [
        {
          electionYear: 2028,
          stage: "general",
          party: null,
          electionDate: "2028-11-08",
          status: "active",
          sources: [],
        },
      ])
    ).rejects.toThrow("Invalid general presidential election date for 2028");

    expect(query).not.toHaveBeenCalled();
  });

  it("validates the full seed batch before issuing any SQL", async () => {
    const query = vi.fn();

    await expect(
      upsertPresidentialCycles({ query }, [
        {
          electionYear: 2028,
          stage: "general",
          party: null,
          electionDate: "2028-11-07",
          status: "active",
          sources: [],
        },
        {
          electionYear: 2028,
          stage: "primary",
          party: " Democratic ",
          electionDate: null,
          status: "active",
          sources: [],
        },
      ])
    ).rejects.toThrow("Primary presidential cycle seed party must be trimmed");

    expect(query).not.toHaveBeenCalled();
  });
});
