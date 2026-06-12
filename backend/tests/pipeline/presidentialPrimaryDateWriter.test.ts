import { describe, expect, it, vi } from "vitest";

import {
  markPresidentialPrimaryDateResearchError,
  writePresidentialPrimaryDatePayloadRows,
} from "../../src/pipeline/presidential/presidentialPrimaryDateWriter.js";

const CYCLE_ID = "00000000-0000-4000-8000-000000000001";
const RESEARCHED_AT = new Date("2027-03-07T00:00:00.000Z");
const NEXT_RESEARCH_AT = "2027-03-14T00:00:00.000Z";

function makeDb(rowCount = 1) {
  return {
    query: vi.fn(async () => ({ rowCount, rows: [] })),
  };
}

describe("presidentialPrimaryDateWriter", () => {
  it("writes official_found rows as terminal primary dates", async () => {
    const db = makeDb();

    const result = await writePresidentialPrimaryDatePayloadRows(db as never, {
      cycleId: CYCLE_ID,
      researchedAt: RESEARCHED_AT,
      payload: {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://www.sos.ca.gov/elections/calendar"],
            notes: "Official calendar lists the date.",
          },
        ],
      },
    });

    expect(result).toEqual({
      officialFoundCount: 1,
      notOfficialYetCount: 0,
      rowsUpdated: 1,
      nextResearchAt: null,
    });
    expect(db.query).toHaveBeenCalledWith(expect.stringContaining("date_research_status = $4"), [
      CYCLE_ID,
      "06",
      "2028-03-07",
      "official_found",
      RESEARCHED_AT.toISOString(),
      null,
      "Official calendar lists the date.",
      JSON.stringify(["https://www.sos.ca.gov/elections/calendar"]),
    ]);
  });

  it("writes not_official_yet rows with a weekly next_research_at", async () => {
    const db = makeDb();

    const result = await writePresidentialPrimaryDatePayloadRows(db as never, {
      cycleId: CYCLE_ID,
      researchedAt: RESEARCHED_AT,
      payload: {
        results: [
          {
            state_fips: "12",
            state_name: "Florida",
            status: "not_official_yet",
            primary_date: null,
            sources: ["https://dos.fl.gov/elections/"],
            notes: "",
          },
        ],
      },
    });

    expect(result).toEqual({
      officialFoundCount: 0,
      notOfficialYetCount: 1,
      rowsUpdated: 1,
      nextResearchAt: NEXT_RESEARCH_AT,
    });
    expect(db.query).toHaveBeenCalledWith(expect.any(String), [
      CYCLE_ID,
      "12",
      null,
      "not_official_yet",
      RESEARCHED_AT.toISOString(),
      NEXT_RESEARCH_AT,
      null,
      JSON.stringify(["https://dos.fl.gov/elections/"]),
    ]);
  });

  it("marks AI failures as retryable row errors", async () => {
    const db = {
      query: vi.fn(async () => ({ rowCount: 2, rows: [] })),
    };

    const result = await markPresidentialPrimaryDateResearchError(db as never, {
      cycleId: CYCLE_ID,
      stateFipsList: ["06", "12", "06"],
      researchedAt: RESEARCHED_AT,
      error: "source URL is not reachable",
    });

    expect(result).toEqual({
      rowsUpdated: 2,
      nextResearchAt: NEXT_RESEARCH_AT,
    });
    expect(db.query).toHaveBeenCalledWith(expect.stringContaining("date_research_status = 'error'"), [
      CYCLE_ID,
      ["06", "12"],
      RESEARCHED_AT.toISOString(),
      NEXT_RESEARCH_AT,
      "source URL is not reachable",
    ]);
  });

  it("throws when an expected row is missing", async () => {
    const db = makeDb(0);

    await expect(
      writePresidentialPrimaryDatePayloadRows(db as never, {
        cycleId: CYCLE_ID,
        researchedAt: RESEARCHED_AT,
        payload: {
          results: [
            {
              state_fips: "06",
              state_name: "California",
              status: "official_found",
              primary_date: "2028-03-07",
              sources: ["https://www.sos.ca.gov/elections/calendar"],
              notes: "",
            },
          ],
        },
      })
    ).rejects.toThrow("Expected to update one presidential primary date row");
  });
});
