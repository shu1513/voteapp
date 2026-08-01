import { describe, expect, it } from "vitest";

import {
  parsePresidentialPrimaryDatePayload,
  parsePresidentialPrimaryDatePayloadPartial,
} from "../../src/contracts/presidentialPrimaryDatePayloadContract.js";

function validPayload(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    results: [
      {
        state_fips: "06",
        state_name: "California",
        status: "official_found",
        primary_date: "2028-03-07",
        sources: ["https://elections.example.gov/2028-primary"],
      },
      {
        state_fips: "11",
        state_name: "District of Columbia",
        status: "not_official_yet",
        primary_date: null,
        sources: ["https://dcboe.example.gov/calendar"],
      },
    ],
    ...overrides,
  };
}

describe("parsePresidentialPrimaryDatePayload", () => {
  it("parses official and not-yet-official primary date results", () => {
    const parsed = parsePresidentialPrimaryDatePayload(validPayload(), {
      electionYear: 2028,
      expectedStateFips: ["11", "06"],
    });

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.payload.results).toEqual([
        {
          state_fips: "06",
          state_name: "California",
          status: "official_found",
          primary_date: "2028-03-07",
          sources: ["https://elections.example.gov/2028-primary"],
          notes: "",
        },
        {
          state_fips: "11",
          state_name: "District of Columbia",
          status: "not_official_yet",
          primary_date: null,
          sources: ["https://dcboe.example.gov/calendar"],
          notes: "",
        },
      ]);
    }
  });

  it("rejects payloads missing expected states", () => {
    const parsed = parsePresidentialPrimaryDatePayload(
      {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://elections.example.gov/2028-primary"],
            notes: "",
          },
        ],
      },
      {
        electionYear: 2028,
        expectedStateFips: ["06", "11"],
      }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("results missing state_fips: 11");
  });

  it("rejects unknown and duplicate state_fips values", () => {
    const unknown = parsePresidentialPrimaryDatePayload(
      {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://elections.example.gov/2028-primary"],
            notes: "",
          },
          {
            state_fips: "12",
            state_name: "Florida",
            status: "not_official_yet",
            primary_date: null,
            sources: ["https://dos.example.gov/calendar"],
            notes: "",
          },
        ],
      },
      {
        electionYear: 2028,
        expectedStateFips: ["06", "11"],
      }
    );
    expect(unknown.ok).toBe(false);
    expect(unknown.ok ? "" : unknown.reason).toContain("outside provided states: 12");

    const duplicate = parsePresidentialPrimaryDatePayload(
      {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://elections.example.gov/2028-primary"],
            notes: "",
          },
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://elections.example.gov/2028-primary"],
            notes: "",
          },
        ],
      },
      {
        electionYear: 2028,
        expectedStateFips: ["06"],
      }
    );
    expect(duplicate.ok).toBe(false);
    expect(duplicate.ok ? "" : duplicate.reason).toContain("duplicate state_fips: 06");
  });

  it("rejects mismatched state names", () => {
    const parsed = parsePresidentialPrimaryDatePayload(
      {
        results: [
          {
            state_fips: "06",
            state_name: "Florida",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://elections.example.gov/2028-primary"],
            notes: "",
          },
        ],
      },
      {
        electionYear: 2028,
        expectedStateFips: ["06"],
      }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("state_name does not match state_fips 06");
  });

  it("rejects date/status inconsistencies", () => {
    const wrongYear = parsePresidentialPrimaryDatePayload(
      {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2027-03-07",
            sources: ["https://elections.example.gov/2028-primary"],
            notes: "",
          },
        ],
      },
      {
        electionYear: 2028,
        expectedStateFips: ["06"],
      }
    );
    expect(wrongYear.ok).toBe(false);
    expect(wrongYear.ok ? "" : wrongYear.reason).toContain("official_found requires primary_date");

    const inferredDate = parsePresidentialPrimaryDatePayload(
      {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "not_official_yet",
            primary_date: "2028-03-07",
            sources: ["https://elections.example.gov/calendar"],
            notes: "",
          },
        ],
      },
      {
        electionYear: 2028,
        expectedStateFips: ["06"],
      }
    );
    expect(inferredDate.ok).toBe(false);
    expect(inferredDate.ok ? "" : inferredDate.reason).toContain("not_official_yet requires primary_date null");
  });

  it("rejects missing or invalid source URLs", () => {
    const parsed = parsePresidentialPrimaryDatePayload(
      {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["not-a-url"],
            notes: "",
          },
        ],
      },
      {
        electionYear: 2028,
        expectedStateFips: ["06"],
      }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("sources must be a non-empty array");
  });

  it("rejects invalid parser options", () => {
    expect(() =>
      parsePresidentialPrimaryDatePayload(validPayload(), {
        electionYear: 2026,
        expectedStateFips: ["06", "11"],
      })
    ).toThrow("Invalid presidential primary date election year: 2026");

    expect(() =>
      parsePresidentialPrimaryDatePayload(validPayload(), {
        electionYear: 2028,
        expectedStateFips: [],
      })
    ).toThrow("requires at least one expected state_fips");
  });
});

describe("parsePresidentialPrimaryDatePayloadPartial", () => {
  it("keeps valid rows while reporting invalid or missing states", () => {
    const parsed = parsePresidentialPrimaryDatePayloadPartial(
      {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://elections.example.gov/2028-primary"],
            notes: "",
          },
          {
            state_fips: "11",
            state_name: "District of Columbia",
            status: "official_found",
            primary_date: "2027-03-07",
            sources: ["https://dcboe.example.gov/calendar"],
            notes: "",
          },
          {
            state_fips: "99",
            state_name: "Not a state",
            status: "not_official_yet",
            primary_date: null,
            sources: ["https://example.gov/calendar"],
            notes: "",
          },
        ],
      },
      {
        electionYear: 2028,
        expectedStateFips: ["06", "11", "12"],
      }
    );

    expect(parsed.payload.results.map((row) => row.state_fips)).toEqual(["06"]);
    expect(parsed.failedRows).toEqual([
      {
        state_fips: "11",
        reason: "official_found requires primary_date in election_year",
      },
      {
        state_fips: "12",
        reason: "results missing state_fips: 12",
      },
    ]);
    expect(parsed.ignoredRowReasons).toEqual(["Ignored row with state_fips outside provided states: 99"]);
    expect(parsed.reviewFeedbackLines.join("\n")).toContain("Fix state_fips=11");
  });
});

describe("parsePresidentialPrimaryDatePayload source-domain policy", () => {
  it("rejects a primary-date row sourced from a blocked platform domain", () => {
    const parsed = parsePresidentialPrimaryDatePayload(
      validPayload({
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://www.reddit.com/r/california/comments/abc"],
          },
        ],
      }),
      { electionYear: 2028, expectedStateFips: ["06"] }
    );

    expect(parsed.ok).toBe(false);
    if (parsed.ok) {
      return;
    }
    expect(parsed.reason).toContain("sources:");
    expect(parsed.reason).toContain("user-generated/social platform");
  });
});
