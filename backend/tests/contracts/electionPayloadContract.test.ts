import { describe, expect, it } from "vitest";

import { parseCanonicalElectionPayload } from "../../src/contracts/electionPayloadContract.js";

describe("parseCanonicalElectionPayload", () => {
  it("parses valid payload", () => {
    const result = parseCanonicalElectionPayload({
      district_id: "d1",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-11-03",
          description: "General election for governor.",
          race_type: "office",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
      review_decision: "approve",
      review_reason: "Looks in scope",
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries).toHaveLength(1);
      expect(result.payload.entries[0].race_type).toBe("office");
    }
  });

  it("rejects invalid race_type", () => {
    const result = parseCanonicalElectionPayload({
      district_id: "d1",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Measure A",
          election_date: "2026-11-03",
          description: "Measure text",
          race_type: "measure",
          sources: ["https://example.gov/elections/measure-a"],
        },
      ],
    });

    expect(result.ok).toBe(false);
  });

  it("rejects non-http sources", () => {
    const result = parseCanonicalElectionPayload({
      district_id: "d1",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Measure A",
          election_date: "2026-11-03",
          description: "Measure text",
          race_type: "ballot_measure",
          sources: ["ftp://example.gov/elections/measure-a"],
        },
      ],
    });

    expect(result.ok).toBe(false);
  });

  it("rejects impossible calendar dates", () => {
    const result = parseCanonicalElectionPayload({
      district_id: "d1",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-02-30",
          description: "General election for governor.",
          race_type: "office",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
    });

    expect(result.ok).toBe(false);
  });

  it("accepts valid leap-day dates", () => {
    const result = parseCanonicalElectionPayload({
      district_id: "d1",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2028-02-29",
          description: "General election for governor.",
          race_type: "office",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
    });

    expect(result.ok).toBe(true);
  });
});
