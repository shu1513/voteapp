import { describe, expect, it } from "vitest";

import { parseAiElectionEntriesPayload, parseCanonicalElectionPayload } from "../../src/contracts/electionPayloadContract.js";

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

describe("parseAiElectionEntriesPayload", () => {
  it("parses impact and maps it to canonical description", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "County Sheriff",
          election_date: "2026-11-03",
          impact:
            "Leads the county sheriff's department, oversees patrol and jail operations, and sets local law-enforcement priorities.",
          race_type: "office",
          sources: ["https://example.gov/elections/sheriff"],
        },
      ],
      review_decision: "approve",
      review_reason: "In scope",
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries).toHaveLength(1);
      expect(result.payload.entries[0].description).toContain("Leads the county sheriff");
    }
  });

  it("parses valid entries-only AI payload", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-11-03",
          impact: "Leads the state executive branch and signs or vetoes legislation.",
          race_type: "office",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
      review_decision: "approve",
      review_reason: "In scope",
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries).toHaveLength(1);
      expect(result.payload.review_decision).toBe("approve");
    }
  });

  it("parses optional election_stage when valid", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-11-03",
          impact: "Leads the state executive branch and signs or vetoes legislation.",
          race_type: "office",
          election_stage: "general",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries[0].election_stage).toBe("general");
    }
  });

  it("parses optional is_partisan for office entries", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-11-03",
          impact: "Leads the state executive branch and signs or vetoes legislation.",
          race_type: "office",
          is_partisan: true,
          sources: ["https://example.gov/elections/governor"],
        },
      ],
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries[0].is_partisan).toBe(true);
    }
  });

  it("rejects invalid entries row in AI payload", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-11-03",
          impact: "Leads the state executive branch and signs or vetoes legislation.",
          race_type: "bad_value",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
    });
    expect(result.ok).toBe(false);
  });

  it("rejects invalid election_stage", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-11-03",
          impact: "Leads the state executive branch and signs or vetoes legislation.",
          race_type: "office",
          election_stage: "invalid",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
    });
    expect(result.ok).toBe(false);
  });

  it("rejects non-boolean is_partisan", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-11-03",
          impact: "Leads the state executive branch and signs or vetoes legislation.",
          race_type: "office",
          is_partisan: "yes",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
    });
    expect(result.ok).toBe(false);
  });

  it("coerces ballot_measure is_partisan=true to false", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "Measure A",
          election_date: "2026-11-03",
          impact: "Raises sales tax for transportation projects.",
          race_type: "ballot_measure",
          is_partisan: true,
          sources: ["https://example.gov/elections/measure-a"],
        },
      ],
    });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries[0].is_partisan).toBe(false);
    }
  });

  it("rejects AI payloads that use description instead of impact", () => {
    const result = parseAiElectionEntriesPayload({
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2026-11-03",
          description: "General election for governor.",
          race_type: "office",
          sources: ["https://example.gov/elections/governor"],
        },
      ],
    });
    expect(result.ok).toBe(false);
  });
});
