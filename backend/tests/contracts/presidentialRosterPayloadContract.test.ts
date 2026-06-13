import { describe, expect, it } from "vitest";

import { parsePresidentialRosterPayload } from "../../src/contracts/presidentialRosterPayloadContract.js";

describe("parsePresidentialRosterPayload", () => {
  it("parses and normalizes valid presidential roster candidates", () => {
    const parsed = parsePresidentialRosterPayload(
      {
        candidates: [
          {
            display_name: " Jane President ",
            party: "Democrat",
            fec_candidate_id: " p80000001 ",
            sources: ["https://example.org/a", "https://example.org/a"],
            status: "ACTIVE",
          },
          {
            display_name: "Pat Suspended",
            party: "Democratic",
            sources: ["https://example.org/b"],
            status: "withdrawn",
          },
        ],
      },
      { expectedParty: "Democratic" }
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.candidates).toEqual([
      {
        display_name: "Jane President",
        party: "Democrat",
        fec_candidate_id: "P80000001",
        sources: ["https://example.org/a"],
        status: "active",
      },
      {
        display_name: "Pat Suspended",
        party: "Democratic",
        sources: ["https://example.org/b"],
        status: "withdrawn",
      },
    ]);
  });

  it("rejects wrong-party candidates when expected party is configured", () => {
    const parsed = parsePresidentialRosterPayload(
      {
        candidates: [
          {
            display_name: "Wrong Primary",
            party: "Republican",
            sources: ["https://example.org/a"],
            status: "active",
          },
        ],
      },
      { expectedParty: "Democratic" }
    );

    expect(parsed.ok).toBe(false);
    expect(parsed.ok ? "" : parsed.reason).toContain("does not match expected party Democratic");
  });

  it("accepts Republican aliases for Republican primary parsing", () => {
    const parsed = parsePresidentialRosterPayload(
      {
        candidates: [
          {
            display_name: "Jane GOP",
            party: "GOP",
            sources: ["https://example.org/a"],
            status: "active",
          },
        ],
      },
      { expectedParty: "Republican" }
    );

    expect(parsed.ok).toBe(true);
  });

  it("rejects invalid rows", () => {
    expect(
      parsePresidentialRosterPayload({
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            sources: [],
            status: "active",
          },
        ],
      }).ok
    ).toBe(false);

    expect(
      parsePresidentialRosterPayload({
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            sources: ["https://example.org/a"],
            status: "maybe",
          },
        ],
      }).ok
    ).toBe(false);

    expect(
      parsePresidentialRosterPayload({
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "H0CA00001",
            sources: ["https://example.org/a"],
            status: "active",
          },
        ],
      }).ok
    ).toBe(false);
  });

  it("rejects non-object payloads and missing candidates array", () => {
    expect(parsePresidentialRosterPayload(null).ok).toBe(false);
    expect(parsePresidentialRosterPayload({}).ok).toBe(false);
  });
});
