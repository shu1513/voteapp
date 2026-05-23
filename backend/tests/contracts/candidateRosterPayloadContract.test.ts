import { describe, expect, it } from "vitest";

import { parseCandidateRosterPayload } from "../../src/contracts/candidateRosterPayloadContract.js";

describe("parseCandidateRosterPayload", () => {
  it("parses valid candidate roster payload", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Jane Doe",
          party: "Independent",
          is_incumbent: true,
          sources: ["https://example.org/a", "https://example.org/a"],
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.candidates).toEqual([
      {
        display_name: "Jane Doe",
        party: "Independent",
        is_incumbent: true,
        sources: ["https://example.org/a"],
      },
    ]);
  });

  it("rejects candidate row without sources", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [{ display_name: "Jane Doe", sources: [] }],
    });

    expect(parsed.ok).toBe(false);
  });

  it("preserves duplicate candidate display names within one payload", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Jane Doe",
          party: "Independent",
          sources: ["https://example.org/a"],
        },
        {
          display_name: "  JANE   DOE ",
          party: "Party X",
          sources: ["https://example.org/b"],
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.candidates).toEqual([
      {
        display_name: "Jane Doe",
        party: "Independent",
        sources: ["https://example.org/a"],
      },
      {
        display_name: "JANE   DOE",
        party: "Party X",
        sources: ["https://example.org/b"],
      },
    ]);
  });
});
