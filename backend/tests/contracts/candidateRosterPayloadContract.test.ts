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
          state_filing_ids: ["CA-123"],
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
        state_filing_ids: ["CA-123"],
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

  it("requires fec_ids for federal roster mode when configured", () => {
    const parsed = parseCandidateRosterPayload(
      {
        candidates: [
          {
            display_name: "Jane Doe",
            sources: ["https://example.org/a"],
          },
        ],
      },
      { requireFecIds: true, allowFecIds: true }
    );

    expect(parsed.ok).toBe(false);
  });

  it("accepts fec_ids for federal roster mode", () => {
    const parsed = parseCandidateRosterPayload(
      {
        candidates: [
          {
            display_name: "Jane Doe",
            fec_ids: ["H0XX00000"],
            sources: ["https://example.org/a"],
          },
        ],
      },
      { requireFecIds: true, allowFecIds: true }
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.candidates[0]?.fec_ids).toEqual(["H0XX00000"]);
  });

  it("rejects fec_ids when allowFecIds is disabled (state-level mode)", () => {
    const parsed = parseCandidateRosterPayload(
      {
        candidates: [
          {
            display_name: "Jane Doe",
            fec_ids: ["H0XX00000"],
            sources: ["https://example.org/a"],
          },
        ],
      },
      { allowFecIds: false }
    );

    expect(parsed.ok).toBe(false);
  });

  it("accepts optional state_filing_ids when present", () => {
    const parsed = parseCandidateRosterPayload({
      candidates: [
        {
          display_name: "Jane Doe",
          state_filing_ids: ["CA-777", "CA-777"],
          sources: ["https://example.org/a"],
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.candidates[0]?.state_filing_ids).toEqual(["CA-777"]);
  });
});
