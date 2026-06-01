import { describe, expect, it } from "vitest";

import { parseCandidateRecordSourceRepairPayload } from "../../src/contracts/candidateRecordSourceRepairPayloadContract.js";

describe("parseCandidateRecordSourceRepairPayload", () => {
  it("parses replacement and no_replacement rows", () => {
    const parsed = parseCandidateRecordSourceRepairPayload(
      {
        repairs: [
          { bad_index: 0, source_url: "https://example.org/a", source_name: "Example" },
          { bad_index: 1, no_replacement: true, reason: "no reliable source" },
        ],
      },
      { badRecordCount: 2 }
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }

    expect(parsed.payload.repairs).toEqual([
      { bad_index: 0, source_url: "https://example.org/a", source_name: "Example" },
    ]);
    expect(parsed.payload.no_replacement_indexes).toEqual([1]);
  });

  it("rejects duplicate bad_index rows", () => {
    const parsed = parseCandidateRecordSourceRepairPayload(
      {
        repairs: [
          { bad_index: 0, source_url: "https://example.org/a", source_name: "Example" },
          { bad_index: 0, no_replacement: true },
        ],
      },
      { badRecordCount: 2 }
    );

    expect(parsed.ok).toBe(false);
  });

  it("rejects out-of-range bad_index", () => {
    const parsed = parseCandidateRecordSourceRepairPayload(
      {
        repairs: [{ bad_index: 2, source_url: "https://example.org/a", source_name: "Example" }],
      },
      { badRecordCount: 2 }
    );

    expect(parsed.ok).toBe(false);
  });
});
