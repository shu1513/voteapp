import { describe, expect, it } from "vitest";

import { parseCandidateRecordSourceRepairPayload } from "../../src/contracts/candidateRecordSourceRepairPayloadContract.js";

describe("parseCandidateRecordSourceRepairPayload", () => {
  it("parses replacement and no_replacement rows", () => {
    const parsed = parseCandidateRecordSourceRepairPayload(
      {
        repairs: [
          {
            bad_index: 0,
            title: "Record A",
            description: "Description A",
            source_url: "https://example.org/a",
            event_date: "2026-01-01",
          },
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
      {
        bad_index: 0,
        title: "Record A",
        description: "Description A",
        source_url: "https://example.org/a",
        event_date: "2026-01-01",
      },
    ]);
    expect(parsed.payload.no_replacement_indexes).toEqual([1]);
  });

  it("rejects duplicate bad_index rows", () => {
    const parsed = parseCandidateRecordSourceRepairPayload(
      {
        repairs: [
          {
            bad_index: 0,
            title: "Record A",
            description: "Description A",
            source_url: "https://example.org/a",
            event_date: "2026-01-01",
          },
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
        repairs: [
          {
            bad_index: 2,
            title: "Record A",
            description: "Description A",
            source_url: "https://example.org/a",
            event_date: "2026-01-01",
          },
        ],
      },
      { badRecordCount: 2 }
    );

    expect(parsed.ok).toBe(false);
  });
});
