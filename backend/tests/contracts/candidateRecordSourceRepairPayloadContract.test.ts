import { describe, expect, it } from "vitest";

import { parseCandidateRecordSourceRepairPayload } from "../../src/contracts/candidateRecordSourceRepairPayloadContract.js";

describe("parseCandidateRecordSourceRepairPayload", () => {
  it("parses replacement and no_replacement rows", () => {
    const parsed = parseCandidateRecordSourceRepairPayload(
      {
        repairs: [
          {
            bad_index: 0,
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

  it("rejects a future event_date so repairs cannot re-admit rows discovery rejected", () => {
    const future = new Date();
    future.setUTCDate(future.getUTCDate() + 10);
    const parsed = parseCandidateRecordSourceRepairPayload(
      {
        repairs: [
          {
            bad_index: 0,
            description: "Repaired with the same invented future date",
            source_url: "https://example.org/a",
            event_date: future.toISOString().slice(0, 10),
          },
        ],
      },
      { badRecordCount: 1 }
    );

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("payload.repairs[].event_date");
      expect(parsed.reason).toContain("is in the future");
    }
  });

  // Same shared-helper regression as discovery: partial dates either shift a
  // day back in timezones behind UTC or invent day 01, so the repair path
  // must reject every spelling too instead of re-admitting a guessed date.
  it("rejects partial event_date spellings instead of shifting or inventing a day", () => {
    for (const eventDate of ["2025", "2025-04", "2025-4", "2025/04", "April 2025"]) {
      const parsed = parseCandidateRecordSourceRepairPayload(
        {
          repairs: [
            {
              bad_index: 0,
              description: "Repaired with only a partial date",
              source_url: "https://example.org/a",
              event_date: eventDate,
            },
          ],
        },
        { badRecordCount: 1 }
      );

      expect(parsed.ok).toBe(false);
      if (!parsed.ok) {
        expect(parsed.reason).toContain(`payload.repairs[].event_date "${eventDate}" is incomplete`);
        expect(parsed.reason).toContain("publication date");
      }
    }
  });

  it("rejects an impossible calendar date", () => {
    const parsed = parseCandidateRecordSourceRepairPayload(
      {
        repairs: [
          {
            bad_index: 0,
            description: "Description A",
            source_url: "https://example.org/a",
            event_date: "2026-02-31",
          },
        ],
      },
      { badRecordCount: 1 }
    );

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("is not a real calendar date");
    }
  });
});
