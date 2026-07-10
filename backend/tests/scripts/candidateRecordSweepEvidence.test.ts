import { describe, expect, it } from "vitest";

import {
  SWEEP_EVIDENCE_MIN_ENTRIES,
  parseSweepEvidencePayload,
  sweepEvidenceRequired,
} from "../../src/scripts/candidateRecordSweepEvidence.js";

describe("sweepEvidenceRequired", () => {
  it("requires evidence for a zero-record payload even without any confirmed-gap flag", () => {
    expect(sweepEvidenceRequired({ recordCount: 0, confirmedGapIds: new Set() })).toBe(true);
  });

  it("requires evidence when no_records_found is asserted", () => {
    expect(
      sweepEvidenceRequired({
        recordCount: 0,
        confirmedGapIds: new Set(["candidate_records.no_records_found"]),
      })
    ).toBe(true);
  });

  it("requires evidence when only_general_labels is asserted on a non-empty record set", () => {
    expect(
      sweepEvidenceRequired({
        recordCount: 2,
        confirmedGapIds: new Set(["candidate_records.only_general_labels"]),
      })
    ).toBe(true);
  });

  it("does not require evidence for a normal record write", () => {
    expect(sweepEvidenceRequired({ recordCount: 3, confirmedGapIds: new Set() })).toBe(false);
  });

  it("does not require evidence for unrelated confirmed gaps", () => {
    expect(
      sweepEvidenceRequired({
        recordCount: 1,
        confirmedGapIds: new Set(["candidate_profile.current_office"]),
      })
    ).toBe(false);
  });
});

describe("parseSweepEvidencePayload", () => {
  const validEntries = [
    { question: "What major roll-call votes did they cast?", finding: "nothing found" },
    { question: "What legislation did they sponsor?", finding: "HB 123 signed 2024-05-02" },
    { question: "What endorsements did they make or receive?", finding: "nothing found" },
  ];

  it("accepts a well-formed evidence table", () => {
    const result = parseSweepEvidencePayload({ entries: validEntries });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.entries).toHaveLength(3);
      expect(result.entries[0].question).toContain("roll-call");
    }
  });

  it("rejects a bare array", () => {
    const result = parseSweepEvidencePayload(validEntries);
    expect(result).toEqual({
      ok: false,
      reason: "evidence payload must be an object with an entries array",
    });
  });

  it("rejects fewer than the minimum entries", () => {
    const result = parseSweepEvidencePayload({ entries: validEntries.slice(0, 1) });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain(`at least ${SWEEP_EVIDENCE_MIN_ENTRIES}`);
    }
  });

  it("rejects an entry with an empty finding", () => {
    const result = parseSweepEvidencePayload({
      entries: [...validEntries.slice(0, 2), { question: "Any court or ethics records?", finding: "  " }],
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("entries[2].finding");
    }
  });

  it("rejects an entry missing a question", () => {
    const result = parseSweepEvidencePayload({
      entries: [...validEntries.slice(0, 2), { finding: "nothing found" }],
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("entries[2].question");
    }
  });

  it("trims question and finding text", () => {
    const result = parseSweepEvidencePayload({
      entries: validEntries.map((entry) => ({
        question: `  ${entry.question}  `,
        finding: `  ${entry.finding}  `,
      })),
    });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.entries[1].finding).toBe("HB 123 signed 2024-05-02");
    }
  });
});
