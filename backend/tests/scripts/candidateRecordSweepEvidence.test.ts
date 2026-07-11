import { describe, expect, it } from "vitest";

import {
  SWEEP_COMPLETENESS_GAP_IDS,
  SWEEP_EVIDENCE_MIN_ENTRIES,
  assertedSweepCompletenessGapIds,
  parseSweepEvidencePayload,
  sweepEvidenceRequired,
  upsertSweepConfirmation,
} from "../../src/scripts/candidateRecordSweepEvidence.js";
import { buildCandidateRecordQualityGaps } from "../../src/scripts/writeManualCandidateRecords.js";

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

  it("rejects duplicate questions (same text repeated to pad the minimum)", () => {
    const result = parseSweepEvidencePayload({
      entries: [
        { question: "What endorsements did they receive?", finding: "nothing found" },
        { question: "what endorsements  did they receive?", finding: "nothing found" },
        { question: "Any court or ethics records?", finding: "nothing found" },
      ],
    });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("entries[1].question duplicates entries[0]");
    }
  });

  it("accepts the same question asked for distinct eras when the era is named", () => {
    const result = parseSweepEvidencePayload({
      entries: [
        { question: "Major roll-call votes (2021 session)?", finding: "nothing found" },
        { question: "Major roll-call votes (2023 session)?", finding: "HB 9 veto override" },
        { question: "Any court or ethics records?", finding: "nothing found" },
      ],
    });
    expect(result.ok).toBe(true);
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

describe("neutral-only writes trigger the evidence guard via quality gaps", () => {
  it("raises only_general_labels — a SWEEP_COMPLETENESS_GAP_ID — for an all-neutral label set", () => {
    const gaps = buildCandidateRecordQualityGaps({
      recordCount: 2,
      labels: [
        { research_area_slug: "general" },
        { research_area_slug: "integrity_and_ethics" },
      ],
    });
    expect(gaps.some((gap) => SWEEP_COMPLETENESS_GAP_IDS.has(gap.id))).toBe(true);
  });

  it("raises no_records_found — a SWEEP_COMPLETENESS_GAP_ID — for a zero-record payload", () => {
    const gaps = buildCandidateRecordQualityGaps({ recordCount: 0, labels: [] });
    expect(gaps.some((gap) => SWEEP_COMPLETENESS_GAP_IDS.has(gap.id))).toBe(true);
  });

  it("raises no completeness gap for a stance-bearing record set", () => {
    const gaps = buildCandidateRecordQualityGaps({
      recordCount: 1,
      labels: [{ research_area_slug: "healthcare_affordability" }],
    });
    expect(gaps.some((gap) => SWEEP_COMPLETENESS_GAP_IDS.has(gap.id))).toBe(false);
  });
});

describe("assertedSweepCompletenessGapIds", () => {
  it("implies no_records_found for a zero-record payload without any flag", () => {
    expect(
      assertedSweepCompletenessGapIds({ recordCount: 0, confirmedGapIds: new Set(), qualityGapIds: [] })
    ).toEqual(["candidate_records.no_records_found"]);
  });

  it("collects the only_general_labels claim from either the flag or the detected quality gap", () => {
    expect(
      assertedSweepCompletenessGapIds({
        recordCount: 2,
        confirmedGapIds: new Set(["candidate_records.only_general_labels"]),
        qualityGapIds: [],
      })
    ).toEqual(["candidate_records.only_general_labels"]);
    expect(
      assertedSweepCompletenessGapIds({
        recordCount: 2,
        confirmedGapIds: new Set(),
        qualityGapIds: ["candidate_records.only_general_labels"],
      })
    ).toEqual(["candidate_records.only_general_labels"]);
  });

  it("ignores non-completeness gap ids and dedupes overlapping claims", () => {
    expect(
      assertedSweepCompletenessGapIds({
        recordCount: 0,
        confirmedGapIds: new Set(["candidate_records.no_records_found", "candidate_records.payload"]),
        qualityGapIds: ["candidate_records.no_records_found", "candidate_records.dropped.0"],
      })
    ).toEqual(["candidate_records.no_records_found"]);
  });
});

describe("upsertSweepConfirmation", () => {
  it("writes one upsert row keyed by candidate with the evidence entries as jsonb", async () => {
    const calls: { text: string; values: unknown[] }[] = [];
    const client = {
      query: async (text: string, values?: unknown[]) => {
        calls.push({ text, values: values ?? [] });
        return { rows: [], rowCount: 1 } as never;
      },
    };

    await upsertSweepConfirmation(client as never, {
      candidateId: "candidate-1",
      confirmedGapIds: ["candidate_records.no_records_found"],
      entries: [
        { question: "votes?", finding: "nothing found" },
        { question: "litigation?", finding: "nothing found" },
        { question: "endorsements?", finding: "nothing found" },
      ],
      contextType: "election",
      contextId: "election-1",
    });

    expect(calls).toHaveLength(1);
    expect(calls[0]!.text).toContain("INSERT INTO public.candidate_record_sweep_confirmations");
    expect(calls[0]!.text).toContain("ON CONFLICT (candidate_id)");
    expect(calls[0]!.values[0]).toBe("candidate-1");
    expect(calls[0]!.values[1]).toEqual(["candidate_records.no_records_found"]);
    expect(JSON.parse(calls[0]!.values[2] as string)).toEqual({
      entries: [
        { question: "votes?", finding: "nothing found" },
        { question: "litigation?", finding: "nothing found" },
        { question: "endorsements?", finding: "nothing found" },
      ],
    });
    expect(calls[0]!.values[3]).toBe("election");
    expect(calls[0]!.values[4]).toBe("election-1");
  });
});
