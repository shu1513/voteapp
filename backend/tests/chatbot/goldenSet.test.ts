import { describe, expect, it } from "vitest";

import { goldenSet, type GoldenCase } from "../../src/chatbot/golden/goldenSet.js";

// Structural invariants only (Phase 0). The retrieval eval that consumes
// expectedSourceTypes/expectedEntity arrives with Phase 1 infrastructure —
// see backend/src/chatbot/BEHAVIOR.md for the release gates.

describe("chatbot golden set", () => {
  it("has at least 50 cases", () => {
    expect(goldenSet.length).toBeGreaterThanOrEqual(50);
  });

  it("has unique, stable ids", () => {
    const ids = goldenSet.map((c) => c.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const id of ids) {
      expect(id).toMatch(/^[a-z0-9]+(-[a-z0-9]+)+$/);
    }
  });

  it("has non-empty questions", () => {
    for (const c of goldenSet) {
      expect(c.question.trim().length, c.id).toBeGreaterThan(0);
    }
  });

  it("covers every required category with minimum counts", () => {
    const byCategory = new Map<string, GoldenCase[]>();
    for (const c of goldenSet) {
      const list = byCategory.get(c.category) ?? [];
      list.push(c);
      byCategory.set(c.category, list);
    }
    const minimums: Record<string, number> = {
      profile: 5,
      election: 5,
      finance: 4,
      records: 3,
      ballot_measure: 3,
      logistics: 5,
      results: 1,
      policy: 5,
      ambiguous: 3,
      out_of_scope: 5,
      adversarial: 5,
      followup: 2,
      smalltalk: 3,
    };
    for (const [category, min] of Object.entries(minimums)) {
      expect(byCategory.get(category)?.length ?? 0, category).toBeGreaterThanOrEqual(min);
    }
  });

  it("gives every retrieval case expected source types and entities", () => {
    for (const c of goldenSet) {
      if (c.expected !== "retrieval") continue;
      expect(c.expectedSourceTypes?.length, c.id).toBeGreaterThan(0);
      expect(c.expectedEntities?.length, c.id).toBeGreaterThan(0);
      for (const entity of c.expectedEntities ?? []) {
        expect(entity.trim().length, c.id).toBeGreaterThan(0);
      }
    }
  });

  it("requires both sides of a comparison question", () => {
    for (const c of goldenSet) {
      if (!/\bcompare\b|\bmore money\b/i.test(c.question)) continue;
      expect(c.expectedEntities?.length, c.id).toBeGreaterThanOrEqual(2);
    }
  });

  it("gives non-retrieval cases no retrieval-only expectations", () => {
    for (const c of goldenSet) {
      if (c.expected === "retrieval") continue;
      expect(c.expectedSourceTypes, c.id).toBeUndefined();
      expect(c.expectedEntities, c.id).toBeUndefined();
    }
  });

  it("gives every followup case a previous question, and only those", () => {
    for (const c of goldenSet) {
      if (c.category === "followup") {
        expect(c.previousQuestion?.trim().length, c.id).toBeGreaterThan(0);
      } else {
        expect(c.previousQuestion, c.id).toBeUndefined();
      }
    }
  });

  it("keeps policy refusals as refuse_policy (no endorsements, ever)", () => {
    for (const c of goldenSet) {
      if (c.category === "policy") {
        expect(c.expected, c.id).toBe("refuse_policy");
      }
    }
  });

  it("keeps ambiguous cases as clarify and out-of-scope cases as refusals", () => {
    for (const c of goldenSet) {
      if (c.category === "ambiguous") expect(c.expected, c.id).toBe("clarify");
      if (c.category === "out_of_scope") expect(c.expected, c.id).toBe("refuse_no_data");
    }
  });

  it("keeps logistics, results, and smalltalk deterministic (template only)", () => {
    for (const c of goldenSet) {
      if (c.category === "logistics" || c.category === "results" || c.category === "smalltalk") {
        expect(c.expected, c.id).toBe("template");
      }
    }
  });
});
