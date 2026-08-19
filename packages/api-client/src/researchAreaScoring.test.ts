import { describe, expect, it } from "vitest";

import type { CandidateRecord } from "./types";
import {
  aggregateRecordAreaStances,
  buildResearchAreaWeights,
  researchAreaWeightForRank,
  scoreStanceRelevance,
  UNRANKED_RESEARCH_AREA_RANK,
} from "./researchAreaScoring";

const AREA_HOUSING = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const AREA_SAFETY = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const AREA_ETHICS = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";

function record(id: string, tags: Array<{ area: string; slug: string; stance: "for" | "against" | null }>): CandidateRecord {
  return {
    id,
    description: "record",
    source_url: "https://example.test",
    event_date: "2026-01-01",
    created_at: "2026-01-02T00:00:00Z",
    research_area_tags: tags.map((tag) => ({
      research_area_id: tag.area,
      slug: tag.slug,
      name: tag.slug,
      stance: tag.stance,
    })),
  };
}

describe("researchAreaWeightForRank", () => {
  // Mirror of backend userResearchAreaScoring.ts — same numbers or the
  // client-side candidate/record sorts disagree with the server ballot sort.
  it("matches the backend formula: 0.75^(rank - 1)", () => {
    expect(researchAreaWeightForRank(1)).toBe(1);
    expect(researchAreaWeightForRank(2)).toBe(0.75);
    expect(researchAreaWeightForRank(3)).toBe(0.5625);
    expect(researchAreaWeightForRank(7)).toBe(0.75 ** 6);
  });

  it("stays positive and strictly decreasing with no rank ceiling", () => {
    for (let rank = 1; rank < 30; rank += 1) {
      expect(researchAreaWeightForRank(rank)).toBeGreaterThan(0);
      expect(researchAreaWeightForRank(rank)).toBeGreaterThan(researchAreaWeightForRank(rank + 1));
    }
  });
});

describe("buildResearchAreaWeights", () => {
  it("maps preferences to weights; an unranked save weighs one rank below the highest explicit rank", () => {
    const weights = buildResearchAreaWeights([
      { research_area_id: AREA_HOUSING, slug: "housing", name: "Housing", description: null, rank: 2, direction: "support", hard_veto: false },
      { research_area_id: AREA_SAFETY, slug: "safety", name: "Safety", description: null, rank: null, direction: "support", hard_veto: false },
    ]);

    expect(weights.get(AREA_HOUSING)).toEqual({ weight: 0.75, rank: 2 });
    // Highest explicit rank is 2 (non-contiguous: nothing at rank 1) → the
    // unranked one weighs as rank 3, strictly below every ranked area.
    expect(weights.get(AREA_SAFETY)).toEqual({ weight: 0.5625, rank: UNRANKED_RESEARCH_AREA_RANK });
  });
});

describe("aggregateRecordAreaStances", () => {
  it("counts for/against per area across records and sorts by slug", () => {
    const stances = aggregateRecordAreaStances([
      record("r1", [
        { area: AREA_HOUSING, slug: "housing_affordability", stance: "for" },
        { area: AREA_SAFETY, slug: "public_safety", stance: "against" },
      ]),
      record("r2", [{ area: AREA_HOUSING, slug: "housing_affordability", stance: "for" }]),
      record("r3", [{ area: AREA_HOUSING, slug: "housing_affordability", stance: "against" }]),
    ]);

    expect(stances).toEqual([
      {
        research_area_id: AREA_HOUSING,
        slug: "housing_affordability",
        name: "housing_affordability",
        for_count: 2,
        against_count: 1,
      },
      {
        research_area_id: AREA_SAFETY,
        slug: "public_safety",
        name: "public_safety",
        for_count: 0,
        against_count: 1,
      },
    ]);
  });

  it("excludes general (null-stance) tags entirely", () => {
    const stances = aggregateRecordAreaStances([
      record("r1", [
        { area: AREA_ETHICS, slug: "ethics", stance: null },
        { area: AREA_HOUSING, slug: "housing_affordability", stance: "for" },
      ]),
    ]);

    expect(stances.map((s) => s.slug)).toEqual(["housing_affordability"]);
  });

  it("returns an empty list for candidates with no stance-bearing tags", () => {
    expect(aggregateRecordAreaStances([])).toEqual([]);
    expect(aggregateRecordAreaStances([record("r1", [{ area: AREA_ETHICS, slug: "ethics", stance: null }])])).toEqual([]);
  });
});

describe("scoreStanceRelevance", () => {
  const weights = buildResearchAreaWeights([
    { research_area_id: AREA_HOUSING, slug: "housing", name: "Housing", description: null, rank: 1, direction: "support", hard_veto: false }, // weight 1
    { research_area_id: AREA_SAFETY, slug: "safety", name: "Safety", description: null, rank: 3, direction: "support", hard_veto: false }, // weight 0.5625
  ]);

  const stances = aggregateRecordAreaStances([
    record("r1", [{ area: AREA_HOUSING, slug: "housing", stance: "for" }]),
    record("r2", [{ area: AREA_HOUSING, slug: "housing", stance: "for" }]),
    record("r3", [{ area: AREA_SAFETY, slug: "safety", stance: "against" }]),
    record("r4", [{ area: AREA_ETHICS, slug: "ethics", stance: "for" }]), // not saved
  ]);

  it("sums saved-area weights across both directions with record counts as volume", () => {
    // Housing (weight 1, 2 for-records) + Safety (weight 0.5625, 1
    // against-record): both areas count once each regardless of direction.
    expect(scoreStanceRelevance(stances, weights)).toEqual({ score: 1.5625, recordCount: 3 });
  });

  it("scores against-only candidates above no-record candidates", () => {
    // The "My issues first" label is direction-neutral: a candidate with only
    // against-records on a saved issue has a track record on it and must not
    // tie with a candidate who has none.
    const againstOnly = aggregateRecordAreaStances([
      record("r1", [{ area: AREA_HOUSING, slug: "housing", stance: "against" }]),
    ]);
    expect(scoreStanceRelevance(againstOnly, weights)).toEqual({ score: 1, recordCount: 1 });
    expect(scoreStanceRelevance([], weights)).toEqual({ score: 0, recordCount: 0 });
  });

  it("ignores unsaved areas and scores zero with no matches", () => {
    const unsavedOnly = aggregateRecordAreaStances([
      record("r1", [{ area: AREA_ETHICS, slug: "ethics", stance: "for" }]),
    ]);
    expect(scoreStanceRelevance(unsavedOnly, weights)).toEqual({ score: 0, recordCount: 0 });
  });
});
