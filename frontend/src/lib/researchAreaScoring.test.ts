import { describe, expect, it } from "vitest";

import type { CandidateRecord } from "../api/types";
import {
  aggregateRecordAreaStances,
  buildResearchAreaWeights,
  researchAreaWeightForRank,
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
  it("matches the backend formula: 8 - rank, unranked = 1", () => {
    expect(researchAreaWeightForRank(1)).toBe(7);
    expect(researchAreaWeightForRank(7)).toBe(1);
    expect(researchAreaWeightForRank(null)).toBe(1);
  });
});

describe("buildResearchAreaWeights", () => {
  it("maps preferences to weights with the unranked sentinel", () => {
    const weights = buildResearchAreaWeights([
      { research_area_id: AREA_HOUSING, slug: "housing", name: "Housing", description: null, rank: 2 },
      { research_area_id: AREA_SAFETY, slug: "safety", name: "Safety", description: null, rank: null },
    ]);

    expect(weights.get(AREA_HOUSING)).toEqual({ weight: 6, rank: 2 });
    expect(weights.get(AREA_SAFETY)).toEqual({ weight: 1, rank: UNRANKED_RESEARCH_AREA_RANK });
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
