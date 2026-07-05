import { describe, expect, it, vi } from "vitest";

import {
  loadUserResearchAreaWeights,
  NO_MATCH_BEST_RANK,
  researchAreaWeightForRank,
  scoreResearchAreaMatch,
  UNRANKED_RESEARCH_AREA_RANK,
} from "../../../src/pipeline/users/userResearchAreaScoring.js";

const USER_ID = "99999999-9999-4999-8999-999999999999";
const AREA_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const AREA_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const AREA_C = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";

describe("researchAreaWeightForRank", () => {
  it("maps rank 1 to the heaviest weight and rank 7 to the lightest", () => {
    expect(researchAreaWeightForRank(1)).toBe(7);
    expect(researchAreaWeightForRank(7)).toBe(1);
  });

  it("weighs a selected-but-unranked area like rank 7", () => {
    expect(researchAreaWeightForRank(null)).toBe(1);
  });
});

describe("loadUserResearchAreaWeights", () => {
  it("returns an empty map without querying for anonymous or malformed user ids", async () => {
    const query = vi.fn();

    await expect(loadUserResearchAreaWeights({ query }, null)).resolves.toEqual(new Map());
    await expect(loadUserResearchAreaWeights({ query }, undefined)).resolves.toEqual(new Map());
    await expect(loadUserResearchAreaWeights({ query }, "not-a-uuid")).resolves.toEqual(new Map());
    expect(query).not.toHaveBeenCalled();
  });

  it("maps saved preferences to weights, tolerating string ranks from pg", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        { research_area_id: AREA_A, rank: 1 },
        { research_area_id: AREA_B, rank: "3" },
        { research_area_id: AREA_C, rank: null },
      ],
    });

    const weights = await loadUserResearchAreaWeights({ query }, USER_ID);

    expect(weights.get(AREA_A)).toEqual({ weight: 7, rank: 1 });
    expect(weights.get(AREA_B)).toEqual({ weight: 5, rank: 3 });
    expect(weights.get(AREA_C)).toEqual({ weight: 1, rank: UNRANKED_RESEARCH_AREA_RANK });
    // Deleted users must not keep personalizing anything.
    expect(String(query.mock.calls[0]?.[0])).toContain("deleted_at IS NULL");
  });
});

describe("scoreResearchAreaMatch", () => {
  const weights = new Map([
    [AREA_A, { weight: 7, rank: 1 }],
    [AREA_B, { weight: 5, rank: 3 }],
  ]);

  it("sums matched weights and reports the best matched rank", () => {
    expect(scoreResearchAreaMatch([AREA_A, AREA_B, AREA_C], weights)).toEqual({
      score: 12,
      bestRank: 1,
    });
    expect(scoreResearchAreaMatch([AREA_B], weights)).toEqual({ score: 5, bestRank: 3 });
  });

  it("counts duplicate area ids once", () => {
    expect(scoreResearchAreaMatch([AREA_B, AREA_B, AREA_B], weights)).toEqual({
      score: 5,
      bestRank: 3,
    });
  });

  it("reports zero score and the no-match sentinel when nothing matches", () => {
    expect(scoreResearchAreaMatch([AREA_C], weights)).toEqual({
      score: 0,
      bestRank: NO_MATCH_BEST_RANK,
    });
    expect(scoreResearchAreaMatch([], weights)).toEqual({ score: 0, bestRank: NO_MATCH_BEST_RANK });
  });
});
