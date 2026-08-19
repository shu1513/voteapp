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
  it("decays geometrically from rank 1 = 1: 0.75^(rank - 1)", () => {
    expect(researchAreaWeightForRank(1)).toBe(1);
    expect(researchAreaWeightForRank(2)).toBe(0.75);
    expect(researchAreaWeightForRank(3)).toBe(0.5625);
    expect(researchAreaWeightForRank(7)).toBe(0.75 ** 6);
  });

  it("stays positive and strictly decreasing with no rank ceiling", () => {
    // The mirror in packages/api-client/src/researchAreaScoring.ts asserts
    // the same values; the two must agree or client sorts disagree with the
    // server ballot sort.
    for (let rank = 1; rank < 30; rank += 1) {
      const weight = researchAreaWeightForRank(rank);
      expect(weight).toBeGreaterThan(0);
      expect(weight).toBeGreaterThan(researchAreaWeightForRank(rank + 1));
    }
  });

  it("orders unranked before no-match in the tiebreak sentinels", () => {
    expect(UNRANKED_RESEARCH_AREA_RANK).toBeLessThan(NO_MATCH_BEST_RANK);
    expect(UNRANKED_RESEARCH_AREA_RANK).toBeGreaterThan(1_000_000);
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

    expect(weights.get(AREA_A)).toEqual({ weight: 1, rank: 1 });
    expect(weights.get(AREA_B)).toEqual({ weight: 0.5625, rank: 3 });
    // Highest explicit rank is 3 → the unranked one weighs as rank 4 (never
    // as much as a ranked area) but keeps the unranked sentinel for tiebreaks.
    expect(weights.get(AREA_C)).toEqual({ weight: 0.75 ** 3, rank: UNRANKED_RESEARCH_AREA_RANK });
    // Deleted users must not keep personalizing anything.
    expect(String(query.mock.calls[0]?.[0])).toContain("deleted_at IS NULL");
  });
});

describe("scoreResearchAreaMatch", () => {
  const weights = new Map([
    [AREA_A, { weight: 1, rank: 1 }],
    [AREA_B, { weight: 0.5625, rank: 3 }],
  ]);

  it("sums matched weights and reports the best matched rank", () => {
    expect(scoreResearchAreaMatch([AREA_A, AREA_B, AREA_C], weights)).toEqual({
      score: 1.5625,
      bestRank: 1,
    });
    expect(scoreResearchAreaMatch([AREA_B], weights)).toEqual({ score: 0.5625, bestRank: 3 });
  });

  it("counts duplicate area ids once", () => {
    expect(scoreResearchAreaMatch([AREA_B, AREA_B, AREA_B], weights)).toEqual({
      score: 0.5625,
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
