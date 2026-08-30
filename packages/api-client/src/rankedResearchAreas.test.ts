import { describe, expect, it } from "vitest";
import { newRankedResearchArea, toPreferenceInputs, toRankedResearchAreas } from "./rankedResearchAreas";
import type { ResearchAreaPreference } from "./types";

const PREFERENCE: ResearchAreaPreference = {
  research_area_id: "11111111-1111-4111-8111-111111111111",
  slug: "housing",
  name: "Housing",
  description: null,
  rank: 1,
  direction: "oppose",
  hard_veto: true,
};

describe("rankedResearchAreas", () => {
  it("defaults a new row to support with no veto", () => {
    expect(newRankedResearchArea("id-1")).toEqual({
      research_area_id: "id-1",
      direction: "support",
      hard_veto: false,
    });
  });

  it("keeps direction and hard_veto through rows → inputs", () => {
    const rows = toRankedResearchAreas([PREFERENCE]);
    expect(rows).toEqual([
      { research_area_id: PREFERENCE.research_area_id, direction: "oppose", hard_veto: true },
    ]);
    // List position is the rank: first = rank 1.
    expect(toPreferenceInputs([...rows, newRankedResearchArea("id-2")])).toEqual([
      { research_area_id: PREFERENCE.research_area_id, rank: 1, direction: "oppose", hard_veto: true },
      { research_area_id: "id-2", rank: 2, direction: "support", hard_veto: false },
    ]);
  });
});
