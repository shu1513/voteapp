import type { ResearchAreaPreference, ResearchAreaPreferenceDirection, ResearchAreaPreferenceInput } from "@voteapp/api-client";

// The issue editor's row model (ResearchAreaPicker) and its two conversions:
// saved preferences → rows, rows → PUT body. List position is the rank
// (first = rank 1), so rows carry no rank of their own.

export type RankedResearchArea = {
  research_area_id: string;
  direction: ResearchAreaPreferenceDirection;
  hard_veto: boolean;
};

export function newRankedResearchArea(researchAreaId: string): RankedResearchArea {
  return { research_area_id: researchAreaId, direction: "support", hard_veto: false };
}

export function toRankedResearchAreas(preferences: readonly ResearchAreaPreference[]): RankedResearchArea[] {
  return preferences.map((preference) => ({
    research_area_id: preference.research_area_id,
    direction: preference.direction,
    hard_veto: preference.hard_veto,
  }));
}

export function toPreferenceInputs(ranked: readonly RankedResearchArea[]): ResearchAreaPreferenceInput[] {
  return ranked.map((row, index) => ({
    research_area_id: row.research_area_id,
    rank: index + 1,
    direction: row.direction,
    hard_veto: row.hard_veto,
  }));
}
