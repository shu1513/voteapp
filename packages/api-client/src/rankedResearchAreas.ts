import type { ResearchAreaPreference, ResearchAreaPreferenceDirection, ResearchAreaPreferenceInput } from "./types";

// The issue editor's row model (web ResearchAreaPicker, mobile
// research-areas screen) and its two conversions: saved preferences → rows,
// rows → PUT body. List position is the rank (first = rank 1), so rows carry
// no rank of their own.

export type RankedResearchArea = {
  research_area_id: string;
  direction: ResearchAreaPreferenceDirection;
  hard_veto: boolean;
};

// An ethics record is a strike whatever the user "supports", so the editors
// give this area no direction control and read its veto direction-neutrally
// ("skip anyone with an ethics record") — mirrors
// candidateRecordResearchAreaPolicy: stance is forbidden on this area, its
// tags only mark that a record exists.
export const INTEGRITY_ETHICS_SLUG = "integrity_and_ethics";

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
