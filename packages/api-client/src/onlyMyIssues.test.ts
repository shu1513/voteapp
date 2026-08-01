import { describe, expect, it } from "vitest";

import { deriveOnlyMyIssues } from "./onlyMyIssues";
import type { ElectionSummary } from "./types";

// Only the fields the derivation reads; the rest of ElectionSummary is
// irrelevant to the intersection.
function election(id: string, areaIds: string[]): ElectionSummary {
  return {
    id,
    research_areas: areaIds.map((areaId) => ({
      id: areaId,
      slug: areaId,
      name: areaId,
      description: null,
    })),
  } as ElectionSummary;
}

const SAVED = new Set(["a-1"]);
const MATCHED = election("e-1", ["a-1", "a-2"]);
const UNMATCHED = election("e-2", ["a-3"]);
const NO_AREAS = election("e-3", []);

describe("deriveOnlyMyIssues", () => {
  it("keeps elections whose research areas intersect the saved areas", () => {
    const view = deriveOnlyMyIssues({
      elections: [MATCHED, UNMATCHED, NO_AREAS],
      savedAreaIds: SAVED,
      hasSaved: true,
      requested: true,
    });
    expect(view.filterOn).toBe(true);
    expect(view.visibleElections).toEqual([MATCHED]);
    expect(view.hiddenCount).toBe(2);
    expect(view.showFilter).toBe(true);
  });

  it("shows the off toggle only when the list splits into matched and unmatched", () => {
    const off = (elections: ElectionSummary[]) =>
      deriveOnlyMyIssues({ elections, savedAreaIds: SAVED, hasSaved: true, requested: false });
    expect(off([MATCHED, UNMATCHED]).showFilter).toBe(true);
    // All-match would be a no-op; none-match would empty the list.
    expect(off([MATCHED]).showFilter).toBe(false);
    expect(off([UNMATCHED]).showFilter).toBe(false);
    expect(off([]).showFilter).toBe(false);
    // Off never hides anything.
    expect(off([MATCHED, UNMATCHED]).visibleElections).toEqual([MATCHED, UNMATCHED]);
  });

  it("keeps an active filter visible and applied even when it empties the view", () => {
    const view = deriveOnlyMyIssues({
      elections: [UNMATCHED, NO_AREAS],
      savedAreaIds: SAVED,
      hasSaved: true,
      requested: true,
    });
    expect(view.filterOn).toBe(true);
    expect(view.showFilter).toBe(true);
    expect(view.visibleElections).toEqual([]);
    expect(view.hiddenCount).toBe(2);
  });

  it("ignores the request for a viewer with no saved areas", () => {
    const view = deriveOnlyMyIssues({
      elections: [MATCHED, UNMATCHED],
      savedAreaIds: new Set<string>(),
      hasSaved: false,
      requested: true,
    });
    expect(view.filterOn).toBe(false);
    expect(view.showFilter).toBe(false);
    expect(view.visibleElections).toEqual([MATCHED, UNMATCHED]);
  });
});
