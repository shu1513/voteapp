import { describe, expect, it } from "vitest";

import { deriveBallotFilters, LONG_BALLOT_THRESHOLD } from "./ballotFilters";
import type { ElectionSummary, VotePower } from "./types";

// Only the fields the derivation reads; the rest of ElectionSummary is
// irrelevant to the two predicates.
function election(
  id: string,
  areaIds: string[],
  label: VotePower["label"] = "medium"
): ElectionSummary {
  return {
    id,
    research_areas: areaIds.map((areaId) => ({
      id: areaId,
      slug: areaId,
      name: areaId,
      description: null,
    })),
    vote_power: { label },
  } as ElectionSummary;
}

const SAVED = new Set(["a-1"]);
const MATCHED = election("e-1", ["a-1", "a-2"]);
const UNMATCHED = election("e-2", ["a-3"]);
const NO_AREAS = election("e-3", []);

const OFF = { savedAreaIds: SAVED, hasSaved: true, issuesRequested: false, impactRequested: false };

describe("deriveBallotFilters — only my issues", () => {
  it("keeps elections whose research areas intersect the saved areas", () => {
    const view = deriveBallotFilters({
      ...OFF,
      elections: [MATCHED, UNMATCHED, NO_AREAS],
      issuesRequested: true,
    });
    expect(view.issuesOn).toBe(true);
    expect(view.visibleElections).toEqual([MATCHED]);
    expect(view.hiddenCount).toBe(2);
    expect(view.showIssuesFilter).toBe(true);
    expect(view.activeFilterCount).toBe(1);
  });

  it("offers the off toggle only when the list splits into matched and unmatched", () => {
    const off = (elections: ElectionSummary[]) => deriveBallotFilters({ ...OFF, elections });
    expect(off([MATCHED, UNMATCHED]).showIssuesFilter).toBe(true);
    // All-match would be a no-op; none-match would empty the list.
    expect(off([MATCHED]).showIssuesFilter).toBe(false);
    expect(off([UNMATCHED]).showIssuesFilter).toBe(false);
    expect(off([]).showIssuesFilter).toBe(false);
    // Off never hides anything.
    expect(off([MATCHED, UNMATCHED]).visibleElections).toEqual([MATCHED, UNMATCHED]);
    expect(off([MATCHED, UNMATCHED]).activeFilterCount).toBe(0);
  });

  it("keeps an active filter visible and applied even when it empties the view", () => {
    const view = deriveBallotFilters({
      ...OFF,
      elections: [UNMATCHED, NO_AREAS],
      issuesRequested: true,
    });
    expect(view.issuesOn).toBe(true);
    expect(view.showIssuesFilter).toBe(true);
    expect(view.visibleElections).toEqual([]);
    expect(view.hiddenCount).toBe(2);
  });

  it("ignores the request for a viewer with no saved areas", () => {
    const view = deriveBallotFilters({
      elections: [MATCHED, UNMATCHED],
      savedAreaIds: new Set<string>(),
      hasSaved: false,
      issuesRequested: true,
      impactRequested: false,
    });
    expect(view.issuesOn).toBe(false);
    expect(view.showIssuesFilter).toBe(false);
    expect(view.visibleElections).toEqual([MATCHED, UNMATCHED]);
    expect(view.activeFilterCount).toBe(0);
  });
});

describe("deriveBallotFilters — high impact only", () => {
  // A long ballot (> LONG_BALLOT_THRESHOLD) that splits on the label test.
  const HIGH = election("h-1", [], "very_high");
  const longSplit = [
    HIGH,
    election("h-2", [], "high"),
    election("m-1", [], "medium"),
    election("m-2", [], "low"),
    election("m-3", [], "very_low"),
    election("m-4", [], "unknown"),
    election("m-5", [], "medium"),
    election("m-6", [], "low"),
  ];

  it("keeps only high/very_high and counts unknown among the hidden", () => {
    const view = deriveBallotFilters({ ...OFF, elections: longSplit, impactRequested: true });
    expect(view.impactOn).toBe(true);
    expect(view.visibleElections.map((e) => e.id)).toEqual(["h-1", "h-2"]);
    // 6 hidden includes the unknown-label race: the filter claims "high
    // impact" and unknown is not known-high.
    expect(view.hiddenCount).toBe(6);
    expect(view.activeFilterCount).toBe(1);
  });

  it("offers the off toggle only past the long-ballot threshold", () => {
    expect(longSplit.length).toBe(LONG_BALLOT_THRESHOLD + 1);
    expect(deriveBallotFilters({ ...OFF, elections: longSplit }).showImpactFilter).toBe(true);
    // Exactly at the threshold (7): short ballot, no offer even though the
    // list splits.
    const atThreshold = longSplit.slice(0, LONG_BALLOT_THRESHOLD);
    expect(deriveBallotFilters({ ...OFF, elections: atThreshold }).showImpactFilter).toBe(false);
    // Long but no split: all-high is a no-op, none-high would empty the
    // ballot unexplained.
    const allHigh = Array.from({ length: 8 }, (_, i) => election(`a-${i}`, [], "high"));
    expect(deriveBallotFilters({ ...OFF, elections: allHigh }).showImpactFilter).toBe(false);
    const noneHigh = Array.from({ length: 8 }, (_, i) => election(`n-${i}`, [], "low"));
    expect(deriveBallotFilters({ ...OFF, elections: noneHigh }).showImpactFilter).toBe(false);
  });

  it("applies an engaged request even on a short ballot", () => {
    // The threshold gates the OFFER only — a shared ?impact=high link onto
    // a short ballot still filters (the data is present, unlike issues with
    // no saved areas).
    const view = deriveBallotFilters({
      ...OFF,
      elections: [HIGH, election("m-1", [], "medium")],
      impactRequested: true,
    });
    expect(view.impactOn).toBe(true);
    expect(view.showImpactFilter).toBe(true);
    expect(view.visibleElections).toEqual([HIGH]);
    expect(view.hiddenCount).toBe(1);
  });
});

describe("deriveBallotFilters — composition", () => {
  it("ANDs both filters into one visible list and one combined hidden count", () => {
    const both = election("b-1", ["a-1"], "high");
    const issuesOnly = election("b-2", ["a-1"], "low");
    const impactOnly = election("b-3", [], "very_high");
    const neither = election("b-4", [], "medium");
    const view = deriveBallotFilters({
      elections: [both, issuesOnly, impactOnly, neither],
      savedAreaIds: SAVED,
      hasSaved: true,
      issuesRequested: true,
      impactRequested: true,
    });
    expect(view.visibleElections).toEqual([both]);
    expect(view.hiddenCount).toBe(3);
    expect(view.activeFilterCount).toBe(2);
    expect(view.showIssuesFilter).toBe(true);
    expect(view.showImpactFilter).toBe(true);
  });
});
