import { describe, expect, it } from "vitest";

import { deriveBallotFilters, LONG_BALLOT_THRESHOLD } from "./ballotFilters";
import type { ElectionSummary, VotePower } from "./types";

// Only the fields the derivation reads; the rest of ElectionSummary is
// irrelevant to the predicates.
function election(
  id: string,
  areaIds: string[],
  label: VotePower["label"] = "low",
  raceType: string = "office"
): ElectionSummary {
  return {
    id,
    race_type: raceType,
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

const OFF = {
  savedAreaIds: SAVED,
  hasSaved: true,
  issuesRequested: false,
  impactRequested: null,
} as const;

describe("deriveBallotFilters — affects my issues", () => {
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

  it("offers the off checkbox only when the list splits into matched and unmatched", () => {
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
      impactRequested: null,
    });
    expect(view.issuesOn).toBe(false);
    expect(view.showIssuesFilter).toBe(false);
    expect(view.visibleElections).toEqual([MATCHED, UNMATCHED]);
    expect(view.activeFilterCount).toBe(0);
  });
});

describe("deriveBallotFilters — vote power thresholds", () => {
  const VERY_HIGH = election("vh-1", [], "very_high");
  const HIGH = election("h-1", [], "high");
  const AVERAGE = election("m-1", [], "medium");
  // A long ballot (> LONG_BALLOT_THRESHOLD) with all three tiers plus an
  // unknown, splitting on both thresholds.
  const longMixed = [
    VERY_HIGH,
    HIGH,
    AVERAGE,
    election("m-2", [], "medium"),
    election("l-1", [], "low"),
    election("l-2", [], "very_low"),
    election("l-3", [], "unknown"),
    election("l-4", [], "low"),
  ];

  it("high threshold keeps high and very_high; unknown counts among the hidden", () => {
    const view = deriveBallotFilters({ ...OFF, elections: longMixed, impactRequested: "high" });
    expect(view.impactLevel).toBe("high");
    expect(view.visibleElections.map((e) => e.id)).toEqual(["vh-1", "h-1"]);
    // 6 hidden includes the unknown-label race: the filter claims a minimum
    // impact and unknown is not known to meet it.
    expect(view.hiddenCount).toBe(6);
    expect(view.activeFilterCount).toBe(1);
  });

  it("medium threshold keeps Average and above", () => {
    const view = deriveBallotFilters({ ...OFF, elections: longMixed, impactRequested: "medium" });
    expect(view.impactLevel).toBe("medium");
    expect(view.visibleElections.map((e) => e.id)).toEqual(["vh-1", "h-1", "m-1", "m-2"]);
    expect(view.hiddenCount).toBe(4);
    expect(view.activeFilterCount).toBe(1);
  });

  it("offers the off checkboxes only past the long-ballot threshold", () => {
    expect(longMixed.length).toBe(LONG_BALLOT_THRESHOLD + 1);
    const long = deriveBallotFilters({ ...OFF, elections: longMixed });
    expect(long.showImpactHigh).toBe(true);
    expect(long.showImpactMedium).toBe(true);
    // Exactly at the threshold (7): short ballot, no offer even though the
    // list splits.
    const atThreshold = deriveBallotFilters({
      ...OFF,
      elections: longMixed.slice(0, LONG_BALLOT_THRESHOLD),
    });
    expect(atThreshold.showImpactHigh).toBe(false);
    expect(atThreshold.showImpactMedium).toBe(false);
  });

  it("offers each threshold only when it splits the list", () => {
    // Long but all-high: both thresholds are no-ops.
    const allHigh = Array.from({ length: 8 }, (_, i) => election(`a-${i}`, [], "high"));
    const allHighView = deriveBallotFilters({ ...OFF, elections: allHigh });
    expect(allHighView.showImpactHigh).toBe(false);
    expect(allHighView.showImpactMedium).toBe(false);
    // Long but none at Average or above: both would empty the ballot.
    const noneHigh = Array.from({ length: 8 }, (_, i) => election(`n-${i}`, [], "low"));
    const noneView = deriveBallotFilters({ ...OFF, elections: noneHigh });
    expect(noneView.showImpactHigh).toBe(false);
    expect(noneView.showImpactMedium).toBe(false);
    // No high races at all: only the medium option is offered.
    const mediumOnly = [AVERAGE, ...Array.from({ length: 7 }, (_, i) => election(`lo-${i}`, [], "low"))];
    const mediumView = deriveBallotFilters({ ...OFF, elections: mediumOnly });
    expect(mediumView.showImpactHigh).toBe(false);
    expect(mediumView.showImpactMedium).toBe(true);
  });

  it("withholds the medium option when it would duplicate the high option", () => {
    // No Average races: medium and high thresholds keep the same set, so
    // offering both would be two checkboxes doing the same thing.
    const noAverage = [
      VERY_HIGH,
      HIGH,
      ...Array.from({ length: 6 }, (_, i) => election(`lo-${i}`, [], "low")),
    ];
    const view = deriveBallotFilters({ ...OFF, elections: noAverage });
    expect(view.showImpactHigh).toBe(true);
    expect(view.showImpactMedium).toBe(false);
    // Unless it is the engaged threshold — an active filter never vanishes.
    const engaged = deriveBallotFilters({ ...OFF, elections: noAverage, impactRequested: "medium" });
    expect(engaged.showImpactMedium).toBe(true);
  });

  it("applies an engaged threshold even on a short ballot", () => {
    // The threshold gates the OFFER only — a shared ?impact=high link onto
    // a short ballot still filters (the data is present, unlike issues with
    // no saved areas).
    const view = deriveBallotFilters({
      ...OFF,
      elections: [HIGH, AVERAGE],
      impactRequested: "high",
    });
    expect(view.impactLevel).toBe("high");
    expect(view.showImpactHigh).toBe(true);
    expect(view.visibleElections).toEqual([HIGH]);
    expect(view.hiddenCount).toBe(1);
  });
});

describe("deriveBallotFilters — race-type tabs", () => {
  const OFFICE = election("o-1", []);
  const OFFICE_2 = election("o-2", []);
  const MEASURE = election("q-1", [], "low", "ballot_measure");

  it("offers the tab bar only when both race types are present", () => {
    expect(deriveBallotFilters({ ...OFF, elections: [OFFICE, MEASURE] }).showRaceTypeTabs).toBe(true);
    expect(deriveBallotFilters({ ...OFF, elections: [OFFICE, OFFICE_2] }).showRaceTypeTabs).toBe(false);
    expect(deriveBallotFilters({ ...OFF, elections: [MEASURE] }).showRaceTypeTabs).toBe(false);
    expect(deriveBallotFilters({ ...OFF, elections: [] }).showRaceTypeTabs).toBe(false);
  });

  it("an engaged tab slices the list without touching the filter counters", () => {
    const view = deriveBallotFilters({
      ...OFF,
      elections: [OFFICE, MEASURE, OFFICE_2],
      raceTypeRequested: "ballot_measure",
    });
    expect(view.raceType).toBe("ballot_measure");
    expect(view.visibleElections).toEqual([MEASURE]);
    // Tab-hidden races are on the other, visibly-labeled tab — never
    // "hidden" and never a filter.
    expect(view.hiddenCount).toBe(0);
    expect(view.activeFilterCount).toBe(0);
  });

  it("ignores the request on a single-type ballot", () => {
    // A shared ?type= link must never empty a ballot that shows no tab bar.
    const view = deriveBallotFilters({
      ...OFF,
      elections: [OFFICE, OFFICE_2],
      raceTypeRequested: "ballot_measure",
    });
    expect(view.raceType).toBe(null);
    expect(view.visibleElections).toEqual([OFFICE, OFFICE_2]);
  });

  it("filters apply within the tab slice and the hidden count is slice-relative", () => {
    const matchedOffice = election("o-m", ["a-1"]);
    const unmatchedOffice = election("o-u", []);
    const unmatchedMeasure = election("q-u", [], "low", "ballot_measure");
    const view = deriveBallotFilters({
      ...OFF,
      elections: [matchedOffice, unmatchedOffice, unmatchedMeasure],
      issuesRequested: true,
      raceTypeRequested: "office",
    });
    expect(view.visibleElections).toEqual([matchedOffice]);
    // Only the filtered-out office counts; the measure is on the other tab.
    expect(view.hiddenCount).toBe(1);
    expect(view.activeFilterCount).toBe(1);
  });

  it("filteredElections is the filter-visible pool without the tab slice", () => {
    // The detail rail re-slices this pool, so it must keep the other tab's
    // races — but never filter-hidden ones.
    const matchedOffice = election("o-m", ["a-1"]);
    const unmatchedOffice = election("o-u", []);
    const matchedMeasure = election("q-m", ["a-1"], "low", "ballot_measure");
    const view = deriveBallotFilters({
      ...OFF,
      elections: [matchedOffice, unmatchedOffice, matchedMeasure],
      issuesRequested: true,
      raceTypeRequested: "ballot_measure",
    });
    expect(view.visibleElections).toEqual([matchedMeasure]);
    expect(view.filteredElections).toEqual([matchedOffice, matchedMeasure]);
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
      impactRequested: "high",
    });
    expect(view.visibleElections).toEqual([both]);
    expect(view.hiddenCount).toBe(3);
    expect(view.activeFilterCount).toBe(2);
    expect(view.showIssuesFilter).toBe(true);
    expect(view.showImpactHigh).toBe(true);
  });
});
