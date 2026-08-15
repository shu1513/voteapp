import type { ElectionSummary } from "./types";

/** The impact filter is offered only on ballots LONGER than this — on a
 * short ballot "read every race" beats trimming, and the default vote_power
 * sort already surfaces the high-impact races on top. */
export const LONG_BALLOT_THRESHOLD = 7;

/** The impact filter is a minimum-label threshold, not a label set: "high"
 * keeps High + Very high, "medium" keeps Average and above. The wire word
 * stays `medium` (matching the backend label and the URL param) even though
 * the UI renders it as "Average" via formatVotePowerLabel. */
export type VoteImpactThreshold = "high" | "medium";

/** The two ElectionSummary.race_type values the tabs slice on. Wire words
 * (matching the backend column and the URL param) even though the UI says
 * "Candidates" / "Ballot Measures". */
export type BallotRaceType = "office" | "ballot_measure";

const IMPACT_LABELS: Record<VoteImpactThreshold, ReadonlySet<string>> = {
  high: new Set(["high", "very_high"]),
  medium: new Set(["medium", "high", "very_high"]),
};

function matchesIssues(election: ElectionSummary, savedAreaIds: Set<string>): boolean {
  return election.research_areas.some((area) => savedAreaIds.has(area.id));
}

function matchesImpact(election: ElectionSummary, threshold: VoteImpactThreshold): boolean {
  // Labels only, never raw score: the label thresholds are backend-authored
  // (votePower.ts) and already the published grading. `unknown` matches
  // neither threshold — the filter claims a minimum impact and unknown is
  // not known to meet it; the combined hidden count explains the
  // disappearance.
  return IMPACT_LABELS[threshold].has(election.vote_power.label);
}

/**
 * Ballot filter derivation, shared by all four ballot surfaces (each
 * platform keeps its own UI in its BallotFiltersControl component; the
 * policy lives here once, like partyBucket, so web and mobile cannot drift).
 *
 * A race-type tab plus two filters, AND-ed into one visible list:
 * - Race-type tabs (All / offices / ballot measures): a view switch, not a
 *   filter — the tab bar is offered only when the full list contains BOTH
 *   race types (a single-type ballot has nothing to switch between), and a
 *   requested type on a single-type ballot is ignored (a shared ?type= link
 *   must never empty a ballot with no tab bar on screen to explain it).
 *   Tab-hidden races are on the other, visibly-labeled tab, so they never
 *   count into hiddenCount or activeFilterCount — those describe only the
 *   filters, and hiddenCount is relative to the current tab's slice so
 *   "Show all" (which clears filters, never the tab) zeroes it.
 *
 * Two filters, AND-ed into one visible list with one combined hidden count:
 * - "Affects my issues": keep = elections whose research areas intersect
 *   the viewer's saved areas. A viewer with no saved areas gets the request
 *   ignored (the intersection is meaningless without them), which also
 *   covers a shared ?issues=mine link opened anonymously. While the saved
 *   areas are still LOADING the web pages withhold the list instead of
 *   calling this (see useMyResearchAreas().isLoading).
 * - My vote impact: keep = elections at or above the requested threshold
 *   ("High or above" / "Average or above"). One threshold at a time — they
 *   nest, so combining them is meaningless and the UI auto-swaps. No data
 *   gate — vote_power ships on every summary, anonymous included — so an
 *   engaged request always applies; LONG_BALLOT_THRESHOLD gates only the
 *   OFFER (a shared ?impact=high link onto a short ballot still filters).
 *   The medium option is additionally withheld when it would duplicate the
 *   high option (a ballot with no Average races), so the panel never
 *   offers two checkboxes that do the same thing.
 *
 * Same visibility rule as the election page's records filter: while OFF a
 * filter is offered only when it could change the current view (its
 * predicate splits the full list into matched + unmatched — all-match is a
 * no-op, none-match would empty the ballot unexplained); while ON it stays
 * visible and keeps applying even when that empties the view ("N elections
 * hidden · Show all" explains it), because an active filter that silently
 * stops applying would show a full ballot the viewer believes is filtered.
 * Offer gates test the FULL list, not the other filter's view — simpler,
 * and an engaged combination that empties the view is already explained by
 * the combined count.
 */
export function deriveBallotFilters({
  elections,
  savedAreaIds,
  hasSaved,
  issuesRequested,
  impactRequested,
  raceTypeRequested = null,
}: {
  elections: ElectionSummary[];
  savedAreaIds: Set<string>;
  hasSaved: boolean;
  issuesRequested: boolean;
  impactRequested: VoteImpactThreshold | null;
  /** Optional so callers without a tab bar (mobile, for now) are unchanged. */
  raceTypeRequested?: BallotRaceType | null;
}): {
  visibleElections: ElectionSummary[];
  /** Offer the tab bar only when the full list contains both race types. */
  showRaceTypeTabs: boolean;
  /** The engaged tab; null = "All" (also when the request was ignored). */
  raceType: BallotRaceType | null;
  issuesOn: boolean;
  showIssuesFilter: boolean;
  /** The engaged threshold; null when the impact filter is off. */
  impactLevel: VoteImpactThreshold | null;
  showImpactHigh: boolean;
  showImpactMedium: boolean;
  /** Filters ON right now — the "Filters · N" badge; the impact filter
   * counts once whichever threshold is engaged. Ordering never counts. */
  activeFilterCount: number;
  /** Relative to the current tab's slice; one count line covers both
   * filters. Tab-hidden races never count — they are on the other tab. */
  hiddenCount: number;
} {
  const issuesMatched = elections.filter((election) => matchesIssues(election, savedAreaIds));
  const highMatched = elections.filter((election) => matchesImpact(election, "high"));
  const mediumMatched = elections.filter((election) => matchesImpact(election, "medium"));
  const issuesOn = hasSaved && issuesRequested;
  const impactLevel = impactRequested;

  const showRaceTypeTabs =
    elections.some((election) => election.race_type === "office") &&
    elections.some((election) => election.race_type === "ballot_measure");
  const raceType = showRaceTypeTabs ? raceTypeRequested : null;
  // The tab slices first; the filters then apply within the slice, so the
  // hidden count explains exactly what the filters removed from the view.
  const tabElections = raceType
    ? elections.filter((election) => election.race_type === raceType)
    : elections;

  const visibleElections = tabElections.filter(
    (election) =>
      (!issuesOn || matchesIssues(election, savedAreaIds)) &&
      (!impactLevel || matchesImpact(election, impactLevel))
  );

  const splits = (matched: ElectionSummary[]) =>
    matched.length > 0 && matched.length < elections.length;
  const longBallot = elections.length > LONG_BALLOT_THRESHOLD;
  const showIssuesFilter = issuesOn || (hasSaved && splits(issuesMatched));
  const showImpactHigh = impactLevel === "high" || (longBallot && splits(highMatched));
  const showImpactMedium =
    impactLevel === "medium" ||
    (longBallot && splits(mediumMatched) && mediumMatched.length > highMatched.length);

  return {
    visibleElections,
    showRaceTypeTabs,
    raceType,
    issuesOn,
    showIssuesFilter,
    impactLevel,
    showImpactHigh,
    showImpactMedium,
    activeFilterCount: (issuesOn ? 1 : 0) + (impactLevel ? 1 : 0),
    hiddenCount: tabElections.length - visibleElections.length,
  };
}
