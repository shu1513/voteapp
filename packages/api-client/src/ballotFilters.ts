import type { ElectionSummary } from "./types";

/** The impact filter is offered only on ballots LONGER than this — on a
 * short ballot "read every race" beats trimming, and the default vote_power
 * sort already surfaces the high-impact races on top. */
export const LONG_BALLOT_THRESHOLD = 7;

function matchesIssues(election: ElectionSummary, savedAreaIds: Set<string>): boolean {
  return election.research_areas.some((area) => savedAreaIds.has(area.id));
}

function matchesImpact(election: ElectionSummary): boolean {
  // Labels only, never raw score: the label thresholds are backend-authored
  // (votePower.ts) and already the published grading. `unknown` does not
  // match — the filter claims "high impact" and unknown is not known-high;
  // the combined hidden count explains the disappearance.
  return election.vote_power.label === "high" || election.vote_power.label === "very_high";
}

/**
 * Ballot filter derivation, shared by all four ballot surfaces (each
 * platform keeps its own UI in its BallotFiltersControl component; the
 * policy lives here once, like partyBucket, so web and mobile cannot drift).
 *
 * Two filters, AND-ed into one visible list with one combined hidden count:
 * - "Only my issues": keep = elections whose research areas intersect the
 *   viewer's saved areas. A viewer with no saved areas gets the request
 *   ignored (the intersection is meaningless without them), which also
 *   covers a shared ?issues=mine link opened anonymously. While the saved
 *   areas are still LOADING the web pages withhold the list instead of
 *   calling this (see useMyResearchAreas().isLoading).
 * - "High impact only": keep = vote_power label high/very_high. No data
 *   gate — vote_power ships on every summary, anonymous included — so an
 *   engaged request always applies; LONG_BALLOT_THRESHOLD gates only the
 *   OFFER (a shared ?impact=high link onto a short ballot still filters).
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
}: {
  elections: ElectionSummary[];
  savedAreaIds: Set<string>;
  hasSaved: boolean;
  issuesRequested: boolean;
  impactRequested: boolean;
}): {
  visibleElections: ElectionSummary[];
  issuesOn: boolean;
  showIssuesFilter: boolean;
  impactOn: boolean;
  showImpactFilter: boolean;
  /** Filters ON right now — the "Filters · N" badge. Ordering never counts. */
  activeFilterCount: number;
  /** Relative to the full list; one count line covers both filters. */
  hiddenCount: number;
} {
  const issuesMatched = elections.filter((election) => matchesIssues(election, savedAreaIds));
  const impactMatched = elections.filter(matchesImpact);
  const issuesOn = hasSaved && issuesRequested;
  const impactOn = impactRequested;

  const visibleElections = elections.filter(
    (election) =>
      (!issuesOn || matchesIssues(election, savedAreaIds)) &&
      (!impactOn || matchesImpact(election))
  );

  const splits = (matched: ElectionSummary[]) =>
    matched.length > 0 && matched.length < elections.length;
  const showIssuesFilter = issuesOn || (hasSaved && splits(issuesMatched));
  const showImpactFilter =
    impactOn || (elections.length > LONG_BALLOT_THRESHOLD && splits(impactMatched));

  return {
    visibleElections,
    issuesOn,
    showIssuesFilter,
    impactOn,
    showImpactFilter,
    activeFilterCount: (issuesOn ? 1 : 0) + (impactOn ? 1 : 0),
    hiddenCount: elections.length - visibleElections.length,
  };
}
