import type { ElectionSummary } from "@voteapp/api-client";

/**
 * "Only my issues" ballot filter derivation, shared by the anonymous and
 * saved ballot pages (the toggle UI lives in components/OnlyMyIssuesFilter).
 * Keep = elections whose research areas intersect the viewer's saved areas.
 * Same visibility rule as the election page's records filter: while OFF it
 * renders only when it could change the current view (viewer has saved areas
 * AND the list splits into matched + unmatched); while ON it stays visible
 * and keeps applying — even when that empties the view ("N elections hidden
 * · Show all" explains the empty list) — because an active filter that
 * silently stops applying would show a full ballot the viewer believes is
 * filtered. A viewer with no saved areas gets the request ignored (the
 * intersection is meaningless without them), which also covers a shared
 * ?issues=mine link opened anonymously. While the saved areas are still
 * LOADING the pages withhold the list instead of calling this (see
 * useMyResearchAreas().isLoading) — otherwise a ?issues=mine load would
 * flash the full ballot before the filter engages.
 */
export function deriveOnlyMyIssues({
  elections,
  savedAreaIds,
  hasSaved,
  requested,
}: {
  elections: ElectionSummary[];
  savedAreaIds: Set<string>;
  hasSaved: boolean;
  requested: boolean;
}): {
  visibleElections: ElectionSummary[];
  filterOn: boolean;
  showFilter: boolean;
  hiddenCount: number;
} {
  const matched = elections.filter((election) =>
    election.research_areas.some((area) => savedAreaIds.has(area.id))
  );
  const filterOn = hasSaved && requested;
  const showFilter =
    filterOn || (hasSaved && matched.length > 0 && matched.length < elections.length);
  return {
    visibleElections: filterOn ? matched : elections,
    filterOn,
    showFilter,
    hiddenCount: elections.length - matched.length,
  };
}
