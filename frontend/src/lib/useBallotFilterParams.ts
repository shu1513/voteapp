import { useSearchParams } from "react-router";
import type { VoteImpactThreshold } from "@voteapp/api-client";

/**
 * URL state for the ballot filters, shared by both web ballot pages so the
 * param names and set/delete semantics cannot drift. `issues=mine` and
 * `impact=high|medium` like `sort`, so the choices survive navigating into
 * an election and back; off by default (absent params). The impact value is
 * the wire word (`medium`, matching the backend label) even though the UI
 * says "Average". Deliberately NOT account preferences — hiding races
 * should never silently persist across visits. Uses the functional updater
 * so it composes with other params (the anonymous page's `d` and `sort`)
 * without clobbering them.
 */
export function useBallotFilterParams(): {
  issuesRequested: boolean;
  impactRequested: VoteImpactThreshold | null;
  onIssuesFilterChange: (on: boolean) => void;
  /** One threshold at a time — setting a level replaces the other (the
   * thresholds nest, so combining them is meaningless); null clears. */
  onImpactFilterChange: (level: VoteImpactThreshold | null) => void;
  /** Clears both filters in one history replace — the "Show all" action. */
  onShowAll: () => void;
} {
  const [searchParams, setSearchParams] = useSearchParams();

  function setParams(changes: Record<string, string | null>) {
    setSearchParams(
      (previous) => {
        const next = new URLSearchParams(previous);
        for (const [name, value] of Object.entries(changes)) {
          if (value === null) {
            next.delete(name);
          } else {
            next.set(name, value);
          }
        }
        return next;
      },
      { replace: true }
    );
  }

  const rawImpact = searchParams.get("impact");
  return {
    issuesRequested: searchParams.get("issues") === "mine",
    // Unknown values (a hand-edited URL) read as off rather than guessing a
    // threshold the user did not pick.
    impactRequested: rawImpact === "high" || rawImpact === "medium" ? rawImpact : null,
    onIssuesFilterChange: (on: boolean) => setParams({ issues: on ? "mine" : null }),
    onImpactFilterChange: (level: VoteImpactThreshold | null) => setParams({ impact: level }),
    onShowAll: () => setParams({ issues: null, impact: null }),
  };
}
