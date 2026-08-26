import { useSearchParams } from "react-router";
import type { BallotRaceType, VoteImpactThreshold } from "@voteapp/api-client";

/**
 * URL state for the ballot filters and the race-type tab, shared by both
 * web ballot pages so the param names and set/delete semantics cannot
 * drift. `issues=mine`, `impact=high|medium`, and `type=office|
 * ballot_measure` like `sort`, so the choices survive navigating into an
 * election and back; off/All by default (absent params). The values are
 * the wire words (`medium`, `ballot_measure` — matching the backend
 * labels) even though the UI says "Normal" / "Ballot Measures".
 * Deliberately NOT account preferences — hiding races should never
 * silently persist across visits. Uses the functional updater so it
 * composes with other params (the anonymous page's `d` and `sort`)
 * without clobbering them.
 */
export function useBallotFilterParams(): {
  issuesRequested: boolean;
  impactRequested: VoteImpactThreshold | null;
  raceTypeRequested: BallotRaceType | null;
  onIssuesFilterChange: (on: boolean) => void;
  /** One threshold at a time — setting a level replaces the other (the
   * thresholds nest, so combining them is meaningless); null clears. */
  onImpactFilterChange: (level: VoteImpactThreshold | null) => void;
  /** The tab switch; null = the "All" tab (param removed). */
  onRaceTypeChange: (raceType: BallotRaceType | null) => void;
  /** Clears both filters in one history replace — the "Show all" action.
   * Never the tab: tabs are visible navigation, not hidden-by-choice. */
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
  const rawType = searchParams.get("type");
  return {
    issuesRequested: searchParams.get("issues") === "mine",
    // Unknown values (a hand-edited URL) read as off rather than guessing a
    // threshold the user did not pick.
    impactRequested: rawImpact === "high" || rawImpact === "medium" ? rawImpact : null,
    // Same rule: an unknown type reads as the "All" tab.
    raceTypeRequested: rawType === "office" || rawType === "ballot_measure" ? rawType : null,
    onIssuesFilterChange: (on: boolean) => setParams({ issues: on ? "mine" : null }),
    onImpactFilterChange: (level: VoteImpactThreshold | null) => setParams({ impact: level }),
    onRaceTypeChange: (raceType: BallotRaceType | null) => setParams({ type: raceType }),
    onShowAll: () => setParams({ issues: null, impact: null }),
  };
}
