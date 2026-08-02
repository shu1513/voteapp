import { useSearchParams } from "react-router";

/**
 * URL state for the ballot filters, shared by both web ballot pages so the
 * param names and set/delete semantics cannot drift. `issues=mine` and
 * `impact=high` like `sort`, so the choices survive navigating into an
 * election and back; off by default (absent params). Deliberately NOT
 * account preferences — hiding races should never silently persist across
 * visits. Uses the functional updater so it composes with other params
 * (the anonymous page's `d` and `sort`) without clobbering them.
 */
export function useBallotFilterParams(): {
  issuesRequested: boolean;
  impactRequested: boolean;
  onIssuesFilterChange: (on: boolean) => void;
  onImpactFilterChange: (on: boolean) => void;
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

  return {
    issuesRequested: searchParams.get("issues") === "mine",
    impactRequested: searchParams.get("impact") === "high",
    onIssuesFilterChange: (on: boolean) => setParams({ issues: on ? "mine" : null }),
    onImpactFilterChange: (on: boolean) => setParams({ impact: on ? "high" : null }),
    onShowAll: () => setParams({ issues: null, impact: null }),
  };
}
