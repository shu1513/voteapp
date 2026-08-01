import { useSearchParams } from "react-router";

/**
 * URL state for the ballot "Only my issues" filter, shared by both web
 * ballot pages so the param name and set/delete semantics cannot drift.
 * `issues=mine` like `sort`, so the choice survives navigating into an
 * election and back; off by default (absent param). Deliberately NOT an
 * account preference — hiding races should never silently persist across
 * visits. Uses the functional updater so it composes with other params
 * (the anonymous page's `d` and `sort`) without clobbering them.
 */
export function useIssuesFilterParam(): {
  issuesRequested: boolean;
  onIssuesFilterChange: (on: boolean) => void;
} {
  const [searchParams, setSearchParams] = useSearchParams();

  function onIssuesFilterChange(on: boolean) {
    setSearchParams(
      (previous) => {
        const next = new URLSearchParams(previous);
        if (on) {
          next.set("issues", "mine");
        } else {
          next.delete("issues");
        }
        return next;
      },
      { replace: true }
    );
  }

  return { issuesRequested: searchParams.get("issues") === "mine", onIssuesFilterChange };
}
