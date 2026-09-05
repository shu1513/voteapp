import { useSearchParams } from "react-router";
import type { BallotRaceType } from "@voteapp/api-client";
import { track } from "./usage";

/**
 * URL state for the ballot pages' race-type tab, shared by both web ballot
 * pages so the param name and set/delete semantics cannot drift.
 * `type=office|ballot_measure` like `sort`, so the tab survives navigating
 * into an election and back; the "All" tab is the absent param. The values
 * are the wire words (`ballot_measure`, matching the backend column) even
 * though the UI says "Ballot Measures". Uses the functional updater so it
 * composes with other params (the anonymous page's `d` and `sort`) without
 * clobbering them.
 */
export function useRaceTypeParam(): {
  raceTypeRequested: BallotRaceType | null;
  /** The tab switch; null = the "All" tab (param removed). */
  onRaceTypeChange: (raceType: BallotRaceType | null) => void;
} {
  const [searchParams, setSearchParams] = useSearchParams();
  const rawType = searchParams.get("type");
  return {
    // An unknown value (a hand-edited URL) reads as the "All" tab.
    raceTypeRequested: rawType === "office" || rawType === "ballot_measure" ? rawType : null,
    onRaceTypeChange: (raceType: BallotRaceType | null) => {
      track("list_control", { control: "race_tab", value: raceType ?? "all" });
      setSearchParams(
        (previous) => {
          const next = new URLSearchParams(previous);
          if (raceType === null) {
            next.delete("type");
          } else {
            next.set("type", raceType);
          }
          return next;
        },
        { replace: true }
      );
    },
  };
}
