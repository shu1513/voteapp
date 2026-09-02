import type { ElectionChoice } from "./types";
import { isDecidedChoice } from "./useElectionChoices";

export type PickProgress = { picked: number; total: number; complete: boolean };

/**
 * The draft link's label, shared by the web header nav, the web candidate
 * page's post-pick actions, and the mobile saved-ballot header so they never
 * drift: plain until the first pick (no homework-flavored "0/8", and no
 * counter while the queries haven't settled — a counter that flashes in
 * later is fine, a wrong one is not), then counting up, then the earned name
 * "My Picks ✓" when every race on the nearest election day is decided.
 */
export function myDraftLabel(progress: PickProgress | null): string {
  return progress && progress.picked > 0
    ? progress.complete
      ? "My Picks ✓"
      : `My Draft ${progress.picked}/${progress.total}`
    : "My Draft";
}

/**
 * Pick progress over the nearest upcoming election day ("My Picks 4/13" →
 * "My Picks ✓"): the same denominator as that day's draft card. Null means
 * no counter (ballot or choices not loaded, or no upcoming races). Pure —
 * each platform's hook supplies its own ballot fetch and today string; only
 * id + election_date are read so any election payload shape qualifies.
 */
export function nearestDayPickProgress(
  elections: { id: string; election_date: string }[] | undefined,
  choiceByElectionId: Map<string, ElectionChoice> | undefined,
  today: string
): PickProgress | null {
  if (elections === undefined || choiceByElectionId === undefined) {
    return null;
  }
  const upcoming = elections.filter((election) => election.election_date >= today);
  if (upcoming.length === 0) {
    return null;
  }
  const date = upcoming.reduce(
    (min, election) => (election.election_date < min ? election.election_date : min),
    upcoming[0].election_date
  );
  const group = upcoming.filter((election) => election.election_date === date);
  const picked = group.filter((election) => isDecidedChoice(choiceByElectionId.get(election.id))).length;
  return { picked, total: group.length, complete: picked === group.length };
}
